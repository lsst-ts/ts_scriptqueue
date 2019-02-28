__all__ = ["RequestModel"]

import asyncio
import logging

from lsst.ts import salobj
from lsst.ts.scriptqueue import ScriptProcessState
from lsst.ts.scriptqueue.base_script import HEARTBEAT_INTERVAL
import SALPY_ScriptQueue
import SALPY_Script

from .queue_state import QueueState


class RequestModel:
    """A model class that represents the user interface to the queue. It stores the current
    state of the queue as well as for scripts in the queue. It provides methods to update its
    internal information and also callback functions that can be scheduled to update in an event loop.

    It also provides methods to send request to the queue and methods to monitor the state of
    executing script. It relies heavily in the ability of SAL to retrieve late joiner information.
    """

    def __init__(self, index):

        self.log = logging.getLogger(__name__)

        self.queue = salobj.Remote(SALPY_ScriptQueue, index)
        self.evt_loop = asyncio.get_event_loop()

        self.cmd_timeout = 120.
        self.max_lost_heartbeats = 5
        """Specify the tolerance in subsequent heartbeats lost when
        monitoring a script.
        """

        self.state = QueueState()
        """Store the overall state of the queue.
        """

        # Initialize information about the queue. If the queue is not enable or offline state
        # will be left unchanged.
        if self.query_queue_state() < 0:
            self.log.warning("Could not get state of the queue. Make sure queue is running and is "
                             "in enabled state.")
            return

        self.query_scripts_info()  # This will force the queue to output the state of all its scripts
        self.update_scripts_info()  # Now update the internal information

    def enable_queue(self):
        """A method to enable the queue. It will try to identify the current state of the
        queue and if it fails, assumes the queue is in STANDBY.
        """
        try:
            self.run(salobj.enable_csc(self.queue, timeout=self.cmd_timeout))
        except Exception as e:
            self.log.error("Could not start queue using enable_csc. Trying to force "
                           "enable from standby.")
            self.log.exception(e)
            pass

        try:
            self.run(self.queue.cmd_start.start(timeout=self.cmd_timeout))
        except Exception as e:
            self.log.exception(e)
            pass

        try:
            self.run(self.queue.cmd_enable.start(timeout=self.cmd_timeout))
        except Exception as e:
            self.log.exception(e)
            pass

        summary_state = self.queue.evt_summaryState.get()
        if summary_state is None:
            self.log.error("Can not determine state of the queue. Try listening for heartbeats "
                           "to make sure queue is running.")
        else:
            self.log.info(f"Queue state: {salobj.State(summary_state.summaryState)}")

        return False

    def get_scripts(self):
        """Return available scripts.

        Returns
        -------
        available_scripts : `dict` [`str`, `list` [`str`]]
            A dictionary with the available "external" and "standard" scripts. Each
            script is represented by their relative paths as seen by the queue.

        """

        script_list = self.queue.evt_availableScripts.get()
        if script_list is None:
            self.log.debug("No previous information about script list. Requesting.")
            available_scripts_coro = self.queue.evt_availableScripts.next(flush=False,
                                                                          timeout=self.cmd_timeout)

            self.run(self.queue.cmd_showAvailableScripts.start(timeout=self.cmd_timeout))

            script_list = self.run(available_scripts_coro)

        return {'external': script_list.external.split(':'),
                'standard': script_list.standard.split(':')}

    def pause_queue(self):
        """Pause the queue."""
        self.run(self.queue.cmd_pause.start(timeout=self.cmd_timeout))

    def resume_queue(self):
        """Resume queue operation"""
        self.run(self.queue.cmd_resume.start(timeout=self.cmd_timeout))

    def quit_queue(self):
        """Quit queue by sending it to exit control. Assume the queue is in enable state
        but if it is not, will skip the rejected commands for disable and standby.
        """

        try:
            self.run(self.queue.cmd_disable.start(timeout=self.cmd_timeout))
        except Exception as e:
            pass

        try:
            self.run(self.queue.cmd_standby.start(timeout=self.cmd_timeout))
        except Exception as e:
            pass

        try:
            self.run(self.queue.cmd_exitControl.start(timeout=self.cmd_timeout))
        except Exception as e:
            pass

        return True

    def stop_scripts(self, sal_indices, terminate=False):
        """Stop list of scripts.

        Parameters
        ----------
        sal_indices : list(int)
            A list of script indices to stop.

        terminate : bool
            Should the script be terminated?

        """
        topic = self.queue.cmd_stopScripts.DataType()
        for i in range(len(sal_indices)):
            topic.salIndices[i] = sal_indices[i]
        topic.length = len(sal_indices)
        topic.terminate = terminate

        self.run(self.queue.cmd_stopScripts.start(topic, timeout=self.cmd_timeout))

    def get_queue_state(self):
        """A method to get the state of the queue and the scripts running on the queue.

        Returns
        -------
        state : dict

        """

        self.update_queue_state()

        self.update_scripts_info()

        return self.state.parse()

    def add(self, path, is_standard, config):
        """Add a script to the queue.

        Parameters
        ----------
        path : str
            Name of the script.
        is_standard : bool
            Is it a standard script?
        config : str
            YAML configuration string.

        Returns
        -------
        salindex : int
            The salindex of the script added to the queue.
        """

        self.queue.cmd_add.set(isStandard=is_standard,
                               path=path,
                               config=config,
                               location=SALPY_ScriptQueue.add_Last)

        ack = self.run(self.queue.cmd_add.start(timeout=self.cmd_timeout))

        self.state.add_script(int(ack.ack.result))

        return int(ack.ack.result)

    def set_queue_log_level(self, level):
        """Set queue log level.

        Parameters
        ----------
        level : int
            Log level; error=40, warning=30, info=20, debug=10

        """
        self.queue.cmd_setLogLevel.set(level=level)
        self.run(self.queue.cmd_setLogLevel.start(timeout=self.cmd_timeout))

    def set_script_log_level(self, index, level):
        """Set the log level for a script.

        Parameters
        ----------
        index : int
            Script index. It will raise an exception if the script is not in the queue.
        level : int
            Log level.

        """
        if self.state.scripts[index]['remote'] is None:
            self.get_script_remote(index)

        self.state.scripts[index]['remote'].cmd_setLogLevel.set(level=level)

    def listen_heartbeat(self):
        """Listen for queue heartbeats."""
        return self.run(self.queue.evt_heartbeat.next(flush=True,
                                                      timeout=self.cmd_timeout))

    def get_script_remote(self, salindex):
        """Listen for a script log messages."""

        self.update_scripts_info()
        info = self.state.scripts[salindex]

        if (info["process_state"] < ScriptProcessState.DONE and
                self.state.scripts[salindex]['remote'] is None):
            self.log.debug('Starting script remote')
            self.state.scripts[salindex]['remote'] = salobj.Remote(SALPY_Script, salindex)
            self.state.scripts[salindex]['remote'].cmd_setLogLevel.set(level=10)
        elif info["process_state"] >= ScriptProcessState.DONE:
            raise RuntimeError(f"Script {salindex} in a final state.")

    def run(self, coro):
        """Run `coro` in the event loop.

        Parameters
        ----------
        coro :
            An awaitable item.

        Returns
        -------
        reval :
            The result of the awaiting the coroutine.
        """
        return self.evt_loop.run_until_complete(coro)

    def queue_state_callback(self, queue):
        """Call back function to update the state of the queue.
        """
        self.state.update(queue)

    def update_queue_state(self):
        """Run `get_oldest` in `self.queue.evt_queue` until there is nothing left.

        Returns
        -------
        n_topics : int
            Number of entries in the remote backlog.
        """
        n_topics = 0
        while True:
            queue = self.queue.evt_queue.get_oldest()
            if queue is None:
                return n_topics
            else:
                n_topics += 1
                self.state.update(queue)

    def query_queue_state(self):
        """Query state of the queue."""
        queue_coro = self.queue.evt_queue.next(flush=True, timeout=self.cmd_timeout)

        try:
            self.run(self.queue.cmd_showQueue.start(timeout=self.cmd_timeout))
        except Exception as e:
            self.log.warning('Could not get state of the queue.')
            self.log.exception(e)
            return -1

        queue = self.run(queue_coro)

        self.state.update(queue)

        self.update_scripts_info()

        return 0

    def script_info_callback(self, info):
        """A callback function to monitor the state of the scripts.

        Parameters
        ----------
        info : SALPY_ScriptQueue.ScriptQueue_logevent_scriptC

        """
        self.state.update_script_info(info)

    def update_scripts_info(self):
        """Run `get_oldest` in `self.queue.evt_script` and pass it to `self.parse_script_info`
        until there is nothing left.
        """
        updated = []
        while True:
            info = self.queue.evt_script.get_oldest()
            if info is None:
                return updated
            else:
                if info.salIndex not in updated:
                    updated.append(info.salIndex)
                self.state.update_script_info(info)

    def query_scripts_info(self):
        """Send a command to the queue to output information about all scripts.
        """
        for salindex in self.state.script_indices:
            self.queue.cmd_showScript.set(salIndex=salindex)
            try:
                self.run(self.queue.cmd_showScript.start(timeout=self.cmd_timeout))
            except salobj.AckError as ack_err:
                self.log.error(f"Could not get info on script {salindex}. "
                               f"Failed with ack.result={ack_err.ack.result}")

    @staticmethod
    def parse_info(info):
        """Utility to parse script information dict.

        Parameters
        ----------
        info : dict
            Dictionary returned by `self.model.get_script_info`.

        Returns
        -------
        parsed_str : str
            Parsed string.
        """
        if info is None:
            return "None"

        parsed_str = f"[salIndex:{info['index']}]" \
                     f"[{info['type']}]" \
                     f"[path:{info['path']}]" \
                     f"[duration:{round(info['duration'], 2)}]" \
                     f"[{str(info['script_state'])}]" \
                     f"[{str(info['process_state'])}]"
        return parsed_str

    def monitor_script(self, salindex):
        """Monitor state and log from script.

        Parameters
        ----------
        salindex : int

        """
        self.get_script_remote(salindex)
        finished, unfinished = self.evt_loop.run_until_complete(
            asyncio.wait([self.monitor_script_info(salindex),
                          self.monitor_script_log(salindex),
                          self.monitor_script_checkpoint(salindex),
                          self.monitor_script_heartbeat(salindex)],
                         return_when=asyncio.FIRST_COMPLETED))
        for task in unfinished:
            task.cancel()

    async def monitor_script_log(self, salindex):
        """Coroutine to monitor a script logger.

        Parameters
        ----------
        salindex : int

        """
        if self.state.scripts[salindex]['remote'] is None:
            raise RuntimeError(f"No remote for script {salindex}")

        while True:

            if self.state.scripts[salindex]["process_state"] >= ScriptProcessState.DONE:
                return
            # I won't set a timeout here because a script may never send a log message
            # or may be running a long task where it won't be logging at all, like
            # during a long exposure.
            log_message = await self.state.scripts[salindex]['remote'].evt_logMessage.next(flush=False)
            self.log.log(log_message.level,
                         f'[{salindex}]:{log_message.message}{log_message.traceback}')

    async def monitor_script_info(self, salindex):
        """Coroutine to monitor the state of a script.

        Parameters
        ----------
        salindex : int

        """

        while True:
            # I won't set a timeout here because a script state may be unchanged for a really
            # long period of time. For instance, during a long exposure.

            info = await self.queue.evt_script.next(flush=False)
            self.state.update_script_info(info)

            if info.salIndex == salindex:
                print(self.parse_info(self.state.scripts[salindex]))
            else:
                self.log.debug(self.parse_info(self.state.scripts[salindex]))
            if info.processState >= ScriptProcessState.DONE:
                return

    async def monitor_script_checkpoint(self, salindex):
        """Coroutine to monitor a script checkpoint.

        Parameters
        ----------
        salindex : int

        """
        while True:
            # I won't set a timeout here because a script may be running a long task
            # and not reporting any new checkpoint for a really
            # long period of time. For instance, during a long exposure.
            state = await self.state.scripts[salindex]['remote'].evt_state.next(flush=False)
            self.log.debug(f"[{salindex}]:[checkpoint]{state.lastCheckpoint}")

    async def monitor_script_heartbeat(self, salindex):
        """Coroutine to monitor script heart beats.

        Parameters
        ----------
        salindex : int

        """
        nbeats = 0
        nlost_total = 0
        nlost_subsequent = 0

        while True:

            if self.state.scripts[salindex]['process_state'] >= ScriptProcessState.DONE:
                self.log.debug(f"Script {salindex} finalized.")
                return
            elif self.state.scripts[salindex]['remote'] is None:
                self.log.debug(f"No remote for script {salindex}.")
                return
            elif nlost_subsequent > self.max_lost_heartbeats:
                self.log.error(f"Script is not responding. Lost {nlost_subsequent} "
                               f"subsequent heartbeats. You may have to interrupt the script execution."
                               f"If this is an expected behaviour you should be able to restart the "
                               f"monitoring routine.")
                return

            try:
                await self.state.scripts[salindex]['remote'].evt_heartbeat.next(flush=False,
                                                                                timeout=HEARTBEAT_INTERVAL*3)
                nbeats += 1
                nlost_subsequent = 0
                self.log.debug(f"[{salindex}]:[heartbeat:{nbeats}] - ["
                               f"total lost:{nlost_total}]")
            except asyncio.TimeoutError as e:
                nlost_subsequent += 1
                nlost_total += 1
                self.log.warning(f"[{salindex}]:[heartbeat:{nbeats}] - "
                                 f"[total lost:{nlost_total} - "
                                 f"subsequent lost: {nlost_subsequent}]:"
                                 f"[Missing heartbeats from script.]")
