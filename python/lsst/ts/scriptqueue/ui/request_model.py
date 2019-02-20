__all__ = ["RequestModel"]

import asyncio
import logging

from lsst.ts import salobj
from lsst.ts.scriptqueue import ScriptState, ScriptProcessState
import SALPY_ScriptQueue
import SALPY_Script


class RequestModel:
    """A model class that represents the user interface to the queue. It contains stores the current
    state of the queue as well as for the scripts in the queue. It provides methods to update its
    internal information and also callback functions that can be scheduled to update in an event loop.

    It also provides methods to send request to the queue and methods to monitor the state of
    executing script. It relies heavily in the ability of SAL to retrieve late joiner information.
    """

    def __init__(self, index):

        self.log = logging.getLogger(__name__)

        self.queue = salobj.Remote(SALPY_ScriptQueue, index)
        self.evt_loop = asyncio.get_event_loop()

        self.cmd_timeout = 120.

        self.state = {'state': "Unknown",
                      'length': 0,
                      'past_length': 0,
                      'queue_scripts': [],
                      'past_scripts': [],
                      'current': None}
        """Store the overall state of the queue.
        """

        self.scripts = {}
        """A dictionary to store the state of scripts in the queue. 
        """

        # Initialize information about the queue. If the queue is not enable or offline state
        # will be left unchanged.
        if self.query_queue_state() < 0:
            self.log.warning("Could not get state of the queue. Make sure queue is running and is "
                             "in enabled state.")
            return

        # Initialize information about scripts
        for i in range(self.state['length']):
            self.query_script_info(self.state['queue_scripts'][i])

        for i in range(self.state['past_length']):
            self.query_script_info(self.state['past_scripts'][i])

        if self.state['current'] is not None:
            self.query_script_info(self.state['current'])

    def enable_queue(self):
        """A method to enable the queue. It will try to identify the current state of the
        queue and if it fails, assumes the queue is in STANDBY.
        """
        state = self.queue.evt_summaryState.get()
        if state is None:
            print("No information about current state of the queue... Trying to enable.")
        elif state.summaryState == salobj.State.STANDBY:
            print("Enabling the queue.")
        else:
            print(f"Queue in {salobj.State(state.summaryState)} state, cannot enable.")

        self.run(self.queue.cmd_start.start(timeout=self.cmd_timeout))
        self.run(self.queue.cmd_enable.start(timeout=self.cmd_timeout))
        print(f"Queue enabled. [{self.queue.evt_summaryState.get().summaryState}]")

        return False

    def list(self):
        """Return available scripts.

        Returns
        -------
        available_scripts : dict
            A dictionary with "external" and "standard" list of scripts.

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
        self.run(self.queue.cmd_pause.start())

    def resume_queue(self):
        """Resume queue operation"""
        self.run(self.queue.cmd_resume.start())

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
        self.queue.cmd_stopScripts.set(salIndices=sal_indices,
                                       length=len(sal_indices),
                                       terminate=terminate)
        self.run(self.queue.cmd_stopScripts.start(timeout=self.cmd_timeout))

    def get_queue_state(self):
        """A method to get the state of the queue and the scripts running on the queue.

        Returns
        -------
        state : dict

        """
        self.update_queue_state()

        state = {'state': self.state['state'],
                 'length': self.state['length'],
                 'past_length': self.state['past_length'],
                 'queue_scripts': {},
                 'past_scripts': {},
                 'current': None}

        for i in range(self.state['length']):
            state['queue_scripts'][self.state['queue_scripts'][i]] = \
                self.get_script_info(self.state['queue_scripts'][i])

        for i in range(self.state['past_length']):
            state['past_scripts'][self.state['past_scripts'][i]] = \
                self.get_script_info(self.state['past_scripts'][i])

        if self.state['current'] is not None:
            state['current'] = self.get_script_info(self.state['current'])

        return state

    def get_script_info(self, salindex):
        """Get info about a script with `salindex`.

        Parameters
        ----------
        salindex : int
            Index of the sal script to get info.

        Returns
        -------
        script_info : dict

        """
        if (salindex in self.scripts and
                self.scripts[salindex]['process_state'] >= ScriptProcessState.DONE):
            # if a script is in a final state it will not be updated anymore so
            # just retrieve stored information
            # self.log.debug(f"Done {salindex}")
            self.scripts[salindex]['updated'] = False
            return self.scripts[salindex]
        elif salindex in self.scripts:
            # if script is in the list update all script info and grab the stored information
            # if the script was updated in updata_script_info, mark it as such, otherwise
            # flag it as not updated.
            # self.log.debug(f"Updating {salindex}")
            updated = self.update_script_info()
            self.scripts[salindex]['updated'] = salindex in updated
            return self.scripts[salindex]
        else:
            # It may happen that a script is in the queue but no information is available,
            # for instance, while it is still pre-loading. In this case, pass an empty
            # state.
            # self.log.debug(f"Adding {salindex}")
            info = self.queue.evt_script.DataType()
            info.salIndex = salindex
            # this will add the script to the list of script, but there's no real information about it
            # except for the sal index
            self.parse_script_info(info)
            self.scripts[salindex]['updated'] = True
            return self.scripts[salindex]

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

        return int(ack.ack.result)

    def set_queue_log_level(self, level):
        """Set queue log level.

        Parameters
        ----------
        level : int
            Log level; error=40, warning=30, info=20, debug=10

        """
        valid_level = [40, 30, 20, 10]
        if level not in valid_level:
            raise RuntimeError(f"Level {level} not valid. Must be one of {valid_level}.")

        self.queue.cmd_setLogLevel.set(level=level)
        self.run(self.queue.cmd_setLogLevel.start(timeout=self.cmd_timeout))

    def listen_heartbeat(self):
        """Listen for queue heartbeats."""
        return self.run(self.queue.evt_heartbeat.next(flush=True,
                                                      timeout=self.cmd_timeout))

    def get_script_remote(self, salindex):
        """Listen for a script log messages."""

        self.update_script_info()
        info = self.get_script_info(salindex)

        if (info["process_state"] < ScriptProcessState.DONE and
                self.scripts[salindex]['remote'] is None):
            self.log.debug('Starting script remote')
            self.scripts[salindex]['remote'] = salobj.Remote(SALPY_Script, salindex)
            self.scripts[salindex]['remote'].cmd_setLogLevel.set(level=10)

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
        self.parse_queue_state(queue)

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
                self.parse_queue_state(queue)

    def query_queue_state(self):
        """Query state of the queue."""
        queue_coro = self.queue.evt_queue.next(flush=True, timeout=self.cmd_timeout)

        try:
            self.run(self.queue.cmd_showQueue.start())
        except Exception as e:
            self.log.warning('Could not get state of the queue.')
            self.log.exception(e)
            return -1

        queue = self.run(queue_coro)

        self.parse_queue_state(queue)

        return 0

    def parse_queue_state(self, queue):
        """A method to parse information about the queue.

        Parameters
        ----------
        queue : SALPY_ScriptQueue.ScriptQueue_logevent_queueC

        """

        self.state['state'] = 'Running' if queue.running else 'Stopped'
        self.state['length'] = queue.length
        self.state['past_length'] = queue.pastLength

        self.state['queue_scripts'] = [queue.salIndices[i] for i in range(queue.length)]
        self.state['past_scripts'] = [queue.pastSalIndices[i] for i in range(queue.pastLength)]

        if queue.currentSalIndex > 0:
            self.state['current'] = queue.currentSalIndex
        else:
            self.state['current'] = None

        # clear script list
        current_indices = list(self.scripts.keys())
        for salindex in current_indices:
            if salindex not in self.state['queue_scripts'] and salindex not in self.state['past_scripts']\
                    and salindex != self.state['current']:
                self.log.debug(f"Removing script {salindex}")
                del self.scripts[salindex]

    def script_info_callback(self, info):
        """A callback function to monitor the state of the scripts.

        Parameters
        ----------
        info : SALPY_ScriptQueue.ScriptQueue_logevent_scriptC

        """
        self.parse_script_info(info)

    def update_script_info(self):
        """Run `get_oldest` in `self.queue.evt_script` and pass it to `self.parse_script_info`
        until there is nothing left."""
        updated = []
        while True:
            info = self.queue.evt_script.get_oldest()
            if info is None:
                return updated
            else:
                if info.salIndex not in updated:
                    updated.append(info.salIndex)
                self.parse_script_info(info)

    def query_script_info(self, salindex):
        """Get info about a script with `salindex`.

        Parameters
        ----------
        salindex : int
            Index of the sal script to get info.

        Returns
        -------
        info : dict

        """
        updated = self.update_script_info()
        if salindex in updated:
            return salindex

        self.queue.cmd_showScript.set(salIndex=salindex)

        self.run(self.queue.cmd_showScript.start(timeout=self.cmd_timeout))

        updated = self.update_script_info()
        if salindex in updated:
            return salindex
        else:
            raise RuntimeError(f"Could not update script[{salindex}] information.")

    def parse_script_info(self, info):
        """A method to parse information about scripts.

        Parameters
        ----------
        info : SALPY_ScriptQueue.ScriptQueue_logevent_scriptC

        """

        s_type = 'Standard' if info.isStandard else 'External'
        if info.salIndex not in self.scripts:
            self.scripts[info.salIndex] = {
                'index': info.salIndex,
                'type': s_type,
                'path': info.path,
                'duration': info.duration,
                'timestamp': info.timestamp,
                'script_state': ScriptState(info.scriptState),
                'process_state': ScriptProcessState(info.processState),
                'remote': None,
                'updated': True
            }
        else:
            self.scripts[info.salIndex]['type'] = s_type
            self.scripts[info.salIndex]['path'] = info.path
            self.scripts[info.salIndex]['duration'] = info.duration
            self.scripts[info.salIndex]['timestamp'] = info.timestamp
            self.scripts[info.salIndex]['script_state'] = ScriptState(info.scriptState)
            self.scripts[info.salIndex]['process_state'] = ScriptProcessState(info.processState)
            self.scripts[info.salIndex]['updated'] = True

            # delete remote if script is done
            if (self.scripts[info.salIndex]['process_state'] >= ScriptProcessState.DONE and
                    self.scripts[info.salIndex]['remote'] is not None):
                del self.scripts[info.salIndex]['remote']
                self.scripts[info.salIndex]['remote'] = None

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
                          self.monitor_script_checkpoint(salindex)],
                         return_when=asyncio.FIRST_COMPLETED))
        for task in unfinished:
            task.cancel()

    async def monitor_script_log(self, salindex):
        """Coroutine to monitor a script logger.

        Parameters
        ----------
        salindex : int

        """
        if self.scripts[salindex]['remote'] is None:
            raise RuntimeError(f"No remote for script {salindex}")

        self.log.debug("Waiting for heartbeat from script")
        await self.scripts[salindex]['remote'].evt_heartbeat.next(flush=False,
                                                                  timeout=self.cmd_timeout)

        while True:

            if self.scripts[salindex]["process_state"] >= ScriptProcessState.DONE:
                return
            log_message = await self.scripts[salindex]['remote'].evt_logMessage.next(
                flush=False,
                timeout=self.cmd_timeout)
            self.log.log(log_message.level,
                         f'[{salindex}]:{log_message.message}{log_message.traceback}')

    async def monitor_script_info(self, salindex):
        """Coroutine to monitor the state of a script.

        Parameters
        ----------
        salindex : int

        """

        while True:
            info = await self.queue.evt_script.next(flush=False,
                                                    timeout=self.cmd_timeout)
            self.parse_script_info(info)
            if info.salIndex == salindex:
                print(self.parse_info(self.get_script_info(salindex)))
            else:
                self.log.debug(self.parse_info(self.get_script_info(salindex)))
            if info.processState >= ScriptProcessState.DONE:
                return

    async def monitor_script_checkpoint(self, salindex):
        """Coroutine to monitor check a script checkpoint.

        Parameters
        ----------
        salindex : int

        """
        while True:
            state = await self.scripts[salindex]['remote'].evt_state.next(flush=False,
                                                                          timeout=self.cmd_timeout)
            self.log.debug(f"[{salindex}]:[checkpoint]{state.lastCheckpoint}")
