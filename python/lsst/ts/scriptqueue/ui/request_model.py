# This file is part of ts_scriptqueue.
#
# Developed for the LSST Telescope and Site Systems.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

__all__ = ["RequestModel"]

import asyncio
import logging

from lsst.ts import salobj
from lsst.ts.idl.enums.ScriptQueue import Location, ScriptProcessState

from .queue_state import QueueState


class RequestModel:
    """Model for the script queue user interface.

    It stores the current state of the queue and of scripts in the queue.
    It provides methods to update its internal information and callback
    functions that can be scheduled to update in an event loop.

    It also provides methods to send request to the queue and methods to
    monitor the state of an executing script. It relies heavily on the ability
    of SAL to retrieve late joiner information.
    """

    def __init__(self, index):

        self.log = logging.getLogger(__name__)

        self.domain = salobj.Domain()
        self.queue = salobj.Remote(self.domain, "ScriptQueue", index)
        self.evt_loop = asyncio.get_event_loop()
        # Wait for the Remote to start
        self.run(self._wait_queue_remote_start())

        self.cmd_timeout = 120.0
        self.max_lost_heartbeats = 5
        """Specify the tolerance in subsequent heartbeats lost when
        monitoring a script.
        """

        self.state = QueueState()
        """Store the overall state of the queue.
        """

        # Initialize information about the queue. If the queue is not in
        # enable or offline state it will be left unchanged.
        if self.query_queue_state() < 0:
            self.log.warning(
                "Could not get state of the queue. Make sure queue is running and is "
                "in enabled state."
            )
            return

        self.query_scripts_info()  # Ask the queue for the state of all scripts
        self.update_scripts_info()  # Update the internal information

    @property
    def summary_state(self):
        """Return summary state"""
        data = self.queue.evt_summaryState.get()
        if data is None:
            return 0
        else:
            return salobj.State(data.summaryState)

    async def _wait_queue_remote_start(self):
        """Wait for the queue remote to start.
        """
        await self.queue.start_task

    def enable_queue(self):
        """Enable the script queue.

        It will try to identify the current state of the queue and if it fails,
        assumes the queue is in STANDBY.
        """
        self.run(
            salobj.set_summary_state(
                remote=self.queue, state=salobj.State.ENABLED, timeout=self.cmd_timeout
            )
        )

    def get_scripts(self):
        """Return available scripts.

        Returns
        -------
        available_scripts : `dict` [`str`, `list` [`str`]]
            A dictionary containing two items:

            * "standard": a list of paths to standard scripts
            * "external": a list of paths to external scripts
            The paths are as reported by the script queue
            and used when adding a script.
        """
        self.queue.evt_availableScripts.flush()

        self.run(self.queue.cmd_showAvailableScripts.start(timeout=self.cmd_timeout))

        script_list = self.run(
            self.queue.evt_availableScripts.next(flush=False, timeout=self.cmd_timeout)
        )

        return {
            "external": script_list.external.split(":"),
            "standard": script_list.standard.split(":"),
        }

    def pause_queue(self):
        """Pause the queue."""
        self.queue.evt_queue.flush()
        self.run(self.queue.cmd_pause.start(timeout=self.cmd_timeout))
        self.run(self.queue.evt_queue.next(flush=False, timeout=self.cmd_timeout))
        self.state.update(self.queue.evt_queue.get())

    def resume_queue(self):
        """Resume queue operation."""
        self.queue.evt_queue.flush()
        self.run(self.queue.cmd_resume.start(timeout=self.cmd_timeout))
        self.run(self.queue.evt_queue.next(flush=False, timeout=self.cmd_timeout))
        self.state.update(self.queue.evt_queue.get())

    def quit_queue(self):
        """Tell the script queue to quit.
        """
        self.run(
            salobj.set_summary_state(
                remote=self.queue, state=salobj.State.OFFLINE, timeout=self.cmd_timeout
            )
        )

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
        """Get the state of the queue and the scripts running on the queue.

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
        ackcmd = self.run(
            self.queue.cmd_add.set_start(
                isStandard=is_standard,
                path=path,
                config=config,
                location=Location.LAST,
                timeout=self.cmd_timeout,
            )
        )

        self.state.add_script(int(ackcmd.result))

        return int(ackcmd.result)

    def set_queue_log_level(self, level):
        """Set queue log level.

        Parameters
        ----------
        level : int
            Log level; error=40, warning=30, info=20, debug=10
        """
        self.run(
            self.queue.cmd_setLogLevel.set_start(level=level, timeout=self.cmd_timeout)
        )

    def set_script_log_level(self, index, level):
        """Set the log level for a script.

        Parameters
        ----------
        index : int
            Script index.
        level : int
            Log level.

        Raises
        ------
        KeyError
            If the script is not on the queue.
        """
        if self.state.scripts[index]["remote"] is None:
            self.get_script_remote(index)

        remote = self.state.scripts[index]["remote"]
        self.run(
            remote.cmd_setLogLevel.set_start(level=level, timeout=self.cmd_timeout)
        )

    def listen_heartbeat(self):
        """Listen for queue heartbeats."""
        return self.run(
            self.queue.evt_heartbeat.next(flush=True, timeout=self.cmd_timeout)
        )

    def get_script_remote(self, salindex):
        """Listen for a script log messages."""
        self.update_scripts_info()
        info = self.state.scripts[salindex]

        if (
            info["process_state"] < ScriptProcessState.DONE
            and self.state.scripts[salindex]["remote"] is None
        ):
            self.log.debug("Starting script remote")
            remote = salobj.Remote(self.domain, "Script", salindex)
            self.state.scripts[salindex]["remote"] = remote
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
        """Update the internal queue state from the queue event.

        Run `get_oldest` in `self.queue.evt_queue` until there is nothing left
        and return a count of the number of entries read.

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
        queue_state = self.queue.evt_summaryState.get()
        if queue_state is None:
            pass  # try next
        elif queue_state.summaryState != salobj.State.ENABLED:
            self.log.warning(
                f"Queue summary state is {salobj.State(queue_state.summaryState)!r}. "
                f"Enable it first and try again."
            )
            return -1

        queue_coro = self.queue.evt_queue.next(flush=False, timeout=self.cmd_timeout)

        try:
            queue = self.run(queue_coro)
        except Exception:
            self.log.exception("Could not get state of the queue.")
            return -1

        self.state.update(queue)

        self.update_scripts_info()

        return 0

    def script_info_callback(self, info):
        """A callback function to monitor the state of the scripts.

        Parameters
        ----------
        info : ``ScriptQueue.evt_script.DataType``
            Script data.
        """
        self.state.update_script_info(info)

    def update_scripts_info(self):
        """Update internal script info from the `script` event.

        Run `get_oldest` in `self.queue.evt_script`
        and pass it to `self.parse_script_info`
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
            try:
                self.run(
                    self.queue.cmd_showScript.set_start(
                        salIndex=salindex, timeout=self.cmd_timeout
                    )
                )
            except salobj.AckError as ack_err:
                self.log.error(
                    f"Could not get info on script {salindex}. "
                    f"Failed with ack.result={ack_err.ack.result}"
                )

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

        parsed_str = (
            f"[salIndex:{info['index']}]"
            f"[{info['type']}]"
            f"[path:{info['path']}]"
            f"[timestamp_process_start:{round(info['timestamp_process_start'], 2)}]"
            f"[timestamp_run_start:{round(info['timestamp_run_start'], 2)}]"
            f"[timestamp_process_end:{round(info['timestamp_process_end'], 2)}]"
            f"[{str(info['script_state'])}]"
            f"[{str(info['process_state'])}]"
        )
        return parsed_str

    def monitor_script(self, salindex):
        """Monitor state and log from script.

        Parameters
        ----------
        salindex : int

        """
        self.get_script_remote(salindex)
        finished, unfinished = self.evt_loop.run_until_complete(
            asyncio.wait(
                [
                    self.monitor_script_info(salindex),
                    self.monitor_script_log(salindex),
                    self.monitor_script_checkpoint(salindex),
                    self.monitor_script_heartbeat(salindex),
                ],
                return_when=asyncio.FIRST_COMPLETED,
            )
        )
        for task in unfinished:
            task.cancel()

    async def monitor_script_log(self, salindex):
        """Coroutine to monitor a script logger.

        Parameters
        ----------
        salindex : int

        """
        if self.state.scripts[salindex]["remote"] is None:
            raise RuntimeError(f"No remote for script {salindex}")

        while True:

            if self.state.scripts[salindex]["process_state"] >= ScriptProcessState.DONE:
                return
            # Don't set a timeout because we cannot predict when, if ever,
            # scripts will output log messages.
            log_message = await self.state.scripts[salindex][
                "remote"
            ].evt_logMessage.next(flush=False)
            self.log.log(
                log_message.level,
                f"[{salindex}]:{log_message.message}{log_message.traceback}",
            )

    async def monitor_script_info(self, salindex):
        """Coroutine to monitor the state of a script.

        Parameters
        ----------
        salindex : int

        """

        while True:
            # Don't set a timeout because we cannot predict when script state
            # will change.
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
            # Don't set a timeout because we cannot predict when, if ever,
            # scripts will output checkpoint events.
            state = await self.state.scripts[salindex]["remote"].evt_state.next(
                flush=False
            )
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

            if self.state.scripts[salindex]["process_state"] >= ScriptProcessState.DONE:
                self.log.debug(f"Script {salindex} finalized.")
                return
            elif self.state.scripts[salindex]["remote"] is None:
                self.log.debug(f"No remote for script {salindex}.")
                return
            elif nlost_subsequent > self.max_lost_heartbeats:
                self.log.error(
                    f"Script is not responding. Lost {nlost_subsequent} "
                    f"subsequent heartbeats. You may have to interrupt the script execution."
                    f"If this is an expected behaviour you should be able to restart the "
                    f"monitoring routine."
                )
                return

            try:
                await self.state.scripts[salindex]["remote"].evt_heartbeat.next(
                    flush=False, timeout=salobj.BaseScript.HEARTBEAT_INTERVAL * 3
                )
                nbeats += 1
                nlost_subsequent = 0
                self.log.debug(
                    f"[{salindex}]:[heartbeat:{nbeats}] - ["
                    f"total lost:{nlost_total}]"
                )
            except asyncio.TimeoutError:
                nlost_subsequent += 1
                nlost_total += 1
                self.log.warning(
                    f"[{salindex}]:[heartbeat:{nbeats}] - "
                    f"[total lost:{nlost_total} - "
                    f"subsequent lost: {nlost_subsequent}]:"
                    f"[Missing heartbeats from script.]"
                )
