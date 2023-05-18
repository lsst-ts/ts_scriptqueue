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

__all__ = ["QueueModel"]

import asyncio
import collections
import copy
import inspect
import os
import pathlib
import signal

import astropy.time
import psutil
from lsst.ts import salobj
from lsst.ts.idl.enums.Script import ScriptState
from lsst.ts.idl.enums.ScriptQueue import Location
from lsst.ts.utils import index_generator

from . import utils
from .script_info import ScriptInfo

# Standard timeout (seconds). Long enough to perform any reasonable operation,
# including starting a CSC or loading a script (seconds)
STD_TIMEOUT = 60

MIN_SAL_INDEX = 1000
MAX_HISTORY = 400


class Scripts:
    """Struct to hold relative paths to scripts.

    Parameters
    ----------
    standard : ``iterable`` of `str`
        Relative paths to standard SAL scripts
    external : ``iterable`` of `str`
        Relative paths to external SAL scripts
    """

    def __init__(self, standard, external):
        self.standard = standard
        self.external = external


class ScriptKey:
    """Key with which to find ScriptInfo in the queue.

    Parameters
    ----------
    index : `int`
        Index of script. This must be unique among all Script SAL
        components that are currently running.
    """

    def __init__(self, index):
        self.index = int(index)

    def __hash__(self):
        return self.index

    def __eq__(self, other):
        return self.index == other.index

    def __ne__(self, other):
        return not (self == other)

    def __repr__(self):
        return f"ScriptKey(index={self.index})"


class QueueModel:
    """Code to load and configure scripts; implementation for ScriptQueue.

    Parameters
    ----------
    domain : `salobj.lsst.ts.salobj.Domain`
        DDS domain; typically ``ScriptQueue.domain``
    log : `logging.Logger`
        Parent logger.
    standardpath : `str`, `bytes` or `os.PathLike`
        Path to standard SAL scripts.
    externalpath : `str`, `bytes` or `os.PathLike`
        Path to external SAL scripts.
    next_visit_callback : ``coroutine`` (optional)
        Coroutine to call when a script gets a new group ID.
        It receives one argument: a `ScriptInfo`.
        This is separate from script_callback to make it easier
        to output the ``nextVisit`` event.
    next_visit_canceled_callback : ``coroutine`` (optional)
        Coroutine to call when a script loses its group ID.
        It receives one argument: a `ScriptInfo` with group_id not yet cleared.
    queue_callback : ``coroutine`` (optional)
        Coroutine to call when the queue state changes.
        It receives no arguments.
    script_callback : ``coroutine`` (optional)
        Coroutine to call when information about a script changes.
        It receives one argument: a `ScriptInfo`.
        This is not called if the only change is to the group ID; see
        ``next_visit_callback`` and ``next_visit_canceled_callback`` for that.
    min_sal_index : `int` (optional)
        Minimum SAL index for Script SAL components
    max_sal_index : `int` (optional)
        Maximum SAL index for Script SAL components
    verbose : `bool` (optional)
        If True then print log messages from scripts to stdout.

    Raises
    ------
    ValueError
        If ``standardpath`` or ``externalpath`` does not exist.
    """

    def __init__(
        self,
        domain,
        log,
        standardpath,
        externalpath,
        next_visit_callback=None,
        next_visit_canceled_callback=None,
        queue_callback=None,
        script_callback=None,
        min_sal_index=MIN_SAL_INDEX,
        max_sal_index=salobj.MAX_SAL_INDEX,
        verbose=False,
    ):
        if not os.path.isdir(standardpath):
            raise ValueError(f"No such dir standardpath={standardpath}")
        if not os.path.isdir(externalpath):
            raise ValueError(f"No such dir externalpath={externalpath}")

        for arg, arg_name in (
            (next_visit_callback, "next_visit_callback"),
            (next_visit_canceled_callback, "next_visit_canceled_callback"),
            (queue_callback, "queue_callback"),
            (script_callback, "script_callback"),
        ):
            if arg is not None and not inspect.iscoroutinefunction(arg):
                raise TypeError(f"{arg_name}={arg} must be a coroutine or None")

        self.domain = domain
        self.log = log.getChild("QueueModel")
        self.standardpath = os.path.abspath(standardpath)
        self.externalpath = os.path.abspath(externalpath)
        self.next_visit_callback = next_visit_callback
        self.next_visit_canceled_callback = next_visit_canceled_callback
        self.queue_callback = queue_callback
        self.script_callback = script_callback
        self.min_sal_index = min_sal_index
        self.max_sal_index = max_sal_index
        self.verbose = verbose
        # queue of ScriptInfo instances
        self.queue = collections.deque()
        self.history = collections.deque(maxlen=MAX_HISTORY)
        self.current_script = None
        self._running = True
        self._enabled = False
        self._index_generator = index_generator(imin=min_sal_index, imax=max_sal_index)
        self._scripts_being_stopped = set()
        # use index=0 so we get messages for all scripts
        self.remote = salobj.Remote(
            domain=domain, name="Script", index=0, evt_max_history=0
        )
        self.remote.evt_metadata.callback = self._script_metadata_callback
        self.remote.evt_state.callback = self._script_state_callback
        if self.verbose:
            self.remote.evt_logMessage.callback = self._log_message_callback
        self.start_task = self.remote.start_task

    async def add(self, script_info, location, location_sal_index):
        """Add a script to the queue.

        Launch the script in a new subprocess and wait for the subprocess
        to start. Start a background task to configure the script
        when it is ready.

        Parameters
        ----------
        script_info : `ScriptInfo`
            Script info.
        location : `Location`
            Location of script.
        location_sal_index : `int`
            SAL index of script that ``location`` is relative to.

        Raises
        ------
        ValueError
            If the script does not exist or is not executable.
        ValueError
            If ``location`` is not one of the supported enum values.
        ValueError
            If location is relative and a script at ``location_sal_index``
            is not queued.
        """
        # do this first to make sure the path exists
        fullpath = self.make_full_path(script_info.is_standard, script_info.path)

        await self._insert_script(
            script_info=script_info,
            location=location,
            location_sal_index=location_sal_index,
        )

        coro = script_info.start_loading(fullpath=fullpath)
        await asyncio.wait_for(coro, timeout=STD_TIMEOUT)

    @property
    def current_index(self):
        """SAL index of the current script, or 0 if none."""
        return 0 if self.current_script is None else self.current_script.index

    @property
    def history_indices(self):
        """SAL indices of scripts on the history queue."""
        return [script_info.index for script_info in self.history]

    @property
    def queue_indices(self):
        """SAL indices of scripts on the queue."""
        return [script_info.index for script_info in self.queue]

    async def close(self):
        """Shut down the queue, terminate all scripts and free resources."""
        await self.terminate_all()

    def find_available_scripts(self):
        """Find available scripts.

        Returns
        -------
        scripts : `Scripts`
            Paths to standard and external scripts.
        """
        return Scripts(
            standard=utils.find_public_scripts(self.standardpath),
            external=utils.find_public_scripts(self.externalpath),
        )

    def get_queue_index(self, sal_index):
        """Get queue index of a script on the queue.

        Parameters
        ----------
        sal_index : `int`
            SAL index of script.

        Raises
        ------
        ValueError
            If the script cannot be found on the queue.
        """
        key = ScriptKey(sal_index)
        return self.queue.index(key)

    def get_script_info(self, sal_index, search_history):
        """Get information about a script.

        Search current script, the queue and history.

        Parameters
        ----------
        sal_index : `int`
            SAL index of script.

        Raises
        ------
        ValueError
            If the script cannot be found.
        """
        if self.current_script and self.current_script.index == sal_index:
            return self.current_script
        key = ScriptKey(sal_index)
        try:
            return self.queue[self.queue.index(key)]
        except ValueError:
            if search_history:
                pass
            else:
                raise
        return self.history[self.history.index(key)]

    def make_full_path(self, is_standard, path):
        """Make a full path from path and is_standard and check that
        it points to a runnable script.

        Parameters
        ----------
        is_standard : `bool`
            Is this a standard (True) or external (False) script?
        path : `str`, `bytes` or `os.PathLike`
            Path to script, relative to standard or external root dir.

        Returns
        -------
        fullpath : `pathlib.Path`
            The full path to the script.

        Raises
        ------
        ValueError
            If The full path is not in the appropriate root path
            (``standardpath`` or ``externalpath``, depending on
            ``is_standard``).
        ValueError
            If the script does not exist or is not a file,
            is invisible (name starts with ".")
            or private (name starts with "_"),
            or is not executable.
        """
        root = pathlib.Path(self.standardpath if is_standard else self.externalpath)
        fullpath = root.joinpath(path)
        if root not in fullpath.parents:
            raise ValueError(f"path {path} is not relative to {root}")
        if not fullpath.is_file():
            raise ValueError(f"Cannot find script {fullpath}.")
        if fullpath.name[0] in (".", "_"):
            raise ValueError(f"script {path} is invisible or private")
        if not os.access(fullpath, os.X_OK):
            raise ValueError(f"Script {fullpath} is not executable.")
        return fullpath

    async def move(self, sal_index, location, location_sal_index):
        """Move a script within the queue.

        Parameters
        ----------
        sal_index : `int`
            SAL index of script to move.
        location : `Location`
            Location of script.
        location_sal_index : `int`
            SAL index of script that ``location`` is relative to.

        Raises
        ------
        ValueError
            If the script is not queued.
        ValueError
            If ``location`` is not one of the supported enum values.
        ValueError
            If location is relative and a script at ``location_sal_index``
            is not queued.
        """
        if (
            location in (Location.BEFORE, Location.AFTER)
            and location_sal_index == sal_index
        ):
            # this is a no-op, and is not properly handled by _insert_script,
            # but first make sure the script is on the queue
            self.get_queue_index(sal_index)
            await self._update_queue()
            return

        old_queue = copy.copy(self.queue)
        script_info = self.pop_script_info(sal_index)
        try:
            await self._insert_script(
                script_info=script_info,
                location=location,
                location_sal_index=location_sal_index,
            )
        except Exception:
            self.queue = old_queue
            raise

    @property
    def next_sal_index(self):
        """Get the next available SAL Script index."""
        return next(self._index_generator)

    def pop_script_info(self, sal_index):
        """Remove and return information about a script on the queue.

        Parameters
        ----------
        sal_index : `int`
            SAL index of script.

        Raises
        ------
        ValueError
            If the script cannot be found on the queue.
        """
        queue_index = self.get_queue_index(sal_index)
        script_info = self.queue[queue_index]
        del self.queue[queue_index]
        return script_info

    async def requeue(self, sal_index, seq_num, location, location_sal_index):
        """Requeue a script.

        Add a script that is a copy of an existing script,
        including the same configuration.

        Launch the script in a new subprocess and wait for the subprocess
        to start. Start a background task to configure the script
        when it is ready.

        Parameters
        ----------
        domain : `lsst.ts.salobj.Domain`
            DDS domain.
        sal_index : `int`
            SAL index of script to requeue.
        seq_num : `int`
            Command sequence number; recorded in the script info.
        location : `Location`
            Location of script.
        location_sal_index : `int`
            SAL index of script that ``location`` is relative to.

        Raises
        ------
        ValueError
            If the script ``sal_index`` cannot be found.
        ValueError
            If ``location`` is not one of the supported enum values.
        ValueError
            If location is relative and a script ``location_sal_index``
            is not queued.

        Returns
        -------
        script_info : `ScriptInfo`
            Info for the requeued script.
        """
        old_script_info = self.get_script_info(sal_index, search_history=True)

        script_info = ScriptInfo(
            log=self.log,
            remote=self.remote,
            index=self.next_sal_index,
            seq_num=seq_num,
            is_standard=old_script_info.is_standard,
            path=old_script_info.path,
            config=old_script_info.config,
            descr=old_script_info.descr,
            verbose=self.verbose,
        )
        await self.add(
            script_info=script_info,
            location=location,
            location_sal_index=location_sal_index,
        )
        return script_info

    async def stop_scripts(self, sal_indices, terminate):
        """Stop one or more queued scripts and/or the current script.

        Silently ignores scripts that cannot be found or are already stopped.

        Parameters
        ----------
        sal_indices : ``iterable`` of `int`
            SAL indices of scripts to stop.
            Scripts whose indices are not found are ignored.
        terminate : `bool`
            Terminate a running script instead of giving it time
            to stop gently?
        """
        self._scripts_being_stopped = set()
        script_info_list = []
        for index in sal_indices:
            try:
                script_info = self.get_script_info(index, search_history=False)
            except ValueError:
                continue
            if script_info.process_done:
                continue
            self._scripts_being_stopped.add(index)
            script_info_list.append(script_info)

        try:
            for script_info in script_info_list:
                if script_info.process_done:
                    continue
                if script_info.running and not terminate:
                    await self.stop_one_script(script_info)
                else:
                    await self.terminate_one_script(script_info)
        finally:
            self._scripts_being_stopped = set()

    async def stop_one_script(self, script_info):
        """Stop a queued or running script, giving it time to clean up.

        First send the script the ``stop`` command, giving that ``timeout``
        a few seconds to succeed or fail. If necessary, terminate the script
        by sending SIGTERM to the process.

        This is slower and than `terminate`, but gives the script
        a chance to clean up.
        If successful, the script is removed from the queue.

        Parameters
        ----------
        script_info : `ScriptInfo`
            Script info for script stop.
        """
        if script_info.process_done:
            return
        if script_info.script_state == ScriptState.RUNNING:
            # process is running, so send the "stop" command
            try:
                await script_info.remote.cmd_stop.set_start(
                    salIndex=script_info.index, timeout=STD_TIMEOUT
                )
                # give the process time to terminate
                await asyncio.wait_for(script_info.process.wait(), timeout=STD_TIMEOUT)
                # let the script be removed or moved
                await asyncio.sleep(0)
                return
            except Exception:
                # oh well, terminate it instead
                pass
        await self.terminate_one_script(script_info)

    async def terminate_one_script(self, script_info):
        """Terminate a queued or running script.

        If successful (as it will be, unless the script catches SIGTERM),
        the script is removed from the the queue.
        If you have time please try `stop` first, as that gives the
        script a chance to clean up. If `stop` fails then the script will
        still be terminated.

        Parameters
        ----------
        script_info : `ScriptInfo`
            Script info for script terminate.

        Raises
        ------
        ValueError
            If a script is not queued or running.
        """
        if script_info.process_done:
            return
        # Clear the group ID, if appropriate. Do not command the script,
        # since we are about to kill it anyway.
        if (
            self.queue
            and self.queue[0].index == script_info.index
            and script_info.group_id
            or script_info.setting_group_id
        ):
            await self.clear_group_id(script_info=script_info, command_script=False)

        await script_info.terminate()

        # let the script be removed or moved
        await asyncio.sleep(0)

    @property
    def enabled(self):
        """Get enabled state.

        True if ScriptQueue is in the enabled state, False otherwise.
        """
        return self._enabled

    async def set_enable(self, enabled):
        """Set enabled state."""
        was_enabled = self._enabled
        self._enabled = bool(enabled)
        if self.enabled != was_enabled:
            await self._update_queue()

    @property
    def running(self):
        """Get or set running state.

        If set False the queue pauses.
        """
        return self._running

    async def set_running(self, run):
        """Set running state."""
        was_running = self._running
        self._running = bool(run)
        if self._running != was_running:
            await self._update_queue(pause_on_failure=False)

    @staticmethod
    def next_group_id():
        """Get the next group ID.

        The group ID is the current TAI date and time as a string in ISO
        format. It has T separating date and time and no time zone suffix.
        Here is an example:
        "2020-01-17T22:59:05.721"
        """
        return astropy.time.Time.now().tai.isot

    async def terminate_all(self):
        """Terminate all scripts and return info for the ones terminated.

        Returns
        -------
        info_list : `list` [`ScriptInfo`]
            ScriptInfo for each terminated script.
            Excludes zombie script processes (which should be rare),
            because ScriptInfo is not available for those.
        """
        info_list = []
        for script_info in self.queue.copy():
            did_terminate = await script_info.terminate()
            if did_terminate:
                info_list.append(script_info)
        if self.current_script:
            did_terminate = await self.current_script.terminate()
            if did_terminate:
                info_list.append(self.current_script)

        # Handle zombie script processes. These should be rare,
        # but it is possible for `ScriptInfo.terminate` to time out
        # before it actually terminates the process.
        main_process = psutil.Process(os.getpid())
        child_processes = main_process.children(recursive=True)

        for process in child_processes:
            self.log.info(f"Killing zombie script process {process.pid}.")
            try:
                process.send_signal(signal.SIGTERM)
            except psutil.NoSuchProcess:
                pass

        return info_list

    async def _insert_script(self, script_info, location, location_sal_index):
        """Insert a script info into the queue.

        Parameters
        ----------
        script_info : `ScriptInfo`
            Script info.
        location : `Location`
            Location of script.
        location_sal_index : `int`
            SAL index of script that ``location`` is relative to.

        Raises
        ------
        ValueError
            If ``location`` is not one of the supported enum values.
        ValueError
            If location is relative and a script at ``location_sal_index``
            is not queued.
        """
        if location == Location.FIRST:
            self.queue.appendleft(script_info)
        elif location == Location.LAST:
            self.queue.append(script_info)
        elif location in (Location.BEFORE, Location.AFTER):
            location_queue_index = self.get_queue_index(location_sal_index)
            if location == Location.AFTER:
                location_queue_index += 1
            if location_queue_index >= len(self.queue):
                self.queue.append(script_info)
            else:
                self.queue.insert(location_queue_index, script_info)
        else:
            raise ValueError(f"Unknown location {location}")

        script_info.callback = self._script_info_callback
        await self._update_queue()

    async def _remove_script(self, sal_index):
        """Remove a script from the queue."""
        key = ScriptKey(sal_index)
        if self.current_script and self.current_script == key:
            if sal_index in self._scripts_being_stopped:
                self._scripts_being_stopped.remove(sal_index)
                if not self._scripts_being_stopped:
                    await self._update_queue()
                # else let removal finish before starting the next job,
                # because it messes up the queue state callbacks otherwise
            else:
                # removal is handled by _update_queue
                await self._update_queue()
        elif key in self.queue:
            script_info = self.pop_script_info(sal_index)
            self.history.appendleft(script_info)
            if sal_index in self._scripts_being_stopped:
                self._scripts_being_stopped.remove(sal_index)
                if not self._scripts_being_stopped:
                    # that was the last script to stop;
                    # now show the queue state
                    await self._update_queue()
            else:
                await self._update_queue()

    def _log_message_callback(self, data):
        """Print Script logMessage data to stdout.

        To use: if self.verbose is true then set this as a callback
        for the logMessage event.

        Parameters
        ----------
        data : `Script_logevent_logMessageC`
            Log message data.
        """
        print(
            f"Script {data.salIndex} log message={data.message!r}; "
            f"level={data.level}; traceback={data.traceback!r}"
        )

    async def clear_group_id(self, script_info, command_script):
        """Clear the group ID of the specified script, if appropriate.

        Clear the group ID of the specified script if the group ID
        is set or is being set.

        Parameters
        ----------
        script_info : `ScriptInfo`
            Script info.
        command_script : `bool`
            If True then issue the setGroupId command to the script
            (in the background).
            The only time you would set this False is if you are about
            to terminate the script.
        """
        self.log.debug(
            f"Clear group info for {script_info.index}; command_script={command_script}"
        )
        if self.next_visit_canceled_callback:
            try:
                await self.next_visit_canceled_callback(script_info)
            except Exception:
                self.log.exception("next_visit_canceled_callback failed; continuing")
        script_info.clear_group_id(command_script=command_script)

    async def set_group_id(self, script_info):
        """Set or clear the group ID for a script.

        Parameters
        ----------
        script_info : `ScriptInfo`
            Script info.

        Raises
        ------
        RuntimeError
            If the group ID cannot be set.
        """
        group_id = self.next_group_id()
        self.log.debug(f"set_group_id of {script_info.index} to {group_id}")
        await script_info.set_group_id(group_id)
        if self.next_visit_callback:
            try:
                await self.next_visit_callback(script_info)
            except Exception:
                self.log.exception("next_visit_callback failed; continuing")

    def _script_info_from_data(self, event_name, data):
        """Get script info for the script specified in Script event data

        Parameters
        ----------
        event_name : `str`
            Name of event, for logging a warning.
        data : ``Script event data``
            Data from a script event. The salIndex field is read.

        Returns
        -------
        script_info_or_None : `ScriptInfo` or `None`
            The script info, if found, else None.
        """
        sal_index = data.salIndex
        if sal_index < self.min_sal_index or sal_index > self.max_sal_index:
            # not a script for this QueueModel
            return None
        try:
            script_info = self.get_script_info(
                sal_index=sal_index, search_history=False
            )
        except ValueError:
            self.log.warning(
                f"QueueModel got a Script {event_name} event for script {sal_index}, "
                "which is neither running nor on the queue"
            )
            return None
        return script_info

    async def _script_metadata_callback(self, data):
        script_info = self._script_info_from_data(event_name="metadata", data=data)
        if script_info:
            script_info.metadata = data

    async def _script_state_callback(self, data):
        script_info = self._script_info_from_data(event_name="state", data=data)
        if script_info:
            await script_info._script_state_callback(data)

    async def _script_info_callback(self, script_info):
        """ScriptInfo callback."""
        if self.script_callback:
            try:
                await self.script_callback(script_info)
            except Exception:
                self.log.exception("script_callback failed; continuing")

        if script_info.process_done or script_info.terminated:
            asyncio.create_task(self._remove_script(script_info.index))
            return

        if (
            self.queue
            and self.queue[0].index == script_info.index
            and script_info.configured
        ):
            # This script is next in line and may need its group ID set
            # or be ready to be run.
            await self._update_queue(force_callback=False)

    async def _update_queue(self, force_callback=True, pause_on_failure=True):
        """Call whenever the queue changes state.

        If the current script is done, move it to the history queue.
        If the next script is ready to run, make it the current script
        and start running it.

        Parameters
        ----------
        force_callback : `bool` (optional)
            If True then always call ``queue_callback``;
            otherwise call ``queue_callback`` if the queue changes.
        pause_on_failure : `bool` (optional)
            This affects the behavior if the current script has failed:

            * If True, leave it as the current script and pause the queue.
            * If False and the queue is running, move the current
              script to history. This is intended for use by ``running``
              to allow the queue to resume after pausing on failure.
        """
        initial_current_index = self.current_index
        initial_queue_indices = self.queue_indices
        initial_history_indices = self.history_indices
        if self.current_script:
            if self.current_script.process_done:
                if self.current_script.failed and (
                    pause_on_failure or not self.running
                ):
                    # set `_running` instead of `running` so as to
                    # not trigger _update_queue
                    self._running = False
                else:
                    self.history.appendleft(self.current_script)
                    self.current_script = None

        if self.enabled and self.running:
            # Clear done scripts from the top of the queue.
            # Done scripts on the queue is rare, but can happen.
            while self.queue:
                # Don't pop here because we leave the script
                # on the queue if it's not yet runnable.
                script_info = self.queue[0]
                if script_info.process_done or script_info.terminated:
                    self.history.appendleft(script_info)
                    self.queue.popleft()
                    continue
                if (
                    not self.current_script
                    and script_info.runnable
                    and script_info.index not in self._scripts_being_stopped
                ):
                    self.current_script = script_info
                    self.queue.popleft()
                    script_info.run()
                break

            # Set the group ID of the top script, if needed
            # and clear the group ID of any other scripts, if needed
            is_top = True
            for script_info in self.queue:
                if is_top:
                    if script_info.needs_group_id:
                        asyncio.create_task(self.set_group_id(script_info))
                    is_top = False
                else:
                    if script_info.group_id or script_info.setting_group_id:
                        await self.clear_group_id(script_info, command_script=True)

        if self.queue_callback is not None and (
            force_callback
            or self.current_index != initial_current_index
            or self.queue_indices != initial_queue_indices
            or self.history_indices != initial_history_indices
        ):
            try:
                await self.queue_callback()
            except Exception:
                self.log.exception("queue_callback failed; continuing")
