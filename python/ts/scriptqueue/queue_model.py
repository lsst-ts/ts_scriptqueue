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

__all__ = ["QueueModel", "ScriptInfo"]

import asyncio
import collections
import copy
import os
import pathlib
import sys
import traceback

import SALPY_ScriptQueue
import salobj
from . import utils
from .script_info import ScriptInfo
from .base_script import ScriptState

_LOAD_TIMEOUT = 20  # seconds

MIN_SAL_INDEX = 1000
MAX_HISTORY = 1000


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
    standardpath : `str`, `bytes` or `os.PathLike`
        Path to standard SAL scripts.
    externalpath : `str`, `bytes` or `os.PathLike`
        Path to external SAL scripts.
    queue_callback : ``callable`` (optional)
        Function to call when the queue state changes.
        It receives no arguments.
    script_callback : ``callable`` (optional)
        Function to call when information about a script changes.
        It receives one argument: a `ScriptInfo`.
    min_sal_index : `int` (optional)
        Minimum SAL index for Script SAL components
    max_sal_index : `int` (optional)
        Maximum SAL index for Script SAL components

    Raises
    ------
    ValueError
        If ``standardpath`` or ``externalpath`` does not exist.
    """
    def __init__(self, standardpath, externalpath, queue_callback=None, script_callback=None,
                 min_sal_index=MIN_SAL_INDEX, max_sal_index=salobj.MAX_SAL_INDEX):
        if not os.path.isdir(standardpath):
            raise ValueError(f"No such dir standardpath={standardpath}")
        if not os.path.isdir(externalpath):
            raise ValueError(f"No such dir externalpath={externalpath}")
        if queue_callback and not callable(queue_callback):
            raise TypeError(f"queue_callback={queue_callback} is not callable")
        if script_callback and not callable(script_callback):
            raise TypeError(f"script_callback={script_callback} is not callable")

        self.standardpath = standardpath
        self.externalpath = externalpath
        self.queue_callback = queue_callback
        self.script_callback = script_callback
        # queue of ScriptInfo instances
        self.queue = collections.deque()
        self.history = collections.deque(maxlen=MAX_HISTORY)
        self.current_script = None
        self._running = True
        self._enabled = False
        self._index_generator = salobj.index_generator(imin=min_sal_index, imax=max_sal_index)

    async def add(self, script_info, location, location_sal_index):
        """Add a script to the queue.

        Start a script in a subprocess, set ``self.process`` to the
        resulting ``asyncio.Process``, and set ``self.process_task`` to an
        ``asyncio.Task`` that waits for the process to finish.
        Wait for the process to start.
        Configure the script.

        Parameters
        ----------
        script_info : `ScriptInfo`
            Script info.
        location : `int`
            One of SALPY_ScriptQueue.add_First, Last, Before or After.
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

        self._insert_script(script_info=script_info,
                            location=location,
                            location_sal_index=location_sal_index)

        coro = script_info.start_loading(fullpath=fullpath)
        await asyncio.wait_for(coro, _LOAD_TIMEOUT)

    @property
    def current_index(self):
        """Return the SAL index of the current script, or 0 if none."""
        return 0 if self.current_script is None else self.current_script.index

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

    def move(self, sal_index, location, location_sal_index):
        """Move a script within the queue.

        Parameters
        ----------
        sal_index : `int`
            SAL index of script to move.
        location : `int`
            One of SALPY_ScriptQueue.add_First, Last, Before or After.
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
        if location in (SALPY_ScriptQueue.add_Before, SALPY_ScriptQueue.add_After) \
                and location_sal_index == sal_index:
            # this is a no-op, and is not properly handled by _insert_script,
            # but first make sure the script is on the queue
            self.get_queue_index(sal_index)
            self._update_queue()
            return

        old_queue = copy.copy(self.queue)
        script_info = self.pop_script_info(sal_index)

        try:
            self._insert_script(script_info=script_info,
                                location=location,
                                location_sal_index=location_sal_index)
        except Exception:
            self.queue = old_queue
            raise

    @property
    def next_sal_index(self):
        """Get the next available SAL Script index.
        """
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

    async def requeue(self, sal_index, cmd_id, location, location_sal_index):
        """Requeue a script.

        Parameters
        ----------
        sal_index : `int`
            SAL index of script to requeue.
        cmd_id : `int`
            Command ID; recorded in the script info.
        location : `int`
            One of SALPY_ScriptQueue.add_First, Last, Before or After.
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
            index=self.next_sal_index,
            cmd_id=cmd_id,
            is_standard=old_script_info.is_standard,
            path=old_script_info.path,
            config=old_script_info.config,
            descr=old_script_info.descr,
        )
        await self.add(script_info=script_info,
                       location=location,
                       location_sal_index=location_sal_index)
        return script_info

    async def stop(self, sal_index, timeout=None):
        """Stop a queued or running script, giving it time to clean up.

        First send the script the ``stop`` command, giving that ``timeout``
        seconds to succeed or fail. If necessary, terminate the script
        by sending SIGTERM to the process.

        This is slower and than `terminate`, but gives the script
        a chance to clean up.
        If successful, the script is removed from the queue.

        Parameters
        ----------
        sal_index : `int`
            SAL index of script to move.
        timeout : `float` (optional)
            Time limit, in seconds. If None then no time limit.

        Raises
        ------
        ValueError
            If a script is not queued or running.
        """
        script_info = self.get_script_info(sal_index, search_history=False)
        if script_info.done or script_info.remote is None:
            return
        if script_info.remote.evt_state.get() == ScriptState.RUNNING:
            # process is running, so send the "stop" command
            try:
                await script_info.remote.cmd_stop.start(script_info.remote.cmd_stop.DataType(),
                                                        timeout=timeout)
                # give the process time to terminate
                await asyncio.wait_for(script_info.process.wait(), timeout=2)
                # let the script be removed or moved
                await asyncio.sleep(0)
                return
            except Exception:
                # oh well, terminate it instead
                pass
        script_info.terminate()
        if timeout is None:
            await script_info.process.wait()
        else:
            await asyncio.wait_for(script_info.process.wait(), timeout=timeout)
        # let the script be removed or moved
        await asyncio.sleep(0)

    async def terminate(self, sal_index):
        """Terminate a queued or running script by sending SIGTERM
        to the subprocess and wait for that to complete.

        If successful (as it will be, unless the script catches SIGTERM),
        the script is removed from the the queue.
        If you have time please try `stop` first, as that gives the
        script a chance to clean up.

        Parameters
        ----------
        sal_index : `int`
            SAL index of script to move.

        Raises
        ------
        ValueError
            If a script is not queued or running.
        """
        script_info = self.get_script_info(sal_index, search_history=False)
        did_terminate = script_info.terminate()
        if did_terminate:
            await script_info.process.wait()
            # let the script be removed or moved
            await asyncio.sleep(0)

    @property
    def enabled(self):
        """Get or set enabled state.

        True if ScriptQueue is in the enabled state, False otherwise.
        """
        return self._enabled

    @enabled.setter
    def enabled(self, enabled):
        was_enabled = self._enabled
        self._enabled = bool(enabled)
        if self.enabled != was_enabled:
            self._update_queue()

    @property
    def running(self):
        """Get or set running state.

        If set False the queue pauses.
        """
        return self._running

    @running.setter
    def running(self, run):
        was_running = self._running
        self._running = bool(run)
        if self._running != was_running:
            self._update_queue(pause_on_failure=False)

    def terminate_all(self):
        """Terminate all subprocesses and info for the ones terminated.

        Does not wait for termination to actually finish.
        """
        info_list = []
        for script_info in self.queue:
            did_terminate = script_info.terminate()
            if did_terminate:
                info_list.append(script_info)
        if self.current_script:
            did_terminate = self.current_script.terminate()
            if did_terminate:
                info_list.append(self.current_script)
        return info_list

    async def wait_terminate_all(self, timeout=10):
        """Awaitable version of terminate_all"""
        term_info_list = self.terminate_all()
        await asyncio.wait_for(asyncio.gather(*[info.process_task for info in term_info_list]), timeout)

    def _insert_script(self, script_info, location, location_sal_index):
        """Insert a script info into the queue

        Parameters
        ----------
        script_info : `ScriptInfo`
            Script info.
        location : `int`
            One of SALPY_ScriptQueue.add_First, Last, Before or After.
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
        if location == SALPY_ScriptQueue.add_First:
            self.queue.appendleft(script_info)
        elif location == SALPY_ScriptQueue.add_Last:
            self.queue.append(script_info)
        elif location in (SALPY_ScriptQueue.add_Before, SALPY_ScriptQueue.add_After):
            location_queue_index = self.get_queue_index(location_sal_index)
            if location == SALPY_ScriptQueue.add_After:
                location_queue_index += 1
            if location_queue_index >= len(self.queue):
                self.queue.append(script_info)
            else:
                self.queue.insert(location_queue_index, script_info)
        else:
            raise ValueError(f"Unknown location {location}")

        script_info.callback = self._script_callback
        self._update_queue()

    def _remove_script(self, sal_index):
        """Remove a script from the queue."""
        async def delete_shortly(sal_index):
            await asyncio.sleep(0)
            key = ScriptKey(sal_index)
            if self.current_script and self.current_script == key:
                # handled by _update_queue
                self._update_queue()
            elif key in self.queue:
                self.pop_script_info(sal_index)
                self._update_queue()

        asyncio.ensure_future(delete_shortly(sal_index))

    def _script_callback(self, script_info):
        """ScriptInfo callback."""
        if self.script_callback:
            try:
                self.script_callback(script_info)
            except Exception:
                traceback.print_exc(file=sys.stderr)

        if script_info.done:
            self._remove_script(script_info.index)
        elif self.enabled and self.running \
                and self.current_script is None and script_info.runnable \
                and self.queue and self.queue[0].index == script_info.index:
            # this script is next in line and ready to run
            self._update_queue(force_callback=False)

    def _update_queue(self, force_callback=True, pause_on_failure=True):
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
        if self.current_script:
            if self.current_script.done:
                if self.current_script.failed and (pause_on_failure or not self.running):
                    # set `_running` instead of `running` so as to
                    # not trigger _update_queue
                    self._running = False
                else:
                    self.history.appendleft(self.current_script)
                    self.current_script = None
            elif not force_callback:
                return

        if self.enabled and self.running and not self.current_script:
            # it is unlikely done scripts are on the queue,
            # but it can happen
            while self.queue:
                script_info = self.queue[0]
                if script_info.done:
                    self.queue.popleft()
                    continue
                if script_info.runnable:
                    self.current_script = script_info
                    self.queue.popleft()
                    script_info.run()
                break

        if self.queue_callback:
            try:
                self.queue_callback()
            except Exception:
                traceback.print_exc(file=sys.stderr)
