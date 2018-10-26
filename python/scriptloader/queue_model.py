# This file is part of scriptloader.
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
import warnings

import SALPY_Script
import SALPY_ScriptQueue
import salobj
from . import utils
from .base_script import ScriptState

_LOAD_TIMEOUT = 20  # seconds
_STATE_TIMEOUT = 15  # seconds; includes time to make Script SAL component
_CONFIGURE_TIMEOUT = 15  # seconds

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


class ScriptInfo:
    """Information about a loaded script.

    Parameters
    ----------
    index : `int`
        Index of script. This must be unique among all Script SAL
        components that are currently running.
    cmd_id : `int`
        Command ID; recorded in the script info.
    is_standard : `bool`
        Is this a standard (True) or external (False) script?
    path : `str`, `bytes` or `os.PathLike`
        Path to script, relative to standard or external root dir.
    config : `str`
        Configuration data as a YAML encoded string.
    descr : `str`
        A short explanation of why this script is being run.
    """
    def __init__(self, index, cmd_id, is_standard, path, config, descr):
        self._index = int(index)
        self.cmd_id = int(cmd_id)
        self.is_standard = bool(is_standard)
        self.path = str(path)
        self.config = config
        self.descr = descr
        self.salinfo = salobj.SalInfo(SALPY_Script, self.index)
        self.timestamp = 0
        """Time at which the script process was started. 0 before that."""
        self.duration = 0
        """Duration of ``task`` (seconds), or 0 if still running."""
        self.start_task = asyncio.Future()
        """Task that finishes when configuration starts.

        By the time start_task is done config_task and process_task
        will exist (instead of being None). Thus:

        * To wait for configuration to be done: first await start_task
          then await config_task.
        * To wait for a script to finish: first await start_task
          then await process_task:
        """
        self.remote = None
        """Remote which talks to the ``Script`` SAL component.
        None initially and after process exits."""
        self.process = None
        """Process in which the ``Script`` SAL component is loaded."""
        self.process_task = None
        """Task awaiting ``process.wait()``, or None if no process yet."""
        self.config_task = None
        """Task awaiting configuration to complete, or None if not yet configuring."""
        self._callback = None
        """Function to be called whenever the script's state changes.

        It receives one argument: this ScriptInfo.
        """
        self.run_started = False
        """True if the script was asked to start running"""

    @property
    def callback(self):
        """Set or get a function to call whenever the script
        state changes.

        It receives one argument: this ScriptInfo.
        """
        return self._callback

    @callback.setter
    def callback(self, callback):
        if not callable(callback):
            raise TypeError(f"callback={callback} is not callable")
        self._callback = callback

    @property
    def configured(self):
        return self.config_task is not None and self.config_task.done()

    @property
    def done(self):
        return self.process_task is not None and self.process_task.done()

    @property
    def index(self):
        """Get the script's SAL index"""
        return self._index

    def run(self):
        """Start the script running.

        Raises
        ------
        RuntimeError
            If the script cannot be run. Reasons could include:
            - The script has not yet been configured.
            - `run` was already called.
            - The script process is done.
        """
        if not self.runnable:
            raise RuntimeError("Script is not runnable")
        self.run_started = True
        asyncio.ensure_future(self.remote.cmd_run.start(self.remote.cmd_run.DataType()))

    @property
    def runnable(self):
        """Can the script be run?

        For a script to be runnable it must be configured,
        the process must not have finished,
        and ``run`` must not have been called.
        """
        return self.configured and not (self.done or self.run_started)

    async def start_loading(self, fullpath):
        """Start the script process and start a task that will configure
        the script when it is ready.

        fullpath : `str`, `bytes` or `os.PathLike`
            Full path to the script.
        """
        initialpath = os.environ["PATH"]
        scriptdir, scriptname = os.path.split(fullpath)
        try:
            os.environ["PATH"] = scriptdir + ":" + initialpath
            self.process = await asyncio.create_subprocess_exec(scriptname, str(self.index))
            self.timestamp = self.salinfo.manager.getCurrentTime()
            self.process_task = asyncio.ensure_future(self.process.wait())
            self.process_task.add_done_callback(self._run_callback)
            self.process_task.add_done_callback(self._cleanup)
            self.config_task = asyncio.ensure_future(self._configure())
            self.config_task.add_done_callback(self._run_callback)
            self.start_task.set_result(None)
        finally:
            os.environ["PATH"] = initialpath

    def terminate(self):
        """Terminate the script by sending SIGTERM.

        Does not wait for termination to finish; to do that use::

            `await script_info.process.wait()`

        Returns
        -------
        did_terminate : `bool`
            True if termination was requested,
            False if there is no process or it was already finished.

        Raises
        ------
        RuntimeError
            If we have no information about that script, meaning
            it already finished or was never loaded.
        """
        if self.process is None:
            return False
        if self.process.returncode is not None:
            return False
        self.process.terminate()
        return True

    def __eq__(self, other):
        return self.index == other.index

    def __ne__(self, other):
        return not (self == other)

    def __repr__(self):
        return f"ScriptInfo(index={self.index}, cmd_id={self.cmd_id}, " \
            f"is_standard={self.is_standard}, path={self.path}, " \
            f"config={self.config}, descr={self.descr})"

    def _add_remote(self):
        """Set the remote attribute.

        This takes awhile so run this in a thread.
        """
        self.remote = salobj.Remote(SALPY_Script, self.index)

    def _cleanup(self, returncode=None):
        """Clean up when the Script subprocess exits.

        Set the duration, cancel the config task if necessary,
        and delete the remote and the salinfo as they are no
        longer useful and consume resources.
        """
        if self.remote:
            self.duration = self.remote.salinfo.manager.getCurrentTime() - self.timestamp
        if self.config_task and not self.config_task.done():
            self.config_task.cancel()
        self.remote = None
        self.salinfo = None

    async def _configure(self):
        """Wait for the script to report UNCONFIGURED state and configure.
        """
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._add_remote)
            script_state = await self.remote.evt_state.next(flush=False, timeout=_STATE_TIMEOUT)
            if script_state.state != ScriptState.UNCONFIGURED:
                raise salobj.ExpectedError(f"Cannot configure script it is in state {script_state} "
                                           f"instead of {ScriptState.UNCONFIGURED}")

            config_data = self.remote.cmd_configure.DataType()
            config_data.config = self.config
            id_ack = await self.remote.cmd_configure.start(config_data, timeout=_CONFIGURE_TIMEOUT)
            if id_ack.ack.ack != self.remote.salinfo.lib.SAL__CMD_COMPLETE:
                raise salobj.ExpectedError(f"Configure command failed")
        except Exception:
            self.terminate()
            self.remote = None
            raise

    def _run_callback(self, state):
        if self.callback:
            self.callback(self)


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
        RuntimeError
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

    def get_script_info(self, sal_index):
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
            pass
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
        RuntimeError
            If The full path is not in the appropriate root path
            (``standardpath`` or ``externalpath``, depending on
            ``is_standard``).
        RuntimeError
            If the script does not exist or is not a file,
            is invisible (name starts with ".")
            or private (name starts with "_"),
            or is not executable.
        """
        root = pathlib.Path(self.standardpath if is_standard else self.externalpath)
        fullpath = root.joinpath(path)
        if root not in fullpath.parents:
            raise RuntimeError(f"path {path} is not relative to {root}")
        if not fullpath.is_file():
            raise RuntimeError(f"Cannot find script {fullpath}.")
        if fullpath.name[0] in (".", "_"):
            raise RuntimeError(f"script {path} is invisible or private")
        if not os.access(fullpath, os.X_OK):
            raise RuntimeError(f"Script {fullpath} is not executable.")
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
            # this is a no-op, but is not properly handled by _insert_script
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

    def remove(self, sal_index):
        """Remove a script from the queue and terminate it.

        Parameters
        ----------
        sal_index : `int`
            SAL index of script to move.

        Raises
        ------
        ValueError
            If a script is not queued or running.
        """
        if self.current_script and self.current_script.index == sal_index:
            script_info = self.current_script
        else:
            script_info = self.pop_script_info(sal_index)
        self._update_queue()
        script_info.terminate()

    def requeue(self, sal_index, cmd_id, location, location_sal_index):
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
        old_script_info = self.get_script_info(sal_index)
        script_info = ScriptInfo(
            index=sal_index,
            cmd_id=cmd_id,
            is_standard=old_script_info.is_standard,
            path=old_script_info.path,
            config=old_script_info.config,
            descr=old_script_info.descr,
        )
        self._insert_script(script_info=script_info,
                            location=location,
                            location_sal_index=location_sal_index)
        return script_info

    @property
    def running(self):
        """Get or set running state.

        If set False the queue pauses.
        """
        return self._running

    @running.setter
    def running(self, run):
        change = bool(run) != self._running
        self._running = bool(run)
        if change:
            self._update_queue()

    def terminate_all(self):
        """Terminate all subprocesses and return the number terminated.

        Does not wait for termination to actually finish.
        """
        nkilled = 0
        for script_info in self.queue:
            did_terminate = script_info.terminate()
            if did_terminate:
                nkilled += 1
        return nkilled

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
        # start the current script, if appropriate
        is_current = self.current_script is not None and self.current_script.index == script_info.index
        if self.running and is_current and script_info.runnable:
            script_info.run()
        if script_info.done:
            self._remove_script(script_info.index)

        if self.script_callback:
            try:
                self.script_callback(script_info)
            except Exception:
                print("queue_callback failed:", file=sys.stderr)
                traceback.print_exc(file=sys.stderr)

    def _update_queue(self, *args):
        """Call whenever the queue changes state.

        If appropriate, move the current script to history
        and start the next script.

        Always call ``queue_callback``, if it exists.
        """
        if self.current_script:
            if not self.current_script.done:
                return
            self.history.appendleft(self.current_script)
            self.current_script = None

        if self.running:
            # it is unlikely any done scripts can be on the queue
            # but just in case, be paranoid
            while self.queue:
                script_info = self.queue.popleft()
                if script_info.done:
                    warnings.warn(f"script {script_info} on queue unexpectedly done")
                    continue
                self.current_script = script_info
                if script_info.runnable:
                    script_info.run()
                break

        if self.queue_callback:
            try:
                self.queue_callback()
            except Exception:
                print("queue_callback failed:", file=sys.stderr)
                traceback.print_exc(file=sys.stderr)
