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

__all__ = ["ScriptInfo"]

import asyncio
import os

import salobj
import SALPY_Script
import SALPY_ScriptQueue
from .base_script import ScriptState

_CONFIGURE_TIMEOUT = 15  # seconds
_STATE_TIMEOUT = 15  # seconds; includes time to make Script SAL component


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
        self.script_state = 0
        """Most recent state reported by the Script, or 0 if the script is not yet loaded."""
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
        None initially and after the process exits."""
        self.process = None
        """Process in which the ``Script`` SAL component is loaded."""
        self.process_task = None
        """Task awaiting ``process.wait()``, or None if not yet started."""
        self.config_task = None
        """Task awaiting configuration to complete,
        or None if not yet started."""
        self._callback = None
        self._run_started = False  # `run` successfully called

        # The following guarantees that if we terminate a process
        # and it sucessfully stops, then we can report it as terminated;
        # I had hoped to use process.returncode < 0 but was getting
        # a returncode of 0 instead.
        self._terminated = False

    @property
    def callback(self):
        """Set, clear or get a function to call whenever the script
        state changes.

        It receives one argument: this ScriptInfo.
        Set to None to clear the callback.
        """
        return self._callback

    @callback.setter
    def callback(self, callback):
        if not callable(callback):
            raise TypeError(f"callback={callback} is not callable")
        self._callback = callback

    @property
    def configured(self):
        """True if the the configuration command succeeded or failed."""
        return self.config_task is not None and self.config_task.done()

    @property
    def running(self):
        """True if the script was commanded to run and is not done."""
        return self._run_started and not self.done

    @property
    def done(self):
        """True if the script subprocess is done."""
        return self.process_task is not None and self.process_task.done()

    @property
    def failed(self):
        """True if the script failed."""
        return self.done and self.process.returncode > 0

    @property
    def terminated(self):
        """True if the script subprocess was terminated."""
        return self.done and (self.process.returncode < 0 or self._terminated)

    @property
    def index(self):
        """The script's SAL index."""
        return self._index

    @property
    def process_state(self):
        """State of the script subprocess.

        One of the ``SALPY_ScriptQueue.script_`` enumeration constants.
        """
        if self.configured and self.config_task.exception() is not None:
            return SALPY_ScriptQueue.script_ConfigureFailed
        elif self.terminated:
            return SALPY_ScriptQueue.script_Terminated
        elif self.done:
            return SALPY_ScriptQueue.script_Done
        elif self.running:
            return SALPY_ScriptQueue.script_Running
        elif self.configured:
            return SALPY_ScriptQueue.script_Configured
        return SALPY_ScriptQueue.script_Loading

    def run(self):
        """Start the script running.

        Raises
        ------
        RuntimeError
            If the script cannot be run, e.g. because:

            - The script has not yet been configured.
            - `run` was already called.
            - The script process is done.
        """
        if not self.runnable:
            raise RuntimeError("Script is not runnable")
        self._run_started = True
        asyncio.ensure_future(self.remote.cmd_run.start(self.remote.cmd_run.DataType()))

    @property
    def runnable(self):
        """Can the script be run?

        For a script to be runnable it must be configured,
        the process must not have finished,
        and ``run`` must not have been called.
        """
        return self.configured and not (self.done or self._run_started)

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
            self.process_task.add_done_callback(self._cleanup)
            await self._add_remote()
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
        """
        if self.process is None:
            return False
        if self.process.returncode is not None:
            return False
        self._terminated = True
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

    async def _add_remote(self):
        """Create the remote.

        This takes awhile (TSS-3191) so run it as part of it in a thread.
        """
        def thread_func():
            self.remote = salobj.Remote(SALPY_Script, self.index)

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, thread_func)
        self.remote.evt_state.callback = self._script_state_callback

    def _cleanup(self, returncode=None):
        """Clean up when the Script subprocess exits.

        Set the duration, cancel the config task if necessary,
        and delete the remote and the salinfo as they are no
        longer useful and consume resources.
        """
        self.duration = self.salinfo.manager.getCurrentTime() - self.timestamp
        if self.config_task and not self.config_task.done():
            self.config_task.cancel()
        self.remote = None
        self.salinfo = None
        self._run_callback()

    async def _configure(self):
        """Configure the script.

        If configuration fails then terminate the script.

        Raises
        ------
        RuntimeError
            If the script state is not ScriptState.UNCONFIGURED
        """
        try:
            if self.script_state != ScriptState.UNCONFIGURED:
                raise RuntimeError(f"Cannot configure script because it is in state {self.script_state} "
                                   f"instead of {ScriptState.UNCONFIGURED}")

            config_data = self.remote.cmd_configure.DataType()
            config_data.config = self.config
            await self.remote.cmd_configure.start(config_data, timeout=_CONFIGURE_TIMEOUT)
        except Exception:
            self.terminate()
            raise

    def _run_callback(self, *args):
        if self.callback:
            self.callback(self)

    def _script_state_callback(self, state):
        self.script_state = state.state
        if self.script_state == ScriptState.UNCONFIGURED and self.config_task is None:
            self.config_task = asyncio.ensure_future(self._configure())
            self.config_task.add_done_callback(self._run_callback)
            self.start_task.set_result(None)
        self._run_callback()
