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

__all__ = ["ScriptInfo", "ScriptProcessState"]

import asyncio
import os
import enum

from lsst.ts import salobj
import SALPY_Script
import SALPY_ScriptQueue
from .base_script import ScriptState

_CONFIGURE_TIMEOUT = 60  # seconds


class ScriptProcessState(enum.IntEnum):
    """processState constants.
    """
    UNKNOWN = 0
    """Script process state is unknown."""
    LOADING = SALPY_ScriptQueue.script_Loading
    """Script is being loaded."""
    CONFIGURED = SALPY_ScriptQueue.script_Configured
    """Script successfully configured."""
    RUNNING = SALPY_ScriptQueue.script_Running
    """Script running."""
    DONE = SALPY_ScriptQueue.script_Done
    """Script completed."""
    LOADFAILED = SALPY_ScriptQueue.script_LoadFailed
    """Script could not be loaded."""
    CONFIGUREFAILED = SALPY_ScriptQueue.script_ConfigureFailed
    """Script failed in the configuration step."""
    TERMINATED = SALPY_ScriptQueue.script_Terminated
    """Script was terminated."""


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
    verbose : `bool` (optional)
        If True then print log messages from the script to stdout.
    """
    def __init__(self, index, cmd_id, is_standard, path, config, descr, verbose=False):
        self._index = int(index)
        self.cmd_id = int(cmd_id)
        self.is_standard = bool(is_standard)
        self.path = str(path)
        self.config = config
        self.descr = descr
        self.verbose = verbose
        self.salinfo = salobj.SalInfo(SALPY_Script, self.index)
        self.script_state = 0
        """Most recent state reported by the Script, or 0 if the script
        is not yet loaded."""
        self.timestamp_process_start = 0
        """Time at which the script process was started. 0 before that."""
        self.timestamp_configure_start = 0
        """Time at which the _configure method was started. 0 before that.

        Note: under unusual circumstances the _configure method may fail
        before the configure command is sent to the task.
        """
        self.timestamp_configure_end = 0
        """Time at which the configure command finished (succeeded or failed).
        0 before that."""
        self.timestamp_run_start = 0
        """Time at which the script started running. 0 before that."""
        self.timestamp_process_end = 0
        """Time at which the script process finished. 0 before that."""
        self.create_process_task = None
        """Task for creating self.process, or None if just beginning to load.
        """
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
        """True if the configure command succeeded."""
        return self._configure_run and self.config_task.exception() is None

    @property
    def configure_failed(self):
        """True if the configure command failed."""
        return self._configure_run and self.config_task.exception() is not None

    @property
    def load_failed(self):
        """True if the script could not be loaded."""
        return self.process_done and self.timestamp_configure_start == 0

    @property
    def running(self):
        """True if the script was commanded to run and is not done."""
        return self.timestamp_run_start > 0 and not self.process_done

    @property
    def process_done(self):
        """True if the script process was started and is done.

        Notes
        -----
        This will be true if the script fails or is terminated *after*
        the process has been created.
        This will be false if the script is terminated *before*
        the process has been created.
        """
        return self.process_task is not None and self.process_task.done()

    @property
    def failed(self):
        """True if the script failed."""
        return self.process_done and self.process.returncode > 0

    @property
    def terminated(self):
        """True if the script was terminated.

        Notes
        -----
        If this is true the termination may be in progress.
        To wait until termination is complete::

            if self.terminated and not self.process_done:
                await self.process_task
        """
        if self._terminated:
            return True
        return self.process_done and self.process.returncode < 0

    @property
    def index(self):
        """The script's SAL index."""
        return self._index

    @property
    def process_state(self):
        """State of the script subprocess.

        One of the ``ScriptProcessState`` enumeration constants.
        """
        if self.load_failed:
            return ScriptProcessState.LOADFAILED
        elif self.configure_failed:
            return ScriptProcessState.CONFIGUREFAILED
        elif self.terminated:
            return ScriptProcessState.TERMINATED
        elif self.process_done:
            return ScriptProcessState.DONE
        elif self.running:
            return ScriptProcessState.RUNNING
        elif self.configured:
            return ScriptProcessState.CONFIGURED
        return ScriptProcessState.LOADING

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
        asyncio.ensure_future(self.remote.cmd_run.start())
        self.timestamp_run_start = self.salinfo.manager.getCurrentTime()

    @property
    def runnable(self):
        """Can the script be run?

        For a script to be runnable it must be configured,
        the process must not have finished,
        and ``run`` must not have been called.
        """
        return self.configured and not (self.process_done or self.timestamp_run_start > 0)

    async def start_loading(self, fullpath):
        """Start the script process and start a task that will configure
        the script when it is ready.

        fullpath : `str`, `bytes` or `os.PathLike`
            Full path to the script.
        """
        if self.create_process_task is not None:
            raise RuntimeError("Already started loading")
        if self._terminated:
            # this can happen if the user stops a script with stopScript
            # while the script is being added
            return
        self._run_callback()  # report loading
        initialpath = os.environ["PATH"]
        scriptdir, scriptname = os.path.split(fullpath)
        try:
            os.environ["PATH"] = scriptdir + ":" + initialpath
            self.create_process_task = asyncio.ensure_future(
                asyncio.create_subprocess_exec(scriptname, str(self.index)))
            self.process = await self.create_process_task
            self.timestamp_process_start = self.salinfo.manager.getCurrentTime()
            self.process_task = asyncio.ensure_future(self.process.wait())
            self.process_task.add_done_callback(self._cleanup)
            await self._add_remote()
        except Exception:
            self._terminated = True
            raise
        finally:
            os.environ["PATH"] = initialpath

    @property
    def process_duration(self):
        """How long the script process was alive (sec).

        0 if the process has not yet started or is still running.
        """
        if self.timestamp_process_start > 0 and self.timestamp_process_end > 0:
            return self.timestamp_process_end - self.timestamp_process_start
        return 0

    @property
    def run_duration(self):
        """How long it took to run the script (sec).

        0 if the script has not been run, or is still running.
        """
        if self.timestamp_run_start > 0 and self.timestamp_process_end > 0:
            return self.timestamp_process_end - self.timestamp_run_start
        return 0

    def terminate(self):
        """Terminate the script.

        Does not wait for termination to finish; to do that use::

            if script_info.process is not None:
                await script_info.process.wait()

        Returns
        -------
        did_terminate : `bool`
            Returns True if the script was terminated (possibly by an earlier
            call to terminate), False if the script process finished
            before being terminated.

        Notes
        -----
        If the process has finished then do nothing.
        Otherwise set self._terminate to True and:

        * If the process is running then terminate it by sending SIGTERM.
        * If the process is being started then cancel that.
        """
        run_callback = False
        if not self.process_done:
            self._terminated = True
            if self.create_process_task is not None and not self.create_process_task.done():
                # cancel creating the process
                self.create_process_task.cancel()
                run_callback = True
            if self.process is not None:
                self.process.terminate()
        if run_callback:
            self._run_callback()
        return self._terminated

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
        if self.remote is None:
            # this may occur if the script cannot be executed
            return
        self.remote.evt_state.callback = self._script_state_callback
        if self.verbose:
            self.remote.evt_logMessage.callback = self._log_message_callback

    def _cleanup(self, returncode=None):
        """Clean up when the Script subprocess exits.

        Set the timestamp_process_end, cancel the config task if necessary,
        and delete the remote and the salinfo as they are no
        longer useful and consume resources.
        """
        self.timestamp_process_end = self.salinfo.manager.getCurrentTime()
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

            # without this sleep the configuration command is sometimes lost
            await asyncio.sleep(0.1)
            self.remote.cmd_configure.set(config=self.config)
            await self.remote.cmd_configure.start(timeout=_CONFIGURE_TIMEOUT)
        except Exception:
            # terminate the script but first let the configure_task fail
            asyncio.ensure_future(self._start_terminate())
            raise
        finally:
            self.timestamp_configure_end = self.salinfo.manager.getCurrentTime()

    async def _start_terminate(self):
        self.terminate()

    @property
    def _configure_run(self):
        """Return True if the _configure method was run."""
        return self.config_task is not None and self.config_task.done()

    def _log_message_callback(self, data):
        """Print logMessage data to stdout.

        To use: if self.verbose is true then set this as a callback
        for the logMessage event.

        Parameters
        ----------
        data : `Script_logevent_logMessageC`
            Log message data.
        """
        print(f"Script {self.index} log message={data.message!r}; "
              f"level={data.level}; traceback={data.traceback!r}")

    def _run_callback(self, *args):
        if self.callback:
            self.callback(self)

    def _script_state_callback(self, state):
        self.script_state = state.state
        if self.script_state == ScriptState.UNCONFIGURED and self.config_task is None:
            self.timestamp_configure_start = self.salinfo.manager.getCurrentTime()
            self.config_task = asyncio.ensure_future(self._configure())
            self.config_task.add_done_callback(self._run_callback)
            self.start_task.set_result(None)
        self._run_callback()
