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
import time

from lsst.ts.idl.enums.Script import ScriptState
from lsst.ts.idl.enums.ScriptQueue import ScriptProcessState

_SET_GROUP_ID_TIMEOUT = 5  # Time limit for setGroupId command (seconds)
_CONFIGURE_TIMEOUT = 60  # Time limit for the configure command (seconds)


class ScriptInfo:
    """Information about a loaded script.

    Parameters
    ----------
    log : `logging.Logger`
        Logger.
    remote : `salinfo.Remote`
        Remote for the "Script" component with index=0.
        This will be used to send commands but not receive events,
        since the queue model does that.
    index : `int`
        Index of script. This must be unique among all Script SAL
        components that are currently running.
    seq_num : `int`
        Command sequence number; recorded in the script info.
    is_standard : `bool`
        Is this a standard (True) or external (False) script?
    path : `str`, `bytes` or `os.PathLike`
        Path to script, relative to standard or external root dir.
    config : `str`
        Configuration data as a YAML encoded string.
    descr : `str`
        A short explanation of why this script is being run.
    log_level : `int` (optional)
        Log level for the script, as a Python logging level.
        0, the default, leaves the level unchanged.
    pause_checkpoint : `str` (optional)
        Checkpoint(s) at which to pause, as a regular expression.
        No checkpoints if blank; all checkpoints if ".*".
    stop_checkpoint : `str` (optional)
        Checkpoint(s) at which to stop, as a regular expression.
        No checkpoints if blank; all checkpoints if ".*".
    verbose : `bool` (optional)
        If True then print log messages from the script to stdout.
    """

    def __init__(
        self,
        log,
        remote,
        index,
        seq_num,
        is_standard,
        path,
        config,
        descr,
        log_level=0,
        pause_checkpoint="",
        stop_checkpoint="",
        verbose=False,
    ):
        self.log = log.getChild(f"ScriptInfo(index={index})")
        self.remote = remote
        self.index = int(index)
        self.seq_num = int(seq_num)
        self.is_standard = bool(is_standard)
        self.path = str(path)
        self.config = config
        self.log_level = log_level
        self.pause_checkpoint = pause_checkpoint
        self.stop_checkpoint = stop_checkpoint
        self.descr = descr
        # The most recent group ID set by `set_group_id`.
        self.group_id = ""
        self.verbose = verbose
        # Most recent value of script metadata; None until set.
        self.metadata = None
        # The most recent state reported by the Script,
        # or 0 if the script is not yet loaded.
        self.script_state = 0
        # Delay between when the script sent state and it was received (sec)
        self.state_delay = 0
        # Time at which the script process was started. 0 before that.
        self.timestamp_process_start = 0
        # Time at which the _configure method was started. 0 before that.
        # Note: under unusual circumstances the _configure method may fail
        # before the configure command is sent to the task.
        self.timestamp_configure_start = 0
        # Time at which the configure command finished (succeeded or failed).
        # 0 before that.
        self.timestamp_configure_end = 0
        # Time at which the script started running. 0 before that.
        self.timestamp_run_start = 0
        # Time at which the script process finished. 0 before that.
        self.timestamp_process_end = 0
        # Task for creating self.process, or None if just beginning to load.
        self.create_process_task = None
        # Task that finishes when configuration starts.
        # By the time start_task is done config_task and process_task
        # will exist (instead of being None). Thus:
        # * To wait for configuration to be done: first await start_task
        #   then await config_task.
        # * To wait for a script to finish: first await start_task
        #   then await process_task:
        self.start_task = asyncio.Future()
        # Process in which the ``Script`` SAL component is loaded.
        self.process = None
        # Task awaiting ``process.wait()``, or None if
        # the process has not yet started.
        self.process_task = None
        # Task awaiting configuration to complete, or None if
        # configuration has not yet started.
        self.config_task = None
        # Task awaiting clearing group ID. None if group ID not cleared.
        # Reset to None when group ID is set.
        self.clear_group_id_task = None
        # Task awaiting setting group ID, None if group ID is not set.
        # Reset to None when group ID is cleared.
        self.set_group_id_task = None
        self._callback = None

        # The following guarantees that if we terminate a process
        # and it sucessfully stops, then we can report it as terminated;
        # (If terminating a process resulted in process.returncode < 0
        # we would not need this attribute, but the returncode is 0).
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
    def started(self):
        """True if the script was commanded to run or terminate.
        """
        self.timestamp_run_start > 0 or self.terminated or self.process_done

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
        """True if the script failed.

        This will be false if the script was terminated."""
        if not self.process_done:
            return False
        return self.process.returncode is not None and self.process.returncode > 0

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
        if not self.process_done:
            return False
        return self.process.returncode is None or self.process.returncode < 0

    @property
    def process_state(self):
        """State of the script subprocess.

        One of the `ScriptProcessState` enumeration constants.
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
        asyncio.create_task(self.remote.cmd_run.set_start(ScriptID=self.index))
        self.timestamp_run_start = time.time()

    @property
    def runnable(self):
        """Can the script be run?

        For a script to be runnable it must be configured, not started,
        and it must have a group ID.
        """
        return self.configured and not self.started and self.group_id

    @property
    def setting_group_id(self):
        """Return True if the group ID is being set.
        """
        return self.set_group_id_task and not self.set_group_id_task.done()

    @property
    def needs_group_id(self):
        """Is this script ready to be assigned a group ID?

        True if the script is configured and not started,
        and the group ID is neither set nor being set.
        """
        return (
            self.configured
            and not self.started
            and not self.group_id
            and not self.setting_group_id
        )

    def clear_group_id(self, command_script):
        """Clear the group ID.

        Can be called in any state.

        Parameters
        ----------
        command_script : `bool`
            If True and if the script is configured and not started,
            then send setGroupId command to the script.
        """
        self.group_id = ""
        self._cancel_set_clear_group_id()
        if command_script and self.configured and not self.started:
            self.clear_group_id_task = asyncio.create_task(
                self.remote.cmd_setGroupId.set_start(
                    ScriptID=self.index, groupId="", timeout=_SET_GROUP_ID_TIMEOUT
                )
            )

    async def set_group_id(self, group_id):
        """Set the group ID.

        Also creates ``self.set_group_id_task`` and sets it done on success
        or to an exception if the setGroupId Script command fails.

        Parameters
        ----------
        group_id : `str`
            New group ID; "" to clear the group ID.

        Raises
        ------
        RuntimeError
            If the script is not in state CONFIGURED.

        asyncio.TimeoutError
            If the command or reply takes too long.
        """
        if not group_id:
            raise ValueError(f"group_id={group_id} must not be blank")

        if not self.needs_group_id:
            raise RuntimeError(
                f"script {self.index} is not in a state to have group ID set."
            )

        self._cancel_set_clear_group_id()
        self.set_group_id_task = asyncio.create_task(
            self.remote.cmd_setGroupId.set_start(
                ScriptID=self.index, groupId=group_id, timeout=_SET_GROUP_ID_TIMEOUT
            )
        )
        await self.set_group_id_task
        self.group_id = group_id
        self._run_callback()

    async def start_loading(self, fullpath):
        """Start the script process and start a task that will configure
        the script when it is ready.

        Parameters
        ----------
        fullpath : `str`, `bytes` or `os.PathLike`
            Full path to the script.

        Notes
        -----
        If loading is canceled the script process is terminated.
        If loading fails the script is marked as terminated.
        """
        if self.create_process_task is not None:
            raise RuntimeError("Already started loading")
        if self._terminated:
            # this can happen if the user stops a script with stopScript
            # while the script is being added
            return
        initialpath = os.environ["PATH"]
        try:
            scriptdir, scriptname = os.path.split(fullpath)
            os.environ["PATH"] = scriptdir + ":" + initialpath
            # save task so process creation can be cancelled if it hangs
            self.create_process_task = asyncio.create_task(
                asyncio.create_subprocess_exec(scriptname, str(self.index))
            )
            self.process = await self.create_process_task
            self.process_task = asyncio.create_task(self.process.wait())
            self.timestamp_process_start = time.time()
            self._run_callback()
            # note: process_task may already be done if the script cannot
            # be run, in which case the callback will be called immediately
            self.process_task.add_done_callback(self._cleanup)
        except asyncio.CancelledError:
            self.log.info("Loading cancelled")
            self._terminated = True
            raise
        except Exception:
            self.log.exception("Loading failed")
            self._terminated = True
            raise
        finally:
            os.environ["PATH"] = initialpath
            if not self.timestamp_process_start == 0:
                self._run_callback()

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
        self.log.debug("Terminate")
        if not self.process_done:
            self._terminated = True
            if (
                self.create_process_task is not None
                and not self.create_process_task.done()
            ):
                # cancel creating the process
                self.create_process_task.cancel()
            if self.process is not None:
                self.process.terminate()
            self._run_callback()
        return self._terminated

    def __eq__(self, other):
        return self.index == other.index

    def __ne__(self, other):
        return not (self == other)

    def __repr__(self):
        return (
            f"ScriptInfo(index={self.index}, seq_num={self.seq_num}, "
            f"is_standard={self.is_standard}, path={self.path}, "
            f"config={self.config}, descr={self.descr})"
        )

    def _cancel_set_clear_group_id(self):
        """Cancel set and/or clear group ID tasks, if running.

        Set the tasks to None.
        """
        if self.clear_group_id_task and not self.clear_group_id_task.done():
            self.clear_group_id_task.cancel()
        self.clear_group_id_task = None

        if self.set_group_id_task and not self.set_group_id_task.done():
            self.set_group_id_task.cancel()
        self.set_group_id_task = None

    def _cleanup(self, returncode=None):
        """Clean up when the Script subprocess exits.

        Set the timestamp_process_end, cancel the config task,
        delete the remote, run the callback and delete the callback.
        """
        self.timestamp_process_end = time.time()
        if self.config_task:
            self.config_task.cancel()
        self._cancel_set_clear_group_id()
        self.remote = None
        self._run_callback()
        self._callback = None

    async def _configure(self):
        """Configure the script.

        If configuration fails or is cancelled then terminate the script.

        Raises
        ------
        RuntimeError
            If the script state is not ScriptState.UNCONFIGURED
        """
        try:
            if self.script_state != ScriptState.UNCONFIGURED:
                raise RuntimeError(
                    f"Cannot configure script {self.index} "
                    f"because it is in state {self.script_state} "
                    f"instead of {ScriptState.UNCONFIGURED!r}"
                )

            await self.remote.cmd_configure.set_start(
                ScriptID=self.index,
                config=self.config,
                logLevel=self.log_level,
                pauseCheckpoint=self.pause_checkpoint,
                stopCheckpoint=self.stop_checkpoint,
                timeout=_CONFIGURE_TIMEOUT,
            )
        except asyncio.CancelledError:
            self.log.info("Configuration cancelled")
            asyncio.create_task(self._start_terminate())
            raise
        except Exception:
            # terminate the script but first let the configure_task fail
            self.log.exception("Configuration failed")
            asyncio.create_task(self._start_terminate())
            raise
        finally:
            self.timestamp_configure_end = time.time()

    async def _start_terminate(self):
        self.terminate()

    @property
    def _configure_run(self):
        """Return True if the _configure method was run."""
        return self.config_task is not None and self.config_task.done()

    def _run_callback(self, *args):
        if self.callback:
            self.callback(self)

    def _script_state_callback(self, state):
        self.script_state = state.state
        self.state_delay = time.time() - state.private_sndStamp
        if self.script_state == ScriptState.UNCONFIGURED and self.config_task is None:
            self.timestamp_configure_start = time.time()
            self.config_task = asyncio.create_task(self._configure())
            self.config_task.add_done_callback(self._run_callback)
            self.start_task.set_result(None)
        self._run_callback()
