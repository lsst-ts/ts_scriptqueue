__all__ = ["ScriptState", "BaseScript"]

import abc
import argparse
import asyncio
import enum
import logging
import logging.handlers
import queue
import re
import sys

import yaml

import SALPY_Script
import salobj


HEARTBEAT_INTERVAL = 5  # seconds
LOG_MESSAGES_INTERVAL = 0.05  # seconds


class ScriptState(enum.IntEnum):
    """ScriptState constants.
    """
    UNCONFIGURED = SALPY_Script.state_Unconfigured
    """Script is not configured and so cannot be run."""
    CONFIGURED = SALPY_Script.state_Configured
    """Script is configured and so can be run."""
    RUNNING = SALPY_Script.state_Running
    """Script is running."""
    PAUSED = SALPY_Script.state_Paused
    """Script has paused, by request."""
    ENDING = SALPY_Script.state_Ending
    """Script is cleaning up after running successfully
    (though it can still fail if there is an error in cleanup)."""
    STOPPING = SALPY_Script.state_Stopping
    """Script is cleaning up after being asked to stop
    (though it can still fail if there is an error in cleanup)."""
    FAILING = SALPY_Script.state_Failing
    """Script is cleaning up after an error."""
    DONE = SALPY_Script.state_Done
    """Script exiting after successfully running."""
    STOPPED = SALPY_Script.state_Stopped
    """Script exiting after being asked to stop."""
    FAILED = SALPY_Script.state_Failed
    """Script exiting after an error."""


def _make_remote_name(remote):
    """Make a remote name from a remote, for output as script metadata.

    Parameters
    ----------
    remote : `salobj.Remote`
        Remote
    """
    name = remote.salinfo.name
    index = remote.salinfo.index
    if index is not None:
        name = name + ":" + str(index)
    return name


class BaseScript(salobj.Controller, abc.ABC):
    """Abstract base class for SAL scripts run as Script SAL commponents.

    Parameters
    ----------
    index : `int`
        Index of SAL Script component. This must be unique among all
        SAL scripts that are currently running.
    descr : `str`
        Short description of what the script does, for operator display.
    remotes_dict : `dict` of `str` : `salobj.Remote` (optional)
        Dict of attribute name: `salobj.Remote`, or `None` if no remotes.
        These remotes are added as attributes of ``self`` and are also
        used to generate a list of remote names for script metadata.

    Attributes
    ----------
    log : `logging.Logger`
        A Python log. You can safely log to it from different threads.
        Note that it can take up to ``LOG_MESSAGES_INTERVAL`` seconds
        before a log message is sent.
    """
    def __init__(self, index, descr, remotes_dict=None):
        super().__init__(SALPY_Script, index, do_callbacks=True)
        remote_names = []
        if remotes_dict:
            for attrname, remote in remotes_dict.items():
                remote_names.append(_make_remote_name(remote))
                setattr(self, attrname, remote)
        self._description_data = self.evt_description.DataType()
        self._description_data.classname = type(self).__name__
        self._description_data.description = str(descr)
        self._description_data.remotes = ",".join(remote_names)
        self._metadata = self.evt_metadata.DataType()
        self.log = logging.getLogger(self._description_data.classname)
        self._log_queue = queue.Queue()
        self.log.addHandler(logging.handlers.QueueHandler(self._log_queue))
        self._checkpoints = self.evt_checkpoints.DataType()
        self._state = self.evt_state.DataType()
        self._state.state = ScriptState.UNCONFIGURED
        self._run_task = None
        self._pause_future = None
        self._final_state_future = asyncio.Future()
        self._is_exiting = False
        self.evt_state.put(self.state)
        self.evt_description.put(self._description_data)
        self._heartbeat_task = asyncio.ensure_future(self._heartbeat_loop())
        self._log_messages_task = asyncio.ensure_future(self._log_messages_loop())
        self.final_state_delay = 0.2
        """Delay (sec) to allow sending final state before exiting."""

    @classmethod
    def main(cls, descr):
        """Start the script from the command line.

        Parameters
        ----------
        descr : `str`
            Short description of why you are running this script.

        The final return code will be:

        * 0 if final state is `ScriptState.DONE` or `ScriptState.STOPPED`
        * 1 if final state is `ScriptState.FAILED`
        * 2 otherwise (which should never happen)
        """
        parser = argparse.ArgumentParser(f"Run {cls.__name__} from the command line")
        parser.add_argument("index", type=int,
                            help="Script SAL Component index; must be unique among running Scripts")
        args = parser.parse_args()
        script = cls(index=args.index, descr=descr)
        asyncio.get_event_loop().run_until_complete(script.final_state_future)
        final_state = script.final_state_future.result()
        return_code = {ScriptState.DONE: 0,
                       ScriptState.STOPPED: 0,
                       ScriptState.FAILED: 1}.get(final_state.state, 2)
        sys.exit(return_code)

    @property
    def checkpoints(self):
        """Get the checkpoints at which to pause and stop,
        an instance of self.evt_checkpoints.DataType()
        """
        return self._checkpoints

    @property
    def state(self):
        """Get the current state, an instance of self.evt_state.DataType().

        State has these fields:

        * state: the current state; a `ScriptState`
        * last_checkpoint: name of most recently seen checkpoint
        * reason: reason for this state
        """
        return self._state

    @property
    def state_name(self):
        """Return the current state as a name"""
        try:
            return ScriptState(self.state.state).name
        except ValueError:
            return f"UNKNOWN({self.state.state})"

    @property
    def final_state_future(self):
        """Get an asyncio.Future that returns the final state
        of the script.
        """
        return self._final_state_future

    def set_state(self, state=None, reason=None, keep_old_reason=False, last_checkpoint=None):
        """Set the script state.

        Parameters
        ----------
        state : `ScriptState` (optional)
            New state, or None if no change
        reason : `str` (optional)
            Reason for state change. None for no change.
        keep_old_reason : `bool`
            If True, keep old reason, else replace with reason,
            or "" if reason is None.
        last_checkpoint : `str` (optional)
            Name of most recently seen checkpoint. None for no change.
        """
        if state is not None:
            if state not in ScriptState:
                raise ValueError(f"{state} is not in ScriptState")
            self._state.state = state
        if keep_old_reason:
            if reason:
                sepstr = "; " if self._state.reason else ""
                self._state.reason = self._state.reason + sepstr + reason
        else:
            self._state.reason = "" if reason is None else reason
        if last_checkpoint is not None:
            self._state.lastCheckpoint = last_checkpoint
        self.evt_state.put(self._state)

    async def checkpoint(self, name=""):
        """Await this at any "nice" point your script can be paused or stopped.

        Parameters
        ----------
        name : `str` (optional)
            Name of checkpoint; "" if it has no name.

        Raises
        ------
        RuntimeError:
            If state is not RUNNING; perhaps you called `checkpoint` from somewhere other than `run`.
        RuntimeError:
            If _run_task is None or done. This probably means your code incorrectly set the state.
        """
        if not self.state.state == ScriptState.RUNNING:
            raise RuntimeError(f"checkpoint error: state={self.state_name} instead of RUNNING; "
                               "did you call checkpoint from somewhere other than `run`?")
        if self._run_task is None:
            raise RuntimeError(f"checkpoint error: state is RUNNING but no run_task")
        if self._run_task.done():
            raise RuntimeError(f"checkpoint error: state is RUNNING but run_task is done")

        if re.fullmatch(self.checkpoints.stop, name):
            self.set_state(ScriptState.STOPPING, last_checkpoint=name)
            raise asyncio.CancelledError(
                f"stop by request: checkpoint {name} matches {self.checkpoints.stop}")
        elif re.fullmatch(self.checkpoints.pause, name):
            self._pause_future = asyncio.Future()
            self.set_state(ScriptState.PAUSED, last_checkpoint=name)
            await self._pause_future
            self.set_state(ScriptState.RUNNING)
        else:
            self.set_state(last_checkpoint=name)

    @abc.abstractmethod
    async def configure(self):
        """Configure the script.

        Subclasses should use named keyword arguments for clarity
        and so that parameter names are automatically checked.
        In other words, do this::

            async def configure(self, arg_a, arg_b=default_b)  # good

        instead of this::

            async def configure(self, **kwards)  # unsafe and unclear

        Notes
        -----
        This method is only called when the script state is
        `ScriptState.UNCONFIGURED`.

        If this method and `set_metadata` both succeed (neither raises
        an exception) then the state is automatically changed to
        `ScriptState.CONFIGURED`.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def set_metadata(self, metadata):
        """Set metadata fields in the provided struct, given the
        current configuration.

        Notes
        -----
        If this method succeeds (does not raise an exception)
        then the metadata is automatically broadcast as an event
        and the script's state is set to `ScriptState.CONFIGURED`.

        This method will only be called if the script state is
        `ScriptState.UNCONFIGURED`. or `ScriptState.CONFIGURED`.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    async def run(self):
        """Run the script.

        Your subclass must provide an implementation, as follows:

        * At points where you support pausing call `checkpoint`.
        * Raise an exception on error. Raise `salobj.ExpectedError`
          to avoid logging a traceback.

        Notes
        -----
        This method is only called when the script state is
        `ScriptState.CONFIGURED`. The remaining state transitions
        are handled automatically.
        """
        raise NotImplementedError()

    async def cleanup(self):
        """Perform final cleanup, if any.

        This method is always called as the script state is exiting
        (unless the script process is aborted by SIGTERM or SIGKILL).
        """
        pass

    def assert_state(self, action, states):
        """Assert that the current state is in ``states`` and the script
        is not exiting.

        Parameters
        ----------
        action : `string`
            Description of what you want to do.
        states : `list` of `salobj.ScriptState`
            The required state.
        """
        if self._is_exiting:
            raise salobj.ExpectedError(f"Cannot {action}: script is exiting")
        if self.state.state not in states:
            states_str = ", ".join(s.name for s in states)
            raise salobj.ExpectedError(
                f"Cannot {action}: state={self.state_name} instead of {states_str}")

    async def do_configure(self, id_data):
        """Configure the currently loaded script.

        This method does the following:

        * Receive the configuration as a ``yaml`` string.
        * Parse the configuration to a `dict`.
        * Call `configure`, using the dict as keyword arguments.
        * Call `set_metadata(metadata)`.
        * Output the metadata event.
        * Change the script state to `ScriptState.CONFIGURED`.

        Raises
        ------
        salobj.ExpectedError
            If state is not UNCONFIGURED.
        """
        self.assert_state("configure", [ScriptState.UNCONFIGURED])
        try:
            config = yaml.safe_load(id_data.data.config)
        except yaml.scanner.ScannerError as e:
            raise salobj.ExpectedError(f"Could not parse config={id_data.data.config}: {e}") from e
        if config and not isinstance(config, dict):
            raise salobj.ExpectedError(f"Could not parse config={id_data.data.config} as a dict")
        if not config:
            config = {}
        try:
            await self.configure(**config)
        except Exception as e:
            raise salobj.ExpectedError(f"config({config}) failed: {e}") from e

        metadata = self.evt_metadata.DataType()
        # initialize to vaguely reasonable values
        metadata.coordinateSystem = SALPY_Script.metadata_CSys_None
        metadata.rotationSystem = SALPY_Script.metadata_Rot_None
        metadata.filters = ""  # any
        metadata.dome = SALPY_Script.metadata_Dome_Either
        metadata.duration = 0
        self.set_metadata(metadata)
        self.evt_metadata.put(metadata)
        self.set_state(ScriptState.CONFIGURED)

    async def do_run(self, id_data):
        """Run the configured script and quit.

        Raises
        ------
        salobj.ExpectedError
            If state is not CONFIGURED.
        """
        self.assert_state("run", [ScriptState.CONFIGURED])
        try:
            self.set_state(ScriptState.RUNNING)
            self._run_task = asyncio.ensure_future(self.run())
            await self._run_task
            self.set_state(ScriptState.ENDING)
        except asyncio.CancelledError:
            if self.state.state != ScriptState.STOPPING:
                self.set_state(ScriptState.STOPPING)
        except Exception as e:
            if not isinstance(e, salobj.ExpectedError):
                self.log.exception("Error in run")
            self.set_state(ScriptState.FAILING, reason=f"Error in run: {e}")
        await self._exit()

    def do_resume(self, id_data):
        """Resume the currently paused script.

        Raises
        ------
        salobj.ExpectedError
            If state is not PAUSED.
        """
        self.assert_state("resume", [ScriptState.PAUSED])
        self._pause_future.set_result(None)

    def do_setCheckpoints(self, id_data):
        """Set or clear the checkpoints at which to pause and stop.

        Parameters
        ----------
        id_data : `salobj.CommandIdData`
            Names of checkpoints for pausing and stopping, each a single
            regular expression; "" for no checkpoints, ".*" for all.

        Raises
        ------
        salobj.ExpectedError
            If state is not UNCONFIGURED, CONFIGURED, RUNNING or PAUSED.
        """
        self.assert_state("setCheckpoints", [ScriptState.UNCONFIGURED, ScriptState.CONFIGURED,
                          ScriptState.RUNNING, ScriptState.PAUSED])
        try:
            re.compile(id_data.data.stop)
        except Exception as e:
            raise salobj.ExpectedError(f"stop={id_data.data.stop!r} not a valid regex: {e}")
        try:
            re.compile(id_data.data.pause)
        except Exception as e:
            raise salobj.ExpectedError(f"pause={id_data.data.pause!r} not a valid regex: {e}")
        self.checkpoints.pause = id_data.data.pause
        self.checkpoints.stop = id_data.data.stop
        self.evt_checkpoints.put(self.checkpoints)

    def do_setLogging(self, id_data):
        """Set logging level.

        Parameters
        ----------
        id_data : `salobj.CommandIdData`
            Logging level.
        """
        self.log.setLevel(id_data.data.level)

    async def do_stop(self, id_data):
        """Stop the script.

        Parameters
        ----------
        id_data : `salobj.CommandIdData`
            Ignored.

        Notes
        -----
        This is a no-op if the script is already exiting.
        This does not wait for _exit to run.
        """
        if self._is_exiting:
            return
        if self._run_task is not None and not self._run_task.done():
            self._run_task.cancel()
        else:
            self.set_state(state=ScriptState.STOPPING)
            await self._exit()

    async def _heartbeat_loop(self):
        """Output heartbeat at regular intervals.
        """
        while True:
            try:
                await asyncio.sleep(HEARTBEAT_INTERVAL)
                self.evt_heartbeat.put(self.evt_heartbeat.DataType())
            except asyncio.CancelledError:
                break
            except Exception:
                self.log.exception(f"Heartbeat output failed")

    async def _log_messages_loop(self):
        """Output log messages.
        """
        while not self._is_exiting:
            try:
                if not self._log_queue.empty():
                    msg = self._log_queue.get_nowait()
                    data = self.evt_logMessage.DataType()
                    data.level = msg.levelno
                    data.message = msg.message
                    self.evt_logMessage.put(data)
                await asyncio.sleep(LOG_MESSAGES_INTERVAL)
            except asyncio.CancelledError:
                break
            except Exception:
                pass  # no point trying to log this since logging failed

    async def _exit(self):
        """Call cleanup (if the script was run) and exit the script.
        """
        if self._is_exiting:
            return
        self._is_exiting = True
        try:
            if self._run_task is not None:
                await self.cleanup()
            self._heartbeat_task.cancel()

            # wait for final log messages, if any
            await self._log_messages_loop()

            reason = None
            final_state = {
                ScriptState.ENDING: ScriptState.DONE,
                ScriptState.STOPPING: ScriptState.STOPPED,
                ScriptState.FAILING: ScriptState.FAILED,
            }.get(self.state.state)
            if final_state is None:
                reason = f"unexpected state for _exit {self.state_name}"
                final_state = ScriptState.FAILED

            self.set_state(final_state, reason=reason, keep_old_reason=True)
        except Exception as e:
            if not isinstance(e, salobj.ExpectedError):
                self.log.exception("Error in run")
            self.set_state(ScriptState.FAILED, reason=f"failed in _exit: {e}", keep_old_reason=True)
        finally:
            # allow time for the final state to be output
            await asyncio.sleep(self.final_state_delay)
            asyncio.ensure_future(self._set_final_state())

    async def _set_final_state(self):
        """Set the result of _final_state_future, if possible.

        Give up the event loop first, so whatever command triggered this
        can be reported as finished.
        """
        if not self._final_state_future.done():
            self._final_state_future.set_result(self.state)
