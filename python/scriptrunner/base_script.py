__all__ = ["Script"]

import abc
import enum

import SALPY_Script
import salobj


class State(enum.Enum):
    """State constants.
    """
    UNCONFIGURED = 1
    CONFIGURED = enum.auto()
    RUNNING = enum.auto()
    PAUSING = enum.auto()
    PAUSED = enum.auto()
    ABORTING = enum.auto()
    STOPPING = enum.auto()
    ABORTED = enum.auto()
    STOPPED = enum.auto()
    DONE = enum.auto()


class Script(salobj.BaseCsc, abc.ABC):
    def __init__(self):
        super().__init__(self, SALPY_Script, "Script")
        self.state = State.STOPPED

    @abc.abstractmethod
    async def run(self, config):
        """Run the script.
        """
        pass

    @abc.abstractmethod
    def pause(self, label=None):
        """Pause the running script at the next convenient point
        or the named point.
        """
        pass

    @abc.abstractmethod
    def resume(self):
        """Resume the paused script.
        """
        pass

    @abc.abstractmethod
    def stop(self, label=None):
        """Stop the paused script at the next convenient point
        or the named point.
        """
        pass

    def assert_state(self, command, required_state):
        if self.state != required_state:
            raise RuntimeError(f"Cannot {command}: state = {self.state} instead of {required_state}")

    def do_run(self, id_data):
        """Run the currently loaded script.

        Parse and validate the specified configuration,
        then call the run method on the script,
        passing it the parsed configuration.
        """
        self.assert_state("run", State.STOPPED)

    def do_pause(self, id_data):
        """Pause the currently running script at the next
        convenient point or a named point.
        """
        self.assert_state("run", State.RUNNING)

    def do_resume(self, id_data):
        """Resume the currently paused script.
        """
        self.assert_state("run", State.PAUSED)

    def do_stop(self, id_data):
        """Stop the currently running or paused script at the next
        convenient point or a named point. A no-op if not running.
        """
        pass

    def do_abort(self, id_data):
        """Abort the script immediately.
        """
        pass
