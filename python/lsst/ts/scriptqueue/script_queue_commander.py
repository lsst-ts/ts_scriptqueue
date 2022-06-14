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

__all__ = ["ScriptQueueCommander"]

import asyncio
import logging
import pathlib
import string

from lsst.ts import salobj
from lsst.ts.idl.enums.ScriptQueue import Location, SalIndex
from lsst.ts.idl.enums.Script import ScriptState
from lsst.ts.utils import make_done_future

ADD_TIMEOUT = 5  # Timeout for the add command (seconds).
# How long to wait before warning that a script heartbeat is late (seconds).
HEARTBEAT_ALARM_INTERVAL = 3


class ScriptQueueCommander(salobj.CscCommander):
    """ScriptQueue command-line commander.

    Parameters
    ----------
    script_log_level : `int`
        Default log level for scripts.
    """

    def __init__(self, script_log_level, **kwargs):
        super().__init__(name="ScriptQueue", **kwargs)
        self.script_log_level = script_log_level
        self.help_dict[
            "add"
        ] = f"""type path config options  # add a script to the end of the queue:
    • type = s or std for standard, e or ext for external
    • config = @yaml_path or keyword1=value1 keyword2=value2 ...
      where yaml_path is the path to a yaml file; the .yaml suffix is optional
    • options can be any of the following, in any order
      (but all option args must follow all config args):
      -location=int  # first=0, last=1, before=2, after=3
      -locationSalIndex=int
      -logLevel=int  # error=40, warning=30, info=20, debug=10; default is {script_log_level}
      -pauseCheckpoint=str  # a regex
      -stopCheckpoint=str  # a regex
    • examples:
      add s auxtel/slew_telescope_icrs.py ra=10 dec=0 -location=1
      add s auxtel/slew_telescope_icrs.py @target -logLevel=10 -location=0
    """
        # Default options for the add command
        self.default_add_options = dict(
            location=Location.LAST,
            locationSalIndex=0,
            logLevel=self.script_log_level,
            pauseCheckpoint="",
            stopCheckpoint="",
        )

        self.help_dict["showSchema"] = "type path  # type=s, std, e, or ext"
        self.help_dict[
            "stopScripts"
        ] = "sal_index1 [sal_index2 [... sal_indexN]] terminate (0 or 1)"

        self.script_remote = salobj.Remote(
            domain=self.domain,
            name="Script",
            index=0,
            readonly=True,
            include=["heartbeat", "logMessage", "state"],
        )
        self.script_remote.evt_logMessage.callback = self.script_log_message
        self.script_remote.evt_state.callback = self.script_state
        self.script_remote.evt_heartbeat.callback = self.script_heartbeat
        # Dict of "type" argument: isStandard
        self.script_type_dict = dict(
            s=True, std=True, standard=True, e=False, ext=False, external=False
        )
        # SAL index of script whose heartbeat is being monitored;
        # this should be the currently executing script.
        self._script_to_monitor = 0
        self.script_heartbeat_monitor_task = make_done_future()

    async def start(self):
        await super().start()
        await self.script_remote.start_task

    def get_is_standard(self, script_type):
        """Convert a script type argument to isStandard bool."""
        try:
            return self.script_type_dict[script_type]
        except KeyError:
            raise KeyError(
                f"type {script_type!r} must be one of {list(self.script_type_dict.keys())}"
            )

    def evt_availableScripts_callback(self, data):
        standard_scripts = data.standard.split(":")
        external_scripts = data.external.split(":")
        print("standard scripts:")
        for name in standard_scripts:
            print(f"• {name}")
        print("external scripts:")
        for name in external_scripts:
            print(f"• {name}")

    def evt_queue_callback(self, data):
        if self._script_to_monitor != data.currentSalIndex:
            self._script_to_monitor = data.currentSalIndex
            self.script_heartbeat_monitor_task.cancel()
            if data.currentSalIndex != 0:
                self.script_heartbeat_monitor_task = asyncio.create_task(
                    self.script_heartbeat_monitor()
                )
        salIndices = data.salIndices[0 : data.length]
        pastSalIndices = data.pastSalIndices[0 : data.pastLength]
        print(
            f"{data.private_sndStamp:0.3f} queue "
            f"enabled={data.enabled}, "
            f"running={data.running}, "
            f"currentSalIndex={data.currentSalIndex}, "
            f"salIndices={salIndices}, "
            f"pastSalIndices={pastSalIndices}"
        )

    async def script_heartbeat_monitor(self):
        while True:
            await asyncio.sleep(HEARTBEAT_ALARM_INTERVAL)
            print(
                f"WARNING: Current script {self._script_to_monitor} "
                f"heartbeat not seen in {HEARTBEAT_ALARM_INTERVAL} seconds"
            )

    def script_log_message(self, data):
        exception_str = (
            (
                f", traceback={data.traceback}, "
                f"filePath={data.filePath}, "
                f"functionName={data.functionName}, "
                f"lineNumber={data.lineNumber}, "
            )
            if data.traceback
            else ""
        )
        print(
            f"{data.private_sndStamp:0.3f} Script:{data.salIndex} "
            f"logMessage level={logging.getLevelName(data.level)}, "
            f"message={data.message}{exception_str}"
        )

    def script_state(self, data):
        try:
            state = ScriptState(data.state)
        except ValueError:
            state = data.state
        reason = f", reason={data.reason}" if data.reason else ""
        print(
            f"{data.private_sndStamp:0.3f} Script:{data.salIndex} "
            f"state={state.name}{reason}, lastCheckpoint={data.lastCheckpoint}"
        )

    def script_heartbeat(self, data):
        if data.salIndex != self._script_to_monitor:
            # A heartbeat from the wrong script.
            return
        self.script_heartbeat_monitor_task.cancel()
        self.script_heartbeat_monitor_task = asyncio.create_task(
            self.script_heartbeat_monitor()
        )

    async def do_add(self, args):
        """Overrride the standard add command to simplify the interface."""
        if len(args) < 2:
            raise ValueError("Need at least 2 arguments")
        is_standard = self.get_is_standard(args[0])
        path = args[1]

        # Parse config
        config_path = None
        config_yaml_items = []
        config_start_ind = 2
        options_start_ind = 2
        if len(args) > 2 and args[2].startswith("@"):
            config_path = pathlib.Path(args[2][1:])  # Skip the leading @
            # Add .yaml if not specified
            config_path = config_path.with_suffix(".yaml")
            options_start_ind = 3
        else:
            for arg in args[config_start_ind:]:
                if arg[0] == "-":
                    # Start of options
                    break
                elif args[0] not in string.ascii_letters:
                    raise ValueError(
                        f"Argument {arg!r} should start with a letter, in args={args}"
                    )
                else:
                    options_start_ind += 1
                    name_value = arg.split("=", 1)
                    if len(name_value) != 2:
                        raise ValueError(
                            f"Could not parse config arg {arg!r} as keyword=value, in {args}"
                        )
                    name, value = name_value
                    config_yaml_items.append(f"{name}: {value}")

        options_dict = self.default_add_options.copy()
        for arg in args[options_start_ind:]:
            if arg.startswith("-"):
                name_value = arg[1:].split("=", 1)
                if len(name_value) != 2:
                    raise ValueError(
                        f"Could not parse option arg {arg!r} as keyword=value, in {args}"
                    )
                name, value = name_value
                default_value = options_dict.get(name, None)
                if default_value is None:
                    raise ValueError(f"Unknown option {name} in {args}")
                if hasattr(default_value, "value"):
                    # An enum; first cast value to an integer, then to the enum
                    value = int(value)
                options_dict[name] = type(default_value)(value)
            else:
                raise ValueError(f"Option {arg!r} must start with '-', in {args}")

        if config_path:
            with open(config_path, "r") as f:
                config = f.read()
        else:
            config = "\n".join(config_yaml_items)
        print(f"config={config!r}")

        await self.remote.cmd_add.set_start(
            isStandard=is_standard,
            path=path,
            config=config,
            **options_dict,
            timeout=ADD_TIMEOUT,
        )

    async def do_showSchema(self, args):
        """Overrride the standard showSchema command for named script type."""
        if len(args) != 2:
            raise ValueError("Need 2 arguments: type path")
        is_standard = self.get_is_standard(args[0])
        path = args[1]
        await self.remote.cmd_showSchema.set_start(
            isStandard=is_standard,
            path=path,
        )

    async def do_stopScripts(self, args):
        """Handle the stopScript command, which takes a list of script
        indices.
        """
        if len(args) < 2:
            raise ValueError("Need at least 2 arguments; sal_index terminate")
        terminate_str = args[-1]
        if terminate_str not in ("0", "1"):
            raise ValueError(f"terminate={terminate_str} must be 0 or 1")
        sal_indices = [int(sal_index) for sal_index in args[:-1]]
        terminate = bool(int(terminate_str))

        stop_data = self.remote.cmd_stopScripts.DataType()
        stop_data.length = len(sal_indices)
        stop_data.salIndices[0 : stop_data.length] = sal_indices
        stop_data.terminate = terminate
        await self.remote.cmd_stopScripts.start(data=stop_data)

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument(
            "-l",
            "--loglevel",
            type=int,
            help="Default log level for scripts, as an integer: error=40, warning=30, info=20, debug=10",
            default=logging.INFO,
        )

    @classmethod
    def add_kwargs_from_args(cls, args, kwargs):
        kwargs["script_log_level"] = args.loglevel


def command_script_queue():
    """Run a command-line interface to command a ScriptQueue.

    Intended for engineering use.
    """
    ScriptQueueCommander.amain(index=SalIndex)
