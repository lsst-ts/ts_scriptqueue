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

__all__ = ["RequestCmd"]

import logging
import warnings
import argparse
import yaml
from lsst.ts.salobj import AckError

try:
    from cmd2 import Cmd
except ModuleNotFoundError:
    warnings.warn(
        "Could not find module cmd2. Fallback to standard "
        "cmd library. This may limit the functionality of the shell."
    )
    from cmd import Cmd

from .request_model import RequestModel

queue_names = ["Main", "Auxiliary"]

LOG_LEVEL_DICT = dict(
    debug=logging.DEBUG,
    info=logging.INFO,
    warning=logging.WARNING,
    error=logging.ERROR,
    critical=logging.CRITICAL,
)


class CmdError(RuntimeError):
    pass


class RequestCmd(Cmd):
    """This class provides a command line interface to the RequestModel class.
    """

    def __init__(self, index):

        self.intro = """Welcome to the Request Script shell. This script is designed to
        operate with the LSST script queue.

        Type help or ? to list commands."""
        self.prompt = "(cmd:None): "

        super().__init__()

        self.log = logging.getLogger("request")
        self.model = RequestModel(index)

        self.username = None

        self.max_items_past = 3

        # Defining an argparse to handle arguments for the do_run method
        self.__run_parser = argparse.ArgumentParser(prog="run")
        self.__run_parser.add_argument(
            "type",
            help="Choose the type of script to run (standard or external).",
            choices=("standard", "std", "s", "external", "ext", "e"),
        )
        self.__run_parser.add_argument("script", help="Name of script to run.")

        config_group = self.__run_parser.add_mutually_exclusive_group(required=False)
        config_group.add_argument(
            "-c", "--config", help="Configuration file for the script."
        )
        config_group.add_argument(
            "-p",
            "--parameters",
            nargs="+",
            help="In line configuration string for the script. Must be "
            "in yaml format.",
        )
        self.__run_parser.add_argument(
            "-m",
            "--monitor",
            dest="monitor",
            action="store_true",
            help="Monitor script execution all the way to the end.",
        )
        self.__run_parser.add_argument(
            "--timeout",
            dest="timeout",
            default=120.0,
            type=float,
            help="Timeout for monitoring scripts.",
        )

    def do_set_max_past(self, arg):
        """Set the maximum number of scripts in the past queue to show."""
        new_max = int(arg)
        if new_max <= 0:
            self.log.error("Maximum number must be greater than zero.")
        else:
            self.max_items_past = int(arg)

    def do_heartbeat(self, arg):
        """Listen for heartbeats from the queue."""

        nbeats = 10
        if len(arg) > 0:
            try:
                nbeats = int(arg)
            except ValueError:
                self.log.warning(
                    f"Could not parse {arg!r} as an integer; "
                    f"using {nbeats} heartbeats (the default) instead."
                )
        for i in range(nbeats):
            self.model.listen_heartbeat()
            self.log.info(f"Heartbeat {i+1}/{nbeats}...")

    def do_enable(self, arg):
        """Enable the queue."""
        self.model.enable_queue()

    def do_list(self, arg):
        """List available scripts."""

        print("Listing all available scripts.")

        scripts = self.model.get_scripts()

        print("List of external scripts:")
        for script in scripts["external"]:
            print(f"\t - {script}")

        print("List of standard scripts:")
        for script in scripts["standard"]:
            print(f"\t - {script}")

    def do_pause(self, arg):
        """Pause the queue."""
        print("Pausing queue.")
        self.model.pause_queue()

    def do_resume(self, arg):
        """Resume queue."""
        print("Resuming queue.")
        self.model.resume_queue()

    def do_exit(self, arg):
        """Exit shell."""
        print("Exiting...")
        return True

    def do_quitQueue(self, arg):
        """Shut down the script queue and exit.

        Interrupt the queue by sending it the exitControl command and exit.

        If it fails to quit the queue exit anyway.
        """
        try:
            self.model.quit_queue()
            print("Bye...")
        except Exception as e:
            print(f"Could not quit queue: {e}")
        return True

    def do_stop(self, args):
        """Stop list of scripts.

        Parameters
        ----------
        args : str
            Comma separated list of scripts indices.
        """
        print(f"Stopping script {args}.")
        sal_indices = [int(i) for i in args.split(",")]
        self.model.stop_scripts(sal_indices, terminate=False)

        return 0

    def do_terminate(self, args):
        """Terminate scripts.

        Similar to stop but terminates a running script without giving
        it time to clean up.

        Parameters
        ----------
        args : str
            Comma separated list of scripts indices.
        """
        print(f"Terminating script {args}")

        sal_indices = [int(i) for i in args.split(",")]
        self.model.stop_scripts(sal_indices, terminate=True)

        return 0

    def do_show(self, args):
        """Show current state information about the queue."""

        print("#\n# Showing tasks in the queue.\n#")
        queue_state = self.model.get_queue_state()

        print(f"\tQueue summary state: {self.model.summary_state!s}")
        print(f'\tQueue status: {queue_state["state"]}')
        print(f'\tCurrent running: {self.model.parse_info(queue_state["current"])}')
        print(f'\tCurrent queue size: {len(queue_state["queue_scripts"])}')
        print(f'\tPast queue size: {len(queue_state["past_scripts"])}')

        if len(queue_state["queue_scripts"]) > 0:
            print("\nItems on queue:")
            for item in queue_state["queue_scripts"]:
                print(
                    f"{item}: {self.model.parse_info(queue_state['queue_scripts'][item])}"
                )
        else:
            print("\nNo items on queue.")

        if len(queue_state["past_scripts"]) > 0:
            print("\nItems on past queue:")

            for i, item in enumerate(queue_state["past_scripts"]):
                print(self.model.parse_info(queue_state["past_scripts"][item]))
                if i + 1 >= self.max_items_past:
                    break
            if len(queue_state["past_scripts"]) > self.max_items_past:
                print(
                    f" ... (+{len(queue_state['past_scripts'])-self.max_items_past}) "
                    "items on past queue"
                )
        else:
            print("\nNo items on past queue.")

        return 0

    def do_monitor_script_state(self, salindex):
        """Monitor a script state until it is done.

        Parameters
        ----------
        salindex : int
            The salindex of the script to monitor.
        """
        self.monitor_script(int(salindex))

    def do_run(self, args):
        """Request a script to run on the queue.
        """
        try:
            parsed = self.__run_parser.parse_args(args.split())
        except SystemExit:
            return

        scripts = self.model.get_scripts()

        is_standard = True if parsed.type in ("standard", "std", "s") else False

        if is_standard and parsed.script not in scripts["standard"]:
            self.log.error(
                "Requested script %s not in the list of standard scripts.",
                parsed.script,
            )
            return

        if not is_standard and parsed.script not in scripts["external"]:
            self.log.error(
                "Requested script %s not in the list of external scripts.",
                parsed.script,
            )
            return

        # Reading in input file
        config = ""
        if parsed.config is not None:
            self.log.debug("Reading configuration from %s", parsed.config)
            with open(parsed.config, "r") as stream:
                yconfig = yaml.load(stream)
                config = yaml.safe_dump(yconfig)
                self.log.debug("Configuration: %s", config)
        elif parsed.parameters is not None:
            self.log.debug("Parsing parameters: %s", parsed.parameters)
            yconfig = {}

            if len(parsed.parameters) % 2 > 0:
                self.log.warning(
                    "Parameters has wrong number of items. "
                    f"Must be key/value pair. Ignoring {parsed.parameters[-1]}"
                )

            for i in range(len(parsed.parameters) // 2):
                yconfig[parsed.parameters[i * 2]] = parsed.parameters[i * 2 + 1]
            config = yaml.safe_dump(yconfig)
            self.log.debug("Configuration: %s", config)
        else:
            self.log.debug("No configuration file or in line parameters.")

        # Preparing to load script
        # Add to the queue
        self.log.debug(f"Adding {parsed.script} to the queue.")
        salindex = self.model.add(parsed.script, is_standard, config)

        if parsed.monitor:
            self.monitor_script(salindex)
        else:
            self.log.info(f"Script index: {salindex}.")

        return

    def help_run(self):
        """Print help for the `run` command using the command parser.
        """
        self.__run_parser.print_help()

    def parse_log_level(self, strlevel):
        level = LOG_LEVEL_DICT.get(strlevel.lower())
        if level is None:
            try:
                level = int(strlevel)
            except ValueError:
                raise CmdError(
                    f"*** Log level {strlevel!r} must be one of "
                    f"{tuple(LOG_LEVEL_DICT.keys())} or an integer"
                )
        return level

    def do_set_log_level(self, args):
        """Set log level of the shell script.

        Parameters
        ----------
        args : str
            A string that is a level name or can be converted to an int.
            Allowed names are debug, info, warning, error, critical.
        """
        level = self.parse_log_level(args)
        logging.basicConfig(level=level)

    def do_set_script_log_level(self, args):
        """Set log level of a script: index, level: 10=DEBUG, 20

        Parameters
        ----------
        args : str
            Two space-separated values:
            * Script SAL index
            * log level as a name or integer; the allowed names are:
              debug, info, warning, error, critical.
        """
        try:
            arglist = args.split()
            assert len(arglist) == 2
            index = int(arglist[0])
        except (AssertionError, ValueError):
            self.log.error(f"*** Could not parse {args!r} as: script_index log_level")
            return
        level = self.parse_log_level(arglist[1])

        self.log.debug(f"Setting script {index} log level to {level}")
        try:
            self.model.set_script_log_level(index, level)
        except KeyError:
            self.log.error(f"*** Unknown script {index}")

    def do_set_queue_log_level(self, args):
        """Set log level of the queue.
        """
        level = self.parse_log_level(args)
        self.log.debug(f"Setting queue log level to {level}")
        self.model.set_queue_log_level(level)

    def do_username(self, args):
        """Register username for using the queue.

        Parameters
        ----------
        args : `str`
            Name to register as current user. Preferably the project slack
            user handle so it is easy to identify and communicate with
            whoever is currently using the queue.

        """
        print(f"Registering as {args} ...")
        self.username = args
        self.prompt = f"(cmd:{self.username}): "

    def do_logout(self, args):
        """Unregister username."""
        self.username = None
        self.prompt = "(cmd:None): "

    def monitor_script(self, salindex):
        """Monitor the execution of a script. Will block until
        script is in a final state.

        Parameters
        ----------
        salindex : int

        """
        queue_state = self.model.get_queue_state()

        if queue_state["state"] != "Running":
            self.log.warning("Queue not running, cannot monitor script.")
            return

        self.log.info(f"Monitoring execution of script {salindex}.")
        self.model.monitor_script(salindex)

    def onecmd(self, *args):
        """Encapsulate all commands with a try/except clause.
        """
        if self.username is None:
            self.log.warning("Please, add slack username before using queue.")

        try:
            return super().onecmd(*args)
        except AckError as ack_err:
            self.log.error(
                f"onecmd(*{args}) failed with ack.result={ack_err.ack.result}"
            )
        except CmdError as e:
            self.log.error(f"*** {e.args[0]}")
        except Exception:
            self.log.exception(f"onecmd(*{args}) failed")
