__all__ = ["RequestCmd"]

import logging
import warnings
import argparse
import yaml

from lsst.ts import salobj
from lsst.ts.scriptqueue.utils import DETAIL_LEVEL
try:
    from cmd2 import Cmd
except ModuleNotFoundError:
    warnings.warn("Could not find module cmd2. Fallback to standard "
                  "cmd library. This may limit the functionality of the shell.")
    from cmd import Cmd

from .request_model import RequestModel

queue_names = ["Main", "Auxiliary"]


class RequestCmd(Cmd):
    """This class provides a command line interface to the RequestModel class.
    """

    def __init__(self, index):

        self.intro = """Welcome to the Request Script shell. This script is designed to
        operate with the LSST script queue.

        Type help or ? to list commands."""
        self.prompt = '(cmd): '

        Cmd.__init__(self)

        self.log = logging.getLogger("request")
        self.model = RequestModel(index)

        self.__run_parser = argparse.ArgumentParser(prog="run")
        self.__run_parser.add_argument('-e', '--external', help="Run an external script.")
        self.__run_parser.add_argument('-s', '--standard', help="Run a standard script.")
        self.__run_parser.add_argument('-c', '--config', help="Configuration file for the script.")
        self.__run_parser.add_argument('-p', '--parameters', nargs='+',
                                       help="In line configuration string for the script. Must be "
                                            "in yaml format.")
        self.__run_parser.add_argument("--monitor", dest="monitor", action="store_true",
                                       help="Monitor script execution all the way to the end.")
        self.__run_parser.add_argument("--timeout", dest="timeout", default=120., type=float,
                                       help="Timeout for monitoring scripts.")

        self.__set_log_parser = argparse.ArgumentParser(prog="log_parser")
        self.__set_log_parser.add_argument("-v", "--verbose", dest="verbose",
                                           action='count', default=0,
                                           help="Set the verbosity for the console logging.")

        self.__set_script_log_parser = argparse.ArgumentParser(prog="script_log")
        self.__set_script_log_parser.add_argument("-v", "--verbose", dest="verbose",
                                                  action='count',
                                                  default=0,
                                                  help="Set the verbosity for the console logging.")
        self.__set_script_log_parser.add_argument("-i", "--index",
                                                  type=int,
                                                  help="SalIndex of script to change log level.")

    def onecmd(self, *args):
        try:
            return super().onecmd(*args)
        except salobj.AckError as ack_err:
            print(f"Failed with ack.result={ack_err.ack.result}")
        except SystemExit:
            pass
        except Exception as e:
            self.log.exception(e)

    def do_heartbeat(self, arg):
        """Listen for queue heartbeat."""
        nbeats = 10
        try:
            nbeats = int(arg)
        except ValueError:
            self.log.debug(f"Could not parse {arg!r} as an integer; using default heartbeat number {nbeats}")
        for i in range(nbeats):
            self.model.listen_heartbeat()
            self.log.info(f"Heartbeat {i+1}/{nbeats}...")

    def do_enable(self, arg):
        """Enable the queue."""
        self.model.enable_queue()

    def do_list(self, arg):
        """List available scripts."""
        print("Listing all available scripts.")

        script_list = self.model.list()

        print('List of external scripts:')
        for script in script_list["external"]:
            print(f'\t - {script}', )

        print('List of standard scripts:')
        for script in script_list["standard"]:
            print(f'\t - {script}', )

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
        """Interrupt the queue by sending it the exitControl command and exit request script.
        If it fails to quit the queue
        """
        try:
            retval = self.model.quit_queue()
            if retval:
                print("Bye...")
            else:
                print("Could not quit queue.")
        except Exception as e:
            print("Could not quit queue.")
            self.log.exception(e)
            retval = False

        return retval

    def do_stop(self, args):
        """Stop list of scripts.

        Parameters
        ----------
        args : str
            Comma separated list of scripts indices.
        """
        print(f'Stopping script {args}.')
        sal_indices = [int(i) for i in args.split(",")]
        self.model.stop_scripts(sal_indices, terminate=False)

        return False

    def do_terminate(self, args):
        """Terminate list of scripts. Similar to stop but will set `terminate` flag to True.

        Parameters
        ----------
        args : str
            Comma separated list of scripts indices.
        """
        print(f'Terminating script {args}')
        sal_indices = [int(i) for i in args.split(",")]
        self.model.stop_scripts(sal_indices, terminate=True)

    def do_show(self, args):
        """Show current state information about the queue."""

        print("#\n# Showing tasks in the queue.\n#")

        queue_state = self.model.get_queue_state()

        print(f'\tQueue state: {queue_state["state"]}')
        print(f'\tCurrent running: {self.model.parse_info(queue_state["current"])}')
        print(f'\tCurrent queue size: {queue_state["length"]}')
        print(f'\tPast queue size: {queue_state["past_length"]}')

        if queue_state['length'] > 0:
            print(f'\nItems on queue:')
            for item in queue_state['queue_scripts']:
                print(f"{item}: {self.model.parse_info(queue_state['queue_scripts'][item])}")
        else:
            print('\nNo items on queue.')

        if queue_state["past_length"] > 0:
            print(f'\nItems on past queue:')
            for item in queue_state['past_scripts']:
                print(self.model.parse_info(queue_state['past_scripts'][item]))
        else:
            print('\nNo items on past queue.')

    def do_monitor_script_state(self, salindex):
        """Monitor a script state until it is done.

        Parameters
        ----------
        salindex : int
            The salindex of the script to monitor.
        """
        self.monitor_script(int(salindex), self.model.cmd_timeout)

    def do_run(self, args):
        """Request a script to run on the queue.
        """
        try:
            parsed = self.__run_parser.parse_args(args.split())
        except SystemExit:
            return

        if parsed.external is not None and parsed.standard is not None:
            self.log.error("Cannot run with standard and external scripts "
                           "at the same time. Pick one.")
            return
        elif parsed.external is None and parsed.standard is None:
            self.log.error("Choose external or standard script.")
            return

        if parsed.config is not None and parsed.parameters is not None:
            self.log.error("Can either run with configuration file or in line parameters.")
            return

        script_list = self.model.list()

        if parsed.external is not None and parsed.external not in script_list['external']:
            self.log.error('Requested script %s not in the list of external scripts.', parsed.external)
            return

        if parsed.standard is not None and parsed.standard not in script_list['standard']:
            self.log.error('Requested script %s not in the list of standard scripts.', parsed.standard)
            return

        # Reading in input file
        config = ""
        if parsed.config is not None:
            self.log.debug("Reading configuration from %s", parsed.config)
            with open(parsed.config, 'r') as stream:
                yconfig = yaml.load(stream)
                config = yaml.safe_dump(yconfig)
                self.log.debug('Configuration: %s', config)
        elif parsed.parameters is not None:
            self.log.debug("Parsing parameters: %s", parsed.parameters)
            yconfig = {}
            for i in range(int(len(parsed.parameters)/2)):
                yconfig[parsed.parameters[i*2]] = parsed.parameters[i*2+1]
            config = yaml.safe_dump(yconfig)
            self.log.debug('Configuration: %s', config)
        else:
            self.log.debug("No configuration file or in line parameters.")

        # Preparing to load script
        script, is_standard = (parsed.standard, True) if parsed.standard is not None \
            else (parsed.external, False)

        # Add to the queue
        self.log.debug(f"Adding {script} to the queue.")
        salindex = self.model.add(script, is_standard, config)

        if parsed.monitor:
            self.monitor_script(salindex, parsed.timeout)
        else:
            self.log.info(f"Script index: {salindex}.")

    def help_run(self):
        """Print help for the `run` command using the command parser.
        """
        self.__run_parser.print_help()

    def do_set_log_level(self, args):
        """Set log level of the shell script.
        """
        try:
            parsed = self.__set_log_parser.parse_args(args.split())
            logging.basicConfig(level=parsed.vebose)
        except SystemExit:
            return

    def help_set_log_level(self):
        """Helper for set_log_level."""
        self.__set_log_parser.print_help()

    def do_set_script_log_level(self, args):
        """Set log level of a script.
        """
        parsed = self.__set_script_log_parser.parse_args(args.split())
        self.log.debug(f"Setting script {parsed.index} log level to {DETAIL_LEVEL[parsed.verbose]}")
        self.model.set_script_log_level(parsed.index,
                                        DETAIL_LEVEL[parsed.verbose])

    def help_set_script_log_level(self):
        """Helper for set_script_log_level."""
        self.__set_script_log_parser.print_help()

    def do_set_queue_log_level(self, args):
        """Set log level of the queue.
        """
        parsed = self.__set_log_parser.parse_args(args.split())
        self.log.debug(f"Setting queue log level to {DETAIL_LEVEL[parsed.verbose]}")
        self.model.set_queue_log_level(DETAIL_LEVEL[parsed.verbose])

    def help_set_queue_log_level(self):
        """Helper for set_log_level."""
        self.__set_log_parser.print_help()

    def monitor_script(self, salindex, timeout):
        """Monitor the execution of a script. Will block until
        script is in a final state.

        Parameters
        ----------
        salindex : int
        timeout : float
            How long to wait until give up on script. If None, will wait forever.

        """
        queue_state = self.model.get_queue_state()

        if queue_state['state'] != "Running":
            self.log.warning("Queue not running, cannot monitor script.")
            return

        self.log.info(f"Monitoring execution of script {salindex}.")
        self.model.monitor_script(salindex)

        # while True:
        #     try:
        #         info = self.model.get_script_info(salindex)
        #         if info is None:
        #             pass
        #         elif info["updated"]:
        #             print(self.parse_info(info))
        #         elif info["process_state"] >= ScriptProcessState.DONE:
        #             break
        #
        #         if info["process_state"] > ScriptProcessState.UNKNOWN:
        #             level, message, traceback, is_alive = self.model.listen_script_log(int(salindex))
        #             if not is_alive:
        #                 break
        #             elif level > 0:
        #                 self.log.log(level,
        #                              f'[{salindex}]:{message}')
        #         time.sleep(0.1)
        #
        #     except ValueError as value_error:
        #         self.log.debug(repr(value_error))
