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

__all__ = ["parse_run_one_script_cmd", "run_one_script"]

import argparse
import datetime
import logging
import pathlib
import random

from lsst.ts import salobj
from lsst.ts import scriptqueue


class ConfigAction(argparse.Action):
    """Read config from a file.
    """
    def __call__(self, parser, namespace, value, option_string=None):
        """Read config from a file.

        Parameters
        ----------
        parser : `argparse.ArgumentParser`
            Argument parser.
        namespace : `argparse.Namespace`
            Parsed command. The following attributes are updated by this
            method: ``namespace.config``.
        value : `str`
            Configuration file path
        option_string : `str`, optional
            Option value specified by the user.
        """
        if not pathlib.Path(value).is_file():
            parser.error(f"Cannot find --config file {value}")
        with open(value, "r") as f:
            config = f.read()
            namespace.config = config


class ParameterAction(argparse.Action):
    """Parse name=value pairs as config.
    """
    def __call__(self, parser, namespace, values, option_string):
        """Parse name=value pairs as config.

        Parameters
        ----------
        parser : `argparse.ArgumentParser`
            Argument parser.
        namespace : `argparse.Namespace`
            Parsed command. The ``namespace.config`` attribute is updated.
        values : `list`
            A list of ``configItemName=value`` pairs.
        option_string : `str`
            Option value specified by the user.
        """
        config_list = []
        for nameValue in values:
            name, sep, valueStr = nameValue.partition("=")
            if not valueStr:
                parser.error("%s value %s must be in form name=value" % (option_string, nameValue))
            config_list.append(f"{name}: {valueStr}")
        namespace.config = "\n".join(config_list)


def parse_run_one_script_cmd(args=None):
    """Parse command-line arguments for run_one_script.
    """
    description = "Run one SAL script."

    parser = argparse.ArgumentParser(description=description,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("script", help="Path of script to run.")
    parser.add_argument("--index", type=int, help="Script index; default is a random value")
    parser.add_argument("-l", "--loglevel", type=int,
                        help="Logging level; debug=10, info=20, warning=30, error=40")

    config_group = parser.add_mutually_exclusive_group(required=False)
    config_group.add_argument("-c", "--config", action=ConfigAction,
                              help="Configuration vales for the script, in yaml format.")
    config_group.add_argument("-p", "--parameters", nargs="+", action=ParameterAction,
                              help="Configuration for the script, in key=value format.")

    cmd = parser.parse_args(args=args)
    if cmd.index is None:
        cmd.index = random.randint(1, scriptqueue.script_queue.SCRIPT_INDEX_MULT-1)
    elif not 0 < cmd.index <= salobj.MAX_SAL_INDEX:
        parser.error(f"index={cmd.index} must be in the range [1, {salobj.MAX_SAL_INDEX}], inclusive")
    if cmd.config is None:
        cmd.config = ""
    return cmd


async def run_one_script(index, script, config, loglevel=None):
    """Run one SAL script.

    Parameters
    ----------
    index : `int`
        SAL index of script
    script :  `str`, `bytes` or `os.PathLike`
        Path to script
    config : `str`
        Configuration for the script encoded as yaml; "" if no configuration.
    loglevel : `int`
        Logging level.
    """
    if index == 0:
        raise ValueError("index must not be zero")
    script = pathlib.Path(script).resolve()
    if not script.is_file():
        raise ValueError(f"Cannot find script {script}")
    print(f"index={index}; script={script}; config={config!r}")
    async with salobj.Domain() as domain:
        # make a remote to communicate with the script; set max_history=0
        # because it needs no late joiner data
        remote = salobj.Remote(domain=domain, name="Script", index=index,
                               evt_max_history=0, tel_max_history=0)
        await remote.start_task

        def log_callback(data):
            iso_time = datetime.datetime.now().time().isoformat(timespec="milliseconds")
            print(f"{iso_time} {logging.getLevelName(data.level)}: {data.message}")
        remote.evt_logMessage.callback = log_callback

        print("starting the script process")
        script_info = scriptqueue.ScriptInfo(log=remote.salinfo.log, remote=remote, index=index, seq_num=1,
                                             is_standard=False, path=script, config=config,
                                             descr="run_one_script.py")
        try:
            remote.evt_state.callback = script_info._script_state_callback
            await script_info.start_loading(script)
            print("waiting for the script to load")
            await script_info.start_task
            print("configuring the script")
            await script_info.config_task
            if loglevel is not None:
                print(f"setting script log level to {loglevel}")
                await remote.cmd_setLogLevel.set_start(level=loglevel, timeout=5)
            print("running the script")
            script_info.run()
            await script_info.process_task
            print("script succeeded")
        except BaseException:
            # make sure the background process is terminated
            script_info.terminate()
            raise
