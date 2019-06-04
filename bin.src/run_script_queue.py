#!/usr/bin/env python
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

import asyncio
import argparse
import os

from lsst.ts import scriptqueue


def get_script_arg_help(is_standard):
    """Get help for a script directory argument."""
    prefix = "standard" if is_standard else "external"
    help_start = f"Path to {prefix} SAL scripts"
    env_var_name = f"TS_{prefix.upper()}SCRIPTS_DIR"
    if env_var_name in os.environ:
        return f"{help_start}; default is ${env_var_name}={os.environ[env_var_name]}"
    else:
        return f"{help_start}; required because ${env_var_name} is not defined"


parser = argparse.ArgumentParser(f"Start the ScriptQueue")
parser.add_argument("index", help="ScriptQueue CSC index: 1 for Main, 2 for AuxTel", type=int)
parser.add_argument("--standard", help=get_script_arg_help(True))
parser.add_argument("--external", help=get_script_arg_help(False))
parser.add_argument("--verbose", action="store_true", help="Print diagnostic information to stdout")
args = parser.parse_args()
csc = scriptqueue.ScriptQueue(index=args.index,
                              standardpath=args.standard,
                              externalpath=args.external,
                              verbose=args.verbose)
asyncio.get_event_loop().run_until_complete(csc.done_task)
