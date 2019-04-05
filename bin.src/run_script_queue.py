#!/usr/bin/env python

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
