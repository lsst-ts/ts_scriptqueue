#!/usr/bin/env python

import asyncio
import argparse

from lsst.ts import scriptqueue

parser = argparse.ArgumentParser(f"Start the ScriptQueue")
parser.add_argument("index", help="ScriptQueue CSC index: 1 for Main, 2 for AuxTel", type=int)
parser.add_argument("standardpath", help="Path to standard SAL scripts")
parser.add_argument("externalpath", help="Path to external SAL scripts")
parser.add_argument("--verbose", action="store_true", help="Print diagnostic information to stdout")
args = parser.parse_args()
csc = scriptqueue.ScriptQueue(index=args.index,
                              standardpath=args.standardpath,
                              externalpath=args.externalpath,
                              verbose=args.verbose)
asyncio.get_event_loop().run_until_complete(csc.done_task)
