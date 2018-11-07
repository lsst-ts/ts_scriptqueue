#!/usr/bin/env python

import asyncio
import argparse

from ts import scriptqueue

parser = argparse.ArgumentParser(f"Start the ScriptQueue")
parser.add_argument("index", help="ScriptQueue CSC index: 1 for Main, 2 for AuxTel")
parser.add_argument("standardpath", help="Path to standard SAL scripts")
parser.add_argument("externalpath", help="Path to external SAL scripts")
args = parser.parse_args()
csc = scriptqueue.ScriptQueue(index=args.index,
                              standardpath=args.standardpath,
                              externalpath=args.externalpath)
asyncio.get_event_loop().run_until_complete(csc.done_task)
