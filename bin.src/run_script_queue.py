#!/usr/bin/env python

import asyncio
import argparse

import ts_scriptqueue

parser = argparse.ArgumentParser(f"Start the ScriptQueue")
parser.add_argument("standardpath", help="Path to standard SAL scripts")
parser.add_argument("externalpath", help="Path to external SAL scripts")
args = parser.parse_args()
ts_scriptqueue.ScriptQueue(standardpath=args.standardpath, externalpath=args.externalpath)
asyncio.get_event_loop().run_forever()
