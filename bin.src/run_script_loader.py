#!/usr/bin/env python

import asyncio
import argparse

import scriptloader

parser = argparse.ArgumentParser(f"Start the ScriptLoader")
parser.add_argument("standardpath", help="Path to standard SAL scripts")
parser.add_argument("externalpath", help="Path to external SAL scripts")
args = parser.parse_args()
scriptloader.ScriptLoader(standardpath=args.standardpath, externalpath=args.externalpath)
asyncio.get_event_loop().run_forever()
