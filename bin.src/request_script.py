#!/usr/bin/env python

import argparse

from lsst.ts.scriptqueue.utils import generate_logfile, configure_logging
from lsst.ts.scriptqueue import __version__
from lsst.ts.scriptqueue.ui import RequestCmd

if __name__ == '__main__':

    description = ["Utility to interact with the script queue."]

    parser = argparse.ArgumentParser(usage="request_script.py [options]",
                                     description=" ".join(description),
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument("index", help="ScriptQueue CSC index: 1 for Main, 2 for AuxTel", type=int)
    parser.add_argument("-v", "--verbose", dest="verbose", action='count', default=0,
                        help="Set the verbosity for the console logging.")
    parser.add_argument("-c", "--console-format", dest="console_format",
                        default="[%(levelname)s]:%(message)s",
                        help="Override the console format.")

    args = parser.parse_args()

    logfilename = generate_logfile("request_script")
    configure_logging(args.verbose, args.console_format, logfilename)

    rs = RequestCmd(args.index)

    rs.cmdloop()
