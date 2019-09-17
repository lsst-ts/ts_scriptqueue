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
