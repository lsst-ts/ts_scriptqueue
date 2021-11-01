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

import os
import pathlib
import unittest

try:
    from lsst.ts import standardscripts
except ImportError:
    standardscripts = None

try:
    from lsst.ts import externalscripts
except ImportError:
    externalscripts = None
from lsst.ts import scriptqueue


class UtilsTestCase(unittest.TestCase):
    def test_find_public_scripts(self):
        root = os.path.join(os.path.dirname(__file__), "data/standard")
        scripts = scriptqueue.find_public_scripts(root)
        expectedscripts = set(
            (
                "script1",
                "script2",
                "unloadable",
                "subdir/script3",
                "subdir/subsubdir/script4",
            )
        )
        self.assertEqual(set(scripts), expectedscripts)

    @unittest.skipIf(standardscripts is None, "Could not import ts_standardscripts")
    def test_get_default_standard_scripts_dir(self):
        standard_dir = scriptqueue.get_default_scripts_dir(is_standard=True)
        self.assertIsInstance(standard_dir, pathlib.Path)
        assert standard_dir.samefile(standardscripts.get_scripts_dir())
        self.assertEqual(standard_dir.name, "scripts")

    @unittest.skipIf(externalscripts is None, "Could not import ts_externalscripts")
    def test_get_default_external_scripts_dir(self):
        external_dir = scriptqueue.get_default_scripts_dir(is_standard=False)
        self.assertIsInstance(external_dir, pathlib.Path)
        assert external_dir.samefile(externalscripts.get_scripts_dir())
        self.assertEqual(external_dir.name, "scripts")
