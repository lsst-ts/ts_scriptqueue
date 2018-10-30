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
import unittest

import ts_scriptqueue.utils


class FindScriptsTestCase(unittest.TestCase):
    def test_find_public_scripts(self):
        root = os.path.join(os.path.dirname(__file__), "data/standard")
        scripts = ts_scriptqueue.utils.find_public_scripts(root)
        expectedscripts = set(("script1", "script2", "subdir/script3", "subdir/subsubdir/script4"))
        self.assertEqual(set(scripts), expectedscripts)


if __name__ == "__main__":
    unittest.main()
