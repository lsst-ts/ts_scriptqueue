# This file is part of scriptrunner.
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
import os
import time
import unittest

from scriptrunner.loader_model import LoaderModel


class LoaderModelTestCase(unittest.TestCase):
    def setUp(self):
        self.datadir = os.path.abspath(os.path.join(os.path.dirname(__file__), "data"))
        self.standardpath = os.path.join(self.datadir, "standard")
        self.externalpath = os.path.join(self.datadir, "external")
        self.model = LoaderModel(standardpath=self.standardpath, externalpath=self.externalpath,
                                 timefunc=time.time)

    def test_constructor_errors(self):
        nonexistentpath = os.path.join(self.datadir, "garbage")
        with self.assertRaises(ValueError):
            LoaderModel(standardpath=self.standardpath, externalpath=nonexistentpath, timefunc=time.time)
        with self.assertRaises(ValueError):
            LoaderModel(standardpath=nonexistentpath, externalpath=self.externalpath, timefunc=time.time)
        with self.assertRaises(ValueError):
            LoaderModel(standardpath=nonexistentpath, externalpath=nonexistentpath, timefunc=time.time)
        with self.assertRaises(TypeError):
            LoaderModel(standardpath=self.standardpath, externalpath=self.externalpath, timefunc=5)

    def test_load(self):
        async def doit():
            script_info = await self.model.load(cmd_id=1, path="script1", is_standard=True)
            stdout, stderr = await script_info.process.communicate()
            await script_info.process.wait()
            self.assertEqual(script_info.process.returncode, 0)
            self.assertEqual(stdout.decode(), "script1 done\n")
            self.assertEqual(stderr.decode(), "")
            self.assertGreater(script_info.timestamp_start, 0)
            # the callback that sets timestamp_end hasn't been called yet,
            # so sleep to give it a chance
            await asyncio.sleep(0)
            self.assertGreater(script_info.timestamp_end, script_info.timestamp_start)

        asyncio.get_event_loop().run_until_complete(doit())

    def test_makefullpath(self):
        for badpath, is_standard in (
            ("../script5", True),  # file is in external, not standard
            ("subdir/nonex2", True),  # file is not executable
            ("doesnotexist", True),  # file does not exist
            ("subdir/_private", False),  # file is private
            ("subdir/.invisible", False),  # file is invisible
            ("subdir", False),  # not a file
        ):
            with self.subTest(badpath=badpath, is_standard=is_standard):
                with self.assertRaises(RuntimeError):
                    self.model.makefullpath(path=badpath, is_standard=is_standard)

        for goodpath, is_standard in (
            ("subdir/subsubdir/script4", True),
            ("subdir/script3", False),
            ("script2", True),
        ):
            with self.subTest(path=goodpath, is_standard=is_standard):
                root = self.standardpath if is_standard else self.externalpath
                fullpath = self.model.makefullpath(path=goodpath, is_standard=is_standard)
                expected_fullpath = os.path.join(root, goodpath)
                self.assertTrue(fullpath.samefile(expected_fullpath))


if __name__ == "__main__":
    unittest.main()
