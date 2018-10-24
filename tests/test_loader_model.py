# This file is part of scriptloader.
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
import unittest
import warnings

import salobj
import scriptloader


class LoaderModelTestCase(unittest.TestCase):
    def setUp(self):
        salobj.test_utils.set_random_lsst_dds_domain()
        self.datadir = os.path.abspath(os.path.join(os.path.dirname(__file__), "data"))
        self.standardpath = os.path.join(self.datadir, "standard")
        self.externalpath = os.path.join(self.datadir, "external")
        self.model = scriptloader.LoaderModel(standardpath=self.standardpath, externalpath=self.externalpath)

    def tearDown(self):
        nkilled = self.model.terminate_all()
        if nkilled > 0:
            warnings.warn(f"Killed {nkilled} subprocesses")

    def test_constructor_errors(self):
        nonexistentpath = os.path.join(self.datadir, "garbage")
        with self.assertRaises(ValueError):
            scriptloader.LoaderModel(standardpath=self.standardpath, externalpath=nonexistentpath)
        with self.assertRaises(ValueError):
            scriptloader.LoaderModel(standardpath=nonexistentpath, externalpath=self.externalpath)
        with self.assertRaises(ValueError):
            scriptloader.LoaderModel(standardpath=nonexistentpath, externalpath=nonexistentpath)

    def test_load_without_config(self):
        async def doit():
            script_path = os.path.join("subdir", "subsubdir", "script4")
            load_coro = self.model.load(cmd_id=1, path=script_path, is_standard=True, config=None)
            script_info = await asyncio.wait_for(load_coro, 20)
            await script_info.process.wait()
            self.assertEqual(script_info.process.returncode, 0)
            self.assertGreater(script_info.timestamp_start, 0)
            # the callback that sets timestamp_end hasn't been called yet,
            # so sleep to give it a chance
            await asyncio.sleep(0.1)
            self.assertGreater(script_info.timestamp_end, script_info.timestamp_start)

        asyncio.get_event_loop().run_until_complete(doit())

    def test_load_and_configure(self):
        async def doit():
            script_path = os.path.join("subdir", "script6")
            config = "wait_time: 0.1"
            load_coro = self.model.load(cmd_id=1, path=script_path, is_standard=False, config=config)
            script_info = await asyncio.wait_for(load_coro, 20)
            self.assertIsNone(script_info.process.returncode)

            remote = script_info.remote
            state = remote.evt_state.get()
            self.assertEqual(state.state, scriptloader.ScriptState.CONFIGURED)

            await remote.cmd_run.start(remote.cmd_run.DataType(), 2)
            await asyncio.wait_for(script_info.process.wait(), 2)

        asyncio.get_event_loop().run_until_complete(doit())

    def test_makefullpath(self):
        for is_standard, badpath in (
            (True, "../script5"),  # file is in external, not standard
            (True, "subdir/nonex2"),  # file is not executable
            (True, "doesnotexist"),  # file does not exist
            (False, "subdir/_private"),  # file is private
            (False, "subdir/.invisible"),  # file is invisible
            (False, "subdir"),  # not a file
        ):
            with self.subTest(is_standard=is_standard, badpath=badpath):
                with self.assertRaises(RuntimeError):
                    self.model.makefullpath(is_standard=is_standard, path=badpath)

        for is_standard, goodpath in (
            (True, "subdir/subsubdir/script4"),
            (False, "subdir/script3"),
            (True, "script2"),
        ):
            with self.subTest(is_standard=is_standard, path=goodpath):
                root = self.standardpath if is_standard else self.externalpath
                fullpath = self.model.makefullpath(is_standard=is_standard, path=goodpath)
                expected_fullpath = os.path.join(root, goodpath)
                self.assertTrue(fullpath.samefile(expected_fullpath))


if __name__ == "__main__":
    unittest.main()
