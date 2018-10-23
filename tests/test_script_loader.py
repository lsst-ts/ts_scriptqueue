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

import SALPY_ScriptLoader
import SALPY_Script
import salobj
import scriptloader


class ScriptLoaderTestCase(unittest.TestCase):
    def setUp(self):
        salobj.test_utils.set_random_lsst_dds_domain()
        self.datadir = os.path.abspath(os.path.join(os.path.dirname(__file__), "data"))
        standardpath = os.path.join(self.datadir, "standard")
        externalpath = os.path.join(self.datadir, "external")
        self.loader = scriptloader.ScriptLoader(standardpath=standardpath, externalpath=externalpath)
        self.remote = salobj.Remote(SALPY_ScriptLoader)
        self.process = None

    def tearDown(self):
        nkilled = self.loader.model.terminate_all()
        if nkilled > 0:
            warnings.warn(f"Killed {nkilled} subprocesses")

    def test_load(self):
        async def doit():
            load_data = self.remote.cmd_load.DataType()
            load_data.is_standard = False
            load_data.path = "script1"
            load_data.config = "wait_time: 1"
            info_coro = self.remote.evt_script_info.next(timeout=2)
            id_ack = await self.remote.cmd_load.start(load_data, timeout=30)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            script_info1 = await info_coro
            self.assertEqual(script_info1.process_state, 1)
            self.assertGreater(script_info1.timestamp_start, 0)
            self.assertEqual(script_info1.timestamp_end, 0)
            self.assertEqual(script_info1.cmd_id, id_ack.cmd_id)

            info_coro2 = self.remote.evt_script_info.next(timeout=3)
            remote = salobj.Remote(SALPY_Script, script_info1.index)
            id_ack = await remote.cmd_run.start(remote.cmd_run.DataType(), timeout=2)
            self.assertEqual(id_ack.ack.ack, remote.salinfo.lib.SAL__CMD_COMPLETE)

            script_info2 = await info_coro2
            self.assertEqual(script_info2.process_state, 2)
            self.assertEqual(script_info2.timestamp_start, script_info1.timestamp_start)
            self.assertGreater(script_info2.timestamp_end, script_info2.timestamp_start)
            self.assertEqual(script_info2.cmd_id, script_info1.cmd_id)

        asyncio.get_event_loop().run_until_complete(doit())


if __name__ == "__main__":
    unittest.main()
