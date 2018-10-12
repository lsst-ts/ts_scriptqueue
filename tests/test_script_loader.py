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
import unittest

import SALPY_ScriptLoader
import salobj
import scriptrunner


class ScriptLoaderTestCase(unittest.TestCase):
    def setUp(self):
        self.datadir = os.path.abspath(os.path.join(os.path.dirname(__file__), "data"))
        standardpath = os.path.join(self.datadir, "standard")
        externalpath = os.path.join(self.datadir, "external")
        self.loader = scriptrunner.ScriptLoader(standardpath=standardpath, externalpath=externalpath)
        self.remote = salobj.Remote(SALPY_ScriptLoader)

    def test_load(self):
        async def doit():
            load_data = self.remote.cmd_load.DataType()
            load_data.path = "script1"
            load_data.is_standard = True
            script_info_data = self.remote.evt_script_info.DataType()
            info_coro = self.remote.evt_script_info.next(script_info_data)
            id_ack = await self.remote.cmd_load.start(load_data, timeout=5)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            script_info1 = await info_coro
            self.assertEqual(script_info1.process_state, 1)
            self.assertGreater(script_info1.timestamp_start, 0)
            self.assertEqual(script_info1.timestamp_end, 0)
            self.assertEqual(script_info1.cmd_id, id_ack.cmd_id)

            script_info2 = await self.remote.evt_script_info.next(script_info_data)
            self.assertEqual(script_info2.process_state, 2)
            self.assertEqual(script_info2.timestamp_start, script_info1.timestamp_start)
            self.assertGreater(script_info2.timestamp_end, script_info2.timestamp_start)
            self.assertEqual(script_info2.cmd_id, id_ack.cmd_id)

        asyncio.get_event_loop().run_until_complete(doit())


if __name__ == "__main__":
    unittest.main()
