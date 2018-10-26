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

import SALPY_ScriptQueue
import salobj
import scriptloader


class ScriptQueueTestCase(unittest.TestCase):
    def setUp(self):
        salobj.test_utils.set_random_lsst_dds_domain()
        self.datadir = os.path.abspath(os.path.join(os.path.dirname(__file__), "data"))
        standardpath = os.path.join(self.datadir, "standard")
        externalpath = os.path.join(self.datadir, "external")
        self.queue = scriptloader.ScriptQueue(standardpath=standardpath,
                                              externalpath=externalpath,
                                              min_sal_index=1000)
        self.queue.summary_state = salobj.State.ENABLED
        self.remote = salobj.Remote(SALPY_ScriptQueue)
        self.process = None

    def tearDown(self):
        nkilled = self.queue.model.terminate_all()
        if nkilled > 0:
            warnings.warn(f"Killed {nkilled} subprocesses")

    def test_add(self):
        async def doit():
            queue_data = await self.remote.evt_queue.next(flush=False, timeout=5)
            self.assertIsNotNone(queue_data)
            self.assertEqual(queue_data.running, True)
            self.assertEqual(queue_data.currentSalIndex, 0)
            self.assertEqual(queue_data.length, 0)
            self.assertEqual(queue_data.pastLength, 0)

            # pause the queue so we know what to expect of queue state
            id_ack = await self.remote.cmd_pause.start(self.remote.cmd_pause.DataType(), timeout=2)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)

            queue_data = await self.remote.evt_queue.next(flush=False, timeout=1)
            self.assertIsNotNone(queue_data)
            self.assertEqual(queue_data.running, False)
            self.assertEqual(queue_data.currentSalIndex, 0)
            self.assertEqual(queue_data.length, 0)
            self.assertEqual(queue_data.pastLength, 0)

            add_data = self.remote.cmd_add.DataType()
            add_data.isStandard = False
            add_data.path = "script1"
            add_data.config = "wait_time: 0.1"
            add_data.location = SALPY_ScriptQueue.add_Last
            add_data.descr = "test_add"
            id_ack = await self.remote.cmd_add.start(add_data, timeout=2)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)

            queue_data = await self.remote.evt_queue.next(flush=False, timeout=1)
            self.assertIsNotNone(queue_data)
            self.assertEqual(queue_data.running, False)
            self.assertEqual(queue_data.currentSalIndex, 0)
            self.assertEqual(queue_data.length, 1)
            self.assertEqual(queue_data.salIndices[0], 1000)
            self.assertEqual(queue_data.pastLength, 0)

            id_ack = await self.remote.cmd_resume.start(self.remote.cmd_resume.DataType(), timeout=2)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)

            queue_data = await self.remote.evt_queue.next(flush=False, timeout=1)
            self.assertEqual(queue_data.running, True)
            self.assertEqual(queue_data.currentSalIndex, 1000)
            self.assertEqual(queue_data.length, 0)
            self.assertEqual(queue_data.pastLength, 0)

        asyncio.get_event_loop().run_until_complete(doit())

    def test_move(self):
        async def doit():
            queue_data = await self.remote.evt_queue.next(flush=False, timeout=1)
            self.assertIsNotNone(queue_data)
            self.assertEqual(queue_data.running, True)

            # pause the queue so we know what to expect of queue state
            id_ack = await self.remote.cmd_pause.start(self.remote.cmd_pause.DataType(), timeout=2)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)

            queue_data = await self.remote.evt_queue.next(flush=False, timeout=1)
            self.assertIsNotNone(queue_data)
            self.assertEqual(queue_data.running, False)

            # queue a few scripts as quickly as possible
            num_scripts = 3
            expected_sal_indices = [1000, 1001, 1002]
            add_data_list = []
            for i in range(num_scripts):
                add_data = self.remote.cmd_add.DataType()
                add_data.isStandard = False
                add_data.path = "script1"
                add_data.config = "wait_time: 0.1"
                add_data.location = SALPY_ScriptQueue.add_Last
                add_data.descr = "test_move"
                add_data_list.append(add_data)
            coro_list = [self.remote.cmd_add.start(add_data, timeout=20) for add_data in add_data_list]
            id_ack_list = await asyncio.gather(*coro_list)
            for id_ack in id_ack_list:
                self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)

            # make sure the scripts got queued in the correct order
            for n in range(1, num_scripts + 1):
                queue_data = await self.remote.evt_queue.next(flush=False, timeout=1)
                self.assertIsNotNone(queue_data)
                self.assertEqual(queue_data.running, False)
                self.assertEqual(queue_data.currentSalIndex, 0)
                self.assertEqual(queue_data.length, n)
                self.assertEqual(list(queue_data.salIndices[0:n]), expected_sal_indices[0:n])
                self.assertEqual(queue_data.pastLength, 0)

            # move the scripts
            move_data = self.remote.cmd_move.DataType()
            move_data.salIndex = 1002
            move_data.location = SALPY_ScriptQueue.add_First
            id_ack = await self.remote.cmd_move.start(move_data)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)

            queue_data = await self.remote.evt_queue.next(flush=False, timeout=1)
            self.assertIsNotNone(queue_data)
            self.assertEqual(queue_data.running, False)
            self.assertEqual(queue_data.currentSalIndex, 0)
            self.assertEqual(queue_data.length, 3)
            self.assertEqual(list(queue_data.salIndices[0:3]), [1002, 1000, 1001])
            self.assertEqual(queue_data.pastLength, 0)

            move_data.salIndex = 1000
            move_data.location = SALPY_ScriptQueue.add_After
            move_data.locationSalIndex = 1001
            id_ack = await self.remote.cmd_move.start(move_data)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)

            queue_data = await self.remote.evt_queue.next(flush=False, timeout=1)
            self.assertIsNotNone(queue_data)
            self.assertEqual(queue_data.running, False)
            self.assertEqual(queue_data.currentSalIndex, 0)
            self.assertEqual(queue_data.length, 3)
            self.assertEqual(list(queue_data.salIndices[0:3]), [1002, 1001, 1000])
            self.assertEqual(queue_data.pastLength, 0)

            move_data.salIndex = 1000
            move_data.location = SALPY_ScriptQueue.add_Before
            move_data.locationSalIndex = 1002
            id_ack = await self.remote.cmd_move.start(move_data)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)

            queue_data = await self.remote.evt_queue.next(flush=False, timeout=1)
            self.assertIsNotNone(queue_data)
            self.assertEqual(queue_data.running, False)
            self.assertEqual(queue_data.currentSalIndex, 0)
            self.assertEqual(queue_data.length, 3)
            self.assertEqual(list(queue_data.salIndices[0:3]), [1000, 1002, 1001])
            self.assertEqual(queue_data.pastLength, 0)

            move_data.salIndex = 1002
            move_data.location = SALPY_ScriptQueue.add_Last
            id_ack = await self.remote.cmd_move.start(move_data)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)

            queue_data = await self.remote.evt_queue.next(flush=False, timeout=1)
            self.assertIsNotNone(queue_data)
            self.assertEqual(queue_data.running, False)
            self.assertEqual(queue_data.currentSalIndex, 0)
            self.assertEqual(queue_data.length, 3)
            self.assertEqual(list(queue_data.salIndices[0:3]), [1000, 1001, 1002])
            self.assertEqual(queue_data.pastLength, 0)

            self.queue.model.terminate_all()
            for i in range(num_scripts):
                queue_data = await self.remote.evt_queue.next(flush=False, timeout=1)
            self.assertEqual(queue_data.length, 0)

        asyncio.get_event_loop().run_until_complete(doit())


if __name__ == "__main__":
    unittest.main()
