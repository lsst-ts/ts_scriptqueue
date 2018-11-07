# This file is part of scriptqueue.
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
from ts import scriptqueue


class ScriptQueueTestCase(unittest.TestCase):
    def setUp(self):
        salobj.test_utils.set_random_lsst_dds_domain()
        self.datadir = os.path.abspath(os.path.join(os.path.dirname(__file__), "data"))
        standardpath = os.path.join(self.datadir, "standard")
        externalpath = os.path.join(self.datadir, "external")
        self.queue = scriptqueue.ScriptQueue(standardpath=standardpath,
                                             externalpath=externalpath,
                                             min_sal_index=1000)
        self.queue.summary_state = salobj.State.DISABLED
        self.remote = salobj.Remote(SALPY_ScriptQueue)
        self.process = None

    def tearDown(self):
        nkilled = len(self.queue.model.terminate_all())
        if nkilled > 0:
            warnings.warn(f"Killed {nkilled} subprocesses")

    async def assert_next_queue(self, enabled=True, running=False,
                                currentSalIndex=0, salIndices=(), pastSalIndices=()):
        """Get the next queue event and check values.

        If wait is True then wait for the next update before checking.

        The defaults are appropriate to an enabled, paused queue
        with no scripts.

        Parameters
        ----------
        enabled : `bool`
            Is the queue enabled?
        running : `bool`
            Is the queue running?
        current_sal_index : `int`
            SAL index of current script, or 0 if no current script.
        sal_indices : ``sequence`` of `int`
            SAL indices of scripts on the queue.
        past_sal_indices : ``sequence`` of `int`
            SAL indices of scripts in history.
        """
        queue_data = await self.remote.evt_queue.next(flush=False, timeout=20)
        self.assertIsNotNone(queue_data)
        if enabled:
            self.assertTrue(queue_data.enabled)
        else:
            self.assertFalse(queue_data.enabled)
        if running:
            self.assertTrue(queue_data.running)
        else:
            self.assertFalse(queue_data.running)
        self.assertEqual(queue_data.currentSalIndex, currentSalIndex)
        self.assertEqual(queue_data.length, len(salIndices))
        self.assertEqual(list(queue_data.salIndices[0:len(salIndices)]), list(salIndices))
        self.assertEqual(queue_data.pastLength, len(pastSalIndices))
        self.assertEqual(list(queue_data.pastSalIndices[0:len(pastSalIndices)]), list(pastSalIndices))

    def test_add(self):
        """Test add, remove and showScript."""
        is_standard = False
        path = "script1"
        config = "wait_time: 1"  # give showScript time to run

        async def doit():
            await self.assert_next_queue(enabled=False, running=True)

            def make_add_data(location, locationSalIndex=0):
                add_data = self.remote.cmd_add.DataType()
                add_data.isStandard = is_standard
                add_data.path = path
                # allow some time for the showScript command to run
                add_data.config = config
                add_data.location = location
                add_data.locationSalIndex = locationSalIndex
                add_data.descr = "test_add"
                return add_data

            # check that add fails when the queue is not enabled
            add_data = make_add_data(location=SALPY_ScriptQueue.add_Last)
            id_ack = await self.remote.cmd_add.start(add_data, timeout=2)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_FAILED)

            id_ack = await self.remote.cmd_enable.start(self.remote.cmd_enable.DataType(), timeout=2)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            await self.assert_next_queue(enabled=True, running=True)

            # pause the queue so we know what to expect of queue state
            id_ack = await self.remote.cmd_pause.start(self.remote.cmd_pause.DataType(), timeout=10)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            await self.assert_next_queue(running=False)

            # add script 1000; queue is empty to location is irrelevant
            add_data = make_add_data(location=SALPY_ScriptQueue.add_Last)
            id_ack = await self.remote.cmd_add.start(add_data, timeout=10)
            cmd_id_1000 = id_ack.cmd_id
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            await self.assert_next_queue(salIndices=[1000])

            # run showScript for a script that has not been configured
            showScript_data = self.remote.cmd_showScript.DataType()
            showScript_data.salIndex = 1000
            id_ack = await self.remote.cmd_showScript.start(showScript_data)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            script_data = self.remote.evt_script.get()
            self.assertEqual(script_data.cmdId, cmd_id_1000)
            self.assertEqual(script_data.salIndex, 1000)
            self.assertEqual(script_data.isStandard, is_standard)
            self.assertEqual(script_data.path, path)
            self.assertEqual(script_data.processState, self.queue.salinfo.lib.script_Loading)
            self.assertGreater(script_data.timestamp, 0)
            self.assertEqual(script_data.duration, 0)

            # add script 1001 last: test add last
            add_data = make_add_data(location=SALPY_ScriptQueue.add_Last)
            id_ack = await self.remote.cmd_add.start(add_data, timeout=10)
            cmd_id1001 = id_ack.cmd_id
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            await self.assert_next_queue(salIndices=[1000, 1001])

            # add script 1002 first: test add first
            add_data = make_add_data(location=SALPY_ScriptQueue.add_First)
            id_ack = await self.remote.cmd_add.start(add_data, timeout=10)
            cmd_id1002 = id_ack.cmd_id
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            await self.assert_next_queue(salIndices=[1002, 1000, 1001])

            # add script 1003 after 1001: test add after last
            add_data = make_add_data(location=SALPY_ScriptQueue.add_After,
                                     locationSalIndex=1001)
            id_ack = await self.remote.cmd_add.start(add_data, timeout=10)
            cmd_id1003 = id_ack.cmd_id
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            await self.assert_next_queue(salIndices=[1002, 1000, 1001, 1003])

            # add script 1004 after 1002: test add after not-last
            add_data = make_add_data(location=SALPY_ScriptQueue.add_After,
                                     locationSalIndex=1002)
            id_ack = await self.remote.cmd_add.start(add_data, timeout=10)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            await self.assert_next_queue(salIndices=[1002, 1004, 1000, 1001, 1003])

            # add script 1005 before 1002: test add before first
            add_data = make_add_data(location=SALPY_ScriptQueue.add_Before,
                                     locationSalIndex=1002)
            id_ack = await self.remote.cmd_add.start(add_data, timeout=10)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            await self.assert_next_queue(salIndices=[1005, 1002, 1004, 1000, 1001, 1003])

            # add script 1006 before 1000: test add before not first
            add_data = make_add_data(location=SALPY_ScriptQueue.add_Before,
                                     locationSalIndex=1000)
            id_ack = await self.remote.cmd_add.start(add_data, timeout=10)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            await self.assert_next_queue(salIndices=[1005, 1002, 1004, 1006, 1000, 1001, 1003])

            # try some failed adds
            # incorrect path
            add_data = make_add_data(location=SALPY_ScriptQueue.add_First)
            add_data.path = "bogus_script_name"
            id_ack = await self.remote.cmd_add.start(add_data, timeout=10)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_FAILED)

            # incorrect location
            add_data = make_add_data(location=25)
            id_ack = await self.remote.cmd_add.start(add_data, timeout=10)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_FAILED)

            # incorrect locationSalIndex
            add_data = make_add_data(location=SALPY_ScriptQueue.add_After,
                                     locationSalIndex=4321)
            id_ack = await self.remote.cmd_add.start(add_data, timeout=10)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_FAILED)

            # make sure the incorrect add commands did not alter the queue
            id_ack = await self.remote.cmd_showQueue.start(self.remote.cmd_showQueue.DataType())
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            await self.assert_next_queue(salIndices=[1005, 1002, 1004, 1006, 1000, 1001, 1003])

            # stop a few scripts
            stop_data = self.remote.cmd_stop.DataType()
            stop_data.salIndex = 1006
            id_ack = await self.remote.cmd_stop.start(stop_data, timeout=10)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            await self.assert_next_queue(salIndices=[1005, 1002, 1004, 1000, 1001, 1003])

            stop_data = self.remote.cmd_stop.DataType()
            stop_data.salIndex = 1005
            id_ack = await self.remote.cmd_stop.start(stop_data, timeout=10)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            await self.assert_next_queue(salIndices=[1002, 1004, 1000, 1001, 1003])

            stop_data = self.remote.cmd_stop.DataType()
            stop_data.salIndex = 1000
            id_ack = await self.remote.cmd_stop.start(stop_data, timeout=10)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            await self.assert_next_queue(salIndices=[1002, 1004, 1001, 1003])

            stop_data = self.remote.cmd_stop.DataType()
            stop_data.salIndex = 1004
            id_ack = await self.remote.cmd_stop.start(stop_data, timeout=10)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            await self.assert_next_queue(salIndices=[1002, 1001, 1003])

            # try to stop a non-existent script
            stop_data = self.remote.cmd_stop.DataType()
            stop_data.salIndex = 5432
            id_ack = await self.remote.cmd_stop.start(stop_data, timeout=10)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_FAILED)

            # make sure the failed stop did not affect the queue
            id_ack = await self.remote.cmd_showQueue.start(self.remote.cmd_showQueue.DataType())
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            await self.assert_next_queue(salIndices=[1002, 1001, 1003])

            # make sure all scripts are runnable before starting the queue
            # so the queue data is more predictable (otherwise the queue
            # may start up with no script running)
            await self.wait_runnable(1001, 1002, 1003)

            # get script state for a script that has been configured but is not running
            showScript_data = self.remote.cmd_showScript.DataType()
            showScript_data.salIndex = 1003
            id_ack = await self.remote.cmd_showScript.start(showScript_data)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            script_data = self.remote.evt_script.get()
            self.assertEqual(script_data.salIndex, 1003)
            self.assertEqual(script_data.cmdId, cmd_id1003)
            self.assertEqual(script_data.isStandard, is_standard)
            self.assertEqual(script_data.path, path)
            self.assertEqual(script_data.processState, self.queue.salinfo.lib.script_Configured)
            self.assertGreater(script_data.timestamp, 0)
            self.assertEqual(script_data.duration, 0)

            id_ack = await self.remote.cmd_resume.start(self.remote.cmd_resume.DataType(), timeout=10)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            await self.assert_next_queue(running=True, currentSalIndex=1002,
                                         salIndices=[1001, 1003], pastSalIndices=[])

            # get script state for the script that is running
            showScript_data = self.remote.cmd_showScript.DataType()
            showScript_data.salIndex = 1002
            id_ack = await self.remote.cmd_showScript.start(showScript_data)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            script_data = self.remote.evt_script.get()
            self.assertEqual(script_data.salIndex, 1002)
            self.assertEqual(script_data.cmdId, cmd_id1002)
            self.assertEqual(script_data.isStandard, is_standard)
            self.assertEqual(script_data.path, path)
            self.assertEqual(script_data.processState, self.queue.salinfo.lib.script_Running)
            self.assertGreater(script_data.timestamp, 0)
            self.assertEqual(script_data.duration, 0)

            await self.assert_next_queue(running=True, currentSalIndex=1001,
                                         salIndices=[1003], pastSalIndices=[1002])
            await self.assert_next_queue(running=True, currentSalIndex=1003,
                                         salIndices=[], pastSalIndices=[1001, 1002])
            await self.assert_next_queue(running=True, currentSalIndex=0,
                                         salIndices=[], pastSalIndices=[1003, 1001, 1002])

            # get script state for a script that has been run
            showScript_data = self.remote.cmd_showScript.DataType()
            showScript_data.salIndex = 1001
            id_ack = await self.remote.cmd_showScript.start(showScript_data)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            script_data = self.remote.evt_script.get()
            self.assertEqual(script_data.salIndex, 1001)
            self.assertEqual(script_data.cmdId, cmd_id1001)
            self.assertEqual(script_data.isStandard, is_standard)
            self.assertEqual(script_data.path, path)
            self.assertEqual(script_data.processState, self.queue.salinfo.lib.script_Complete)
            self.assertGreater(script_data.timestamp, 0)
            self.assertGreater(script_data.duration, 0.9)  # wait time is 1

            # get script state for non-existent script
            showScript_data = self.remote.cmd_showScript.DataType()
            showScript_data.salIndex = 3579
            id_ack = await self.remote.cmd_showScript.start(showScript_data)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_FAILED)

        asyncio.get_event_loop().run_until_complete(doit())

    def test_move(self):
        """Test move, pause and showQueue
        """
        async def doit():
            await self.assert_next_queue(enabled=False, running=True)

            # pause the queue so we know what to expect of queue state
            # also check that pause works while not enabled
            id_ack = await self.remote.cmd_pause.start(self.remote.cmd_pause.DataType(), timeout=10)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            await self.assert_next_queue(enabled=False, running=False)

            id_ack = await self.remote.cmd_enable.start(self.remote.cmd_enable.DataType(), timeout=2)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            await self.assert_next_queue(enabled=True, running=False)

            # queue scripts 1000, 1001 and 1002
            sal_indices = [1000, 1001, 1002]
            for i, index in enumerate(sal_indices):
                add_data = self.remote.cmd_add.DataType()
                add_data.isStandard = True
                add_data.path = os.path.join("subdir", "script3")
                add_data.config = "wait_time: 0.1"
                add_data.location = SALPY_ScriptQueue.add_Last
                add_data.descr = f"test_move {i}"
                id_ack = await self.remote.cmd_add.start(add_data, timeout=10)
                self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
                await self.assert_next_queue(salIndices=sal_indices[0:i+1])

            # move 1002 first
            move_data = self.remote.cmd_move.DataType()
            move_data.salIndex = 1002
            move_data.location = SALPY_ScriptQueue.add_First
            id_ack = await self.remote.cmd_move.start(move_data)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            await self.assert_next_queue(salIndices=[1002, 1000, 1001])

            # move 1002 first again (should be a no-op)
            move_data = self.remote.cmd_move.DataType()
            move_data.salIndex = 1002
            move_data.location = SALPY_ScriptQueue.add_First
            id_ack = await self.remote.cmd_move.start(move_data)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            await self.assert_next_queue(salIndices=[1002, 1000, 1001])

            # move 1000 last
            move_data = self.remote.cmd_move.DataType()
            move_data.salIndex = 1000
            move_data.location = SALPY_ScriptQueue.add_Last
            id_ack = await self.remote.cmd_move.start(move_data)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            await self.assert_next_queue(salIndices=[1002, 1001, 1000])

            # move 1000 last again (should be a no-op)
            move_data = self.remote.cmd_move.DataType()
            move_data.salIndex = 1000
            move_data.location = SALPY_ScriptQueue.add_Last
            id_ack = await self.remote.cmd_move.start(move_data)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            await self.assert_next_queue(salIndices=[1002, 1001, 1000])

            # move 1000 before 1002: before first
            move_data.salIndex = 1000
            move_data.location = SALPY_ScriptQueue.add_Before
            move_data.locationSalIndex = 1002
            id_ack = await self.remote.cmd_move.start(move_data)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            await self.assert_next_queue(salIndices=[1000, 1002, 1001])

            # move 1001 before 1002: before not-first
            move_data.salIndex = 1001
            move_data.location = SALPY_ScriptQueue.add_Before
            move_data.locationSalIndex = 1002
            id_ack = await self.remote.cmd_move.start(move_data)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            await self.assert_next_queue(salIndices=[1000, 1001, 1002])

            # move 1000 after 1002: after last
            move_data.salIndex = 1000
            move_data.location = SALPY_ScriptQueue.add_After
            move_data.locationSalIndex = 1002
            id_ack = await self.remote.cmd_move.start(move_data)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            await self.assert_next_queue(salIndices=[1001, 1002, 1000])

            # move 1001 after 1002: after not-last
            move_data.salIndex = 1001
            move_data.location = SALPY_ScriptQueue.add_After
            move_data.locationSalIndex = 1002
            id_ack = await self.remote.cmd_move.start(move_data)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            await self.assert_next_queue(salIndices=[1002, 1001, 1000])

            # move 1000 after itself: this should be a no-op
            # but it should still output the queue event
            move_data.salIndex = 1000
            move_data.location = SALPY_ScriptQueue.add_After
            move_data.locationSalIndex = 1000
            id_ack = await self.remote.cmd_move.start(move_data)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            await self.assert_next_queue(salIndices=[1002, 1001, 1000])

            # move 1001 before itself: this should be a no-op
            # but it should still output the queue event
            move_data.salIndex = 1001
            move_data.location = SALPY_ScriptQueue.add_After
            move_data.locationSalIndex = 1001
            id_ack = await self.remote.cmd_move.start(move_data)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            await self.assert_next_queue(salIndices=[1002, 1001, 1000])

            # try some incorrect moves
            move_data.salIndex = 1234  # no such script
            move_data.location = SALPY_ScriptQueue.add_Last
            id_ack = await self.remote.cmd_move.start(move_data)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_FAILED)

            move_data.salIndex = 1001
            move_data.location = 21  # no such location
            id_ack = await self.remote.cmd_move.start(move_data)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_FAILED)

            move_data.salIndex = 1001
            move_data.location = SALPY_ScriptQueue.add_Before
            move_data.locationSalIndex = 1234  # no such script
            id_ack = await self.remote.cmd_move.start(move_data)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_FAILED)

            # try incorrect index and the same "before" locationSalIndex
            move_data.salIndex = 1234
            move_data.location = SALPY_ScriptQueue.add_Before
            move_data.locationSalIndex = 1234
            id_ack = await self.remote.cmd_move.start(move_data)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_FAILED)

            # try incorrect index and the same "after" locationSalIndex
            move_data.salIndex = 1234
            move_data.location = SALPY_ScriptQueue.add_After
            move_data.locationSalIndex = 1234
            id_ack = await self.remote.cmd_move.start(move_data)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_FAILED)

            # make sure those commands did not alter the queue
            id_ack = await self.remote.cmd_showQueue.start(self.remote.cmd_showQueue.DataType())
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            await self.assert_next_queue(salIndices=[1002, 1001, 1000])

            await self.queue.model.wait_terminate_all()
            for i in range(len(sal_indices)):
                queue_data = await self.remote.evt_queue.next(flush=False, timeout=10)
            self.assertEqual(queue_data.length, 0)

        asyncio.get_event_loop().run_until_complete(doit())

    def test_requeue(self):
        """Test requeue, move and terminate
        """
        async def doit():
            await self.assert_next_queue(enabled=False, running=True)

            id_ack = await self.remote.cmd_enable.start(self.remote.cmd_enable.DataType(), timeout=2)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            await self.assert_next_queue(enabled=True, running=True)

            # pause the queue so we know what to expect of queue state
            id_ack = await self.remote.cmd_pause.start(self.remote.cmd_pause.DataType(), timeout=10)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            await self.assert_next_queue(running=False)

            # queue scripts 1000, 1001 and 1002
            sal_indices = [1000, 1001, 1002]
            for i, index in enumerate(sal_indices):
                add_data = self.remote.cmd_add.DataType()
                add_data.isStandard = True
                add_data.path = "script2"
                add_data.config = "wait_time: 0.1"
                add_data.location = SALPY_ScriptQueue.add_Last
                add_data.descr = f"test_requeue {i}"
                id_ack = await self.remote.cmd_add.start(add_data, timeout=10)
                self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
                await self.assert_next_queue(salIndices=sal_indices[0:i+1])

            # disable the queue and make sure requeue, move and resume fail
            # (I added some jobs before disabling so we have scripts
            # to try to requeue and move).
            id_ack = await self.remote.cmd_disable.start(self.remote.cmd_disable.DataType(), timeout=2)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            await self.assert_next_queue(enabled=False, running=False, salIndices=sal_indices)

            requeue_data = self.remote.cmd_requeue.DataType()
            requeue_data.salIndex = 1000
            requeue_data.location = SALPY_ScriptQueue.requeue_Last
            id_ack = await self.remote.cmd_requeue.start(requeue_data, timeout=10)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_FAILED)

            move_data = self.remote.cmd_move.DataType()
            move_data.salIndex = 1002
            move_data.location = SALPY_ScriptQueue.add_First
            id_ack = await self.remote.cmd_move.start(move_data)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_FAILED)

            id_ack = await self.remote.cmd_resume.start(self.remote.cmd_resume.DataType())
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_FAILED)

            # re-enable the queue and proceed with the rest of the test
            id_ack = await self.remote.cmd_enable.start(self.remote.cmd_enable.DataType(), timeout=2)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            await self.assert_next_queue(enabled=True, running=False, salIndices=sal_indices)

            # requeue 1000 to last
            requeue_data = self.remote.cmd_requeue.DataType()
            requeue_data.salIndex = 1000
            requeue_data.location = SALPY_ScriptQueue.requeue_Last
            id_ack = await self.remote.cmd_requeue.start(requeue_data, timeout=10)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            await self.assert_next_queue(salIndices=[1000, 1001, 1002, 1003])

            # requeue 1001 to first
            requeue_data = self.remote.cmd_requeue.DataType()
            requeue_data.salIndex = 1000
            requeue_data.location = SALPY_ScriptQueue.requeue_First
            id_ack = await self.remote.cmd_requeue.start(requeue_data, timeout=10)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            await self.assert_next_queue(salIndices=[1004, 1000, 1001, 1002, 1003])

            # requeue 1002 to before 1004: before first
            requeue_data = self.remote.cmd_requeue.DataType()
            requeue_data.salIndex = 1002
            requeue_data.location = SALPY_ScriptQueue.requeue_Before
            requeue_data.locationSalIndex = 1004
            id_ack = await self.remote.cmd_requeue.start(requeue_data, timeout=10)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            await self.assert_next_queue(salIndices=[1005, 1004, 1000, 1001, 1002, 1003])

            # requeue 1003 to before 1003: before not-first
            # and check that matching indices is not a problem
            requeue_data = self.remote.cmd_requeue.DataType()
            requeue_data.salIndex = 1003
            requeue_data.location = SALPY_ScriptQueue.requeue_Before
            requeue_data.locationSalIndex = 1003
            id_ack = await self.remote.cmd_requeue.start(requeue_data, timeout=10)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            await self.assert_next_queue(salIndices=[1005, 1004, 1000, 1001, 1002, 1006, 1003])

            # requeue 1003 to after 1003: after last
            # and check that matching indices is not a problem
            requeue_data = self.remote.cmd_requeue.DataType()
            requeue_data.salIndex = 1003
            requeue_data.location = SALPY_ScriptQueue.requeue_After
            requeue_data.locationSalIndex = 1003
            id_ack = await self.remote.cmd_requeue.start(requeue_data, timeout=10)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            await self.assert_next_queue(salIndices=[1005, 1004, 1000, 1001, 1002, 1006, 1003, 1007])

            # requeue 1005 to after 1007: after last
            requeue_data = self.remote.cmd_requeue.DataType()
            requeue_data.salIndex = 1005
            requeue_data.location = SALPY_ScriptQueue.requeue_Last
            requeue_data.locationSalIndex = 1007
            id_ack = await self.remote.cmd_requeue.start(requeue_data, timeout=10)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            await self.assert_next_queue(salIndices=[1005, 1004, 1000, 1001, 1002, 1006, 1003, 1007, 1008])

            # stop all scripts except 1001 and 1002
            sal_indices = [1005, 1004, 1000, 1001, 1002, 1006, 1003, 1007, 1008]
            for remove_index in sal_indices[:]:
                if remove_index in (1001, 1002):
                    continue
                stop_data = self.remote.cmd_stop.DataType()
                stop_data.salIndex = remove_index
                id_ack = await self.remote.cmd_stop.start(stop_data, timeout=10)
                self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)

                sal_indices.remove(remove_index)
                await self.assert_next_queue(salIndices=sal_indices)

            await self.wait_runnable(1001, 1002)

            # run the queue and let it finish
            id_ack = await self.remote.cmd_resume.start(self.remote.cmd_resume.DataType(), timeout=10)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            await self.assert_next_queue(running=True, currentSalIndex=1001,
                                         salIndices=[1002], pastSalIndices=[])
            await self.assert_next_queue(running=True, currentSalIndex=1002,
                                         salIndices=[], pastSalIndices=[1001])
            await self.assert_next_queue(running=True, currentSalIndex=0,
                                         salIndices=[], pastSalIndices=[1002, 1001])

            # pause while we requeue from history
            id_ack = await self.remote.cmd_pause.start(self.remote.cmd_pause.DataType(), timeout=10)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            await self.assert_next_queue(running=False, currentSalIndex=0,
                                         salIndices=[], pastSalIndices=[1002, 1001])

            # requeue a script from the history queue
            requeue_data = self.remote.cmd_requeue.DataType()
            requeue_data.salIndex = 1001
            requeue_data.location = SALPY_ScriptQueue.requeue_First
            id_ack = await self.remote.cmd_requeue.start(requeue_data, timeout=10)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            await self.assert_next_queue(running=False, currentSalIndex=0,
                                         salIndices=[1009], pastSalIndices=[1002, 1001])

            # make sure all scripts are runnable before starting the queue
            # so the queue data is more predictable (otherwise the queue
            # may start up with no script running)
            await self.wait_runnable(1009)

            # run the queue and let it finish
            id_ack = await self.remote.cmd_resume.start(self.remote.cmd_resume.DataType(), timeout=10)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            await self.assert_next_queue(running=True, currentSalIndex=1009,
                                         salIndices=[], pastSalIndices=[1002, 1001])
            await self.assert_next_queue(running=True, currentSalIndex=0,
                                         salIndices=[], pastSalIndices=[1009, 1002, 1001])

        asyncio.get_event_loop().run_until_complete(doit())

    def test_showAvailableScripts(self):
        async def doit():
            # make sure showAvailableScripts fails when not enabled
            id_ack = await self.remote.cmd_showAvailableScripts.start(
                self.remote.cmd_showAvailableScripts.DataType(), timeout=10)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_FAILED)

            id_ack = await self.remote.cmd_enable.start(self.remote.cmd_enable.DataType(), timeout=2)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)

            # this should output available scripts without sending the command
            available_scripts0 = await self.remote.evt_availableScripts.next(flush=False, timeout=2)
            with self.assertRaises(asyncio.TimeoutError):
                await self.remote.evt_availableScripts.next(flush=False, timeout=0.1)

            id_ack = await self.remote.cmd_showAvailableScripts.start(
                self.remote.cmd_showAvailableScripts.DataType(), timeout=10)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)

            available_scripts1 = await self.remote.evt_availableScripts.next(flush=False, timeout=2)

            expected_std_set = set(["script1", "script2", "subdir/script3", "subdir/subsubdir/script4"])
            expected_ext_set = set(["script1", "script5", "subdir/script3", "subdir/script6"])
            for available_scripts in (available_scripts0, available_scripts1):
                standard_set = set(available_scripts.standard.split(":"))
                external_set = set(available_scripts.external.split(":"))
                self.assertEqual(standard_set, expected_std_set)
                self.assertEqual(external_set, expected_ext_set)

            # disable and again make sure showAvailableScripts fails
            id_ack = await self.remote.cmd_disable.start(self.remote.cmd_disable.DataType(), timeout=2)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)

            id_ack = await self.remote.cmd_showAvailableScripts.start(
                self.remote.cmd_showAvailableScripts.DataType(), timeout=10)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_FAILED)

        asyncio.get_event_loop().run_until_complete(doit())

    def test_showQueue(self):
        async def doit():
            await self.assert_next_queue(enabled=False, running=True)

            # make sure showQueue fails when not enabled
            id_ack = await self.remote.cmd_showQueue.start(
                self.remote.cmd_showQueue.DataType(), timeout=10)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_FAILED)

            id_ack = await self.remote.cmd_enable.start(self.remote.cmd_enable.DataType(), timeout=2)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            await self.assert_next_queue(enabled=True, running=True)

            # make sure we have no more queue events
            with self.assertRaises(asyncio.TimeoutError):
                await self.remote.evt_queue.next(flush=False, timeout=0.1)

            id_ack = await self.remote.cmd_showQueue.start(self.remote.cmd_showQueue.DataType(), timeout=2)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            await self.assert_next_queue(enabled=True, running=True)

            # make sure disabling the queue outputs the queue event with runnable False
            # and disables the showQueue command.
            id_ack = await self.remote.cmd_disable.start(self.remote.cmd_disable.DataType(), timeout=2)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_COMPLETE)
            await self.assert_next_queue(enabled=False, running=True)
            with self.assertRaises(asyncio.TimeoutError):
                await self.remote.evt_queue.next(flush=False, timeout=0.1)
            id_ack = await self.remote.cmd_showQueue.start(self.remote.cmd_showQueue.DataType(), timeout=2)
            self.assertEqual(id_ack.ack.ack, self.remote.salinfo.lib.SAL__CMD_FAILED)

        asyncio.get_event_loop().run_until_complete(doit())

    async def wait_runnable(self, *sal_indices):
        """Wait for the specified scripts to be runnable.

        Call this before running the queue if you want the queue data
        to be predictable; otherwise the queue may start up with
        no script running.
        """
        for sal_index in sal_indices:
            print(f"waiting for script {sal_index} to be runnable")
            script_info = self.queue.model.get_script_info(sal_index, search_history=False)
            await asyncio.wait_for(script_info.start_task, 20)
            await asyncio.wait_for(script_info.config_task, 20)
            # this will fail if the script was already run
            self.assertTrue(script_info.runnable)


if __name__ == "__main__":
    unittest.main()
