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

import SALPY_Script
import SALPY_ScriptQueue
import salobj
from ts import scriptqueue

I0 = scriptqueue.script_queue.SCRIPT_INDEX_MULT  # initial Script SAL index


class ScriptQueueTestCase(unittest.TestCase):
    def setUp(self):
        salobj.test_utils.set_random_lsst_dds_domain()
        self.datadir = os.path.abspath(os.path.join(os.path.dirname(__file__), "data"))
        standardpath = os.path.join(self.datadir, "standard")
        externalpath = os.path.join(self.datadir, "external")
        self.queue = scriptqueue.ScriptQueue(index=1,
                                             standardpath=standardpath,
                                             externalpath=externalpath)
        self.queue.summary_state = salobj.State.DISABLED
        self.remote = salobj.Remote(SALPY_ScriptQueue, index=1)
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
        # print(f"assert_next_queue(enabled={enabled}, running={running}, "
        #       f"currentSalIndex={currentSalIndex}, salIndices={salIndices}, "
        #       f"pastSalIndices={pastSalIndices}")
        queue_data = await self.remote.evt_queue.next(flush=False, timeout=60)
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
        self.assertEqual(list(queue_data.salIndices[0:queue_data.length]), list(salIndices))
        self.assertEqual(list(queue_data.pastSalIndices[0:queue_data.pastLength]), list(pastSalIndices))

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
            with self.assertRaises(salobj.AckError):
                await self.remote.cmd_add.start(add_data, timeout=2)

            await self.remote.cmd_enable.start(self.remote.cmd_enable.DataType(), timeout=2)
            await self.assert_next_queue(enabled=True, running=True)

            # pause the queue so we know what to expect of queue state
            await self.remote.cmd_pause.start(self.remote.cmd_pause.DataType(), timeout=60)
            await self.assert_next_queue(running=False)

            # add script I0; queue is empty, so location is irrelevant
            add_data = make_add_data(location=SALPY_ScriptQueue.add_Last)
            id_ack = await self.remote.cmd_add.start(add_data, timeout=60)
            cmd_id_0 = id_ack.cmd_id
            await self.assert_next_queue(salIndices=[I0])

            # run showScript for a script that has not been configured
            showScript_data = self.remote.cmd_showScript.DataType()
            showScript_data.salIndex = I0
            id_ack = await self.remote.cmd_showScript.start(showScript_data, timeout=2)
            script_data = self.remote.evt_script.get()
            self.assertEqual(script_data.cmdId, cmd_id_0)
            self.assertEqual(script_data.salIndex, I0)
            self.assertEqual(script_data.isStandard, is_standard)
            self.assertEqual(script_data.path, path)
            self.assertEqual(script_data.processState, SALPY_ScriptQueue.script_Loading)
            self.assertGreater(script_data.timestamp, 0)
            self.assertEqual(script_data.duration, 0)

            # add script I0+1 last: test add last
            add_data = make_add_data(location=SALPY_ScriptQueue.add_Last)
            id_ack = await self.remote.cmd_add.start(add_data, timeout=60)
            cmd_id1 = id_ack.cmd_id
            await self.assert_next_queue(salIndices=[I0, I0+1])

            # add script I0+2 first: test add first
            add_data = make_add_data(location=SALPY_ScriptQueue.add_First)
            id_ack = await self.remote.cmd_add.start(add_data, timeout=60)
            cmd_id2 = id_ack.cmd_id
            await self.assert_next_queue(salIndices=[I0+2, I0, I0+1])

            # add script I0+3 after I0+1: test add after last
            add_data = make_add_data(location=SALPY_ScriptQueue.add_After,
                                     locationSalIndex=I0+1)
            id_ack = await self.remote.cmd_add.start(add_data, timeout=60)
            cmd_id3 = id_ack.cmd_id
            await self.assert_next_queue(salIndices=[I0+2, I0, I0+1, I0+3])

            # add script I0+4 after I0+2: test add after not-last
            add_data = make_add_data(location=SALPY_ScriptQueue.add_After,
                                     locationSalIndex=I0+2)
            await self.remote.cmd_add.start(add_data, timeout=60)
            await self.assert_next_queue(salIndices=[I0+2, I0+4, I0, I0+1, I0+3])

            # add script I0+5 before I0+2: test add before first
            add_data = make_add_data(location=SALPY_ScriptQueue.add_Before,
                                     locationSalIndex=I0+2)
            await self.remote.cmd_add.start(add_data, timeout=60)
            await self.assert_next_queue(salIndices=[I0+5, I0+2, I0+4, I0, I0+1, I0+3])

            # add script I0+6 before I0: test add before not first
            add_data = make_add_data(location=SALPY_ScriptQueue.add_Before,
                                     locationSalIndex=I0)
            await self.remote.cmd_add.start(add_data, timeout=60)
            await self.assert_next_queue(salIndices=[I0+5, I0+2, I0+4, I0+6, I0, I0+1, I0+3])

            # try some failed adds
            # incorrect path
            add_data = make_add_data(location=SALPY_ScriptQueue.add_First)
            add_data.path = "bogus_script_name"
            with self.assertRaises(salobj.AckError):
                await self.remote.cmd_add.start(add_data, timeout=60)

            # incorrect location
            add_data = make_add_data(location=25)
            with self.assertRaises(salobj.AckError):
                await self.remote.cmd_add.start(add_data, timeout=60)

            # incorrect locationSalIndex
            add_data = make_add_data(location=SALPY_ScriptQueue.add_After,
                                     locationSalIndex=4321)
            with self.assertRaises(salobj.AckError):
                await self.remote.cmd_add.start(add_data, timeout=60)

            # make sure the incorrect add commands did not alter the queue
            await self.remote.cmd_showQueue.start(self.remote.cmd_showQueue.DataType(), timeout=2)
            await self.assert_next_queue(salIndices=[I0+5, I0+2, I0+4, I0+6, I0, I0+1, I0+3])

            # stop a few scripts
            stop_data = self.remote.cmd_stop.DataType()
            stop_data.salIndex = I0+6
            await self.remote.cmd_stop.start(stop_data, timeout=60)
            await self.assert_next_queue(salIndices=[I0+5, I0+2, I0+4, I0, I0+1, I0+3])

            stop_data = self.remote.cmd_stop.DataType()
            stop_data.salIndex = I0+5
            await self.remote.cmd_stop.start(stop_data, timeout=60)
            await self.assert_next_queue(salIndices=[I0+2, I0+4, I0, I0+1, I0+3])

            stop_data = self.remote.cmd_stop.DataType()
            stop_data.salIndex = I0
            await self.remote.cmd_stop.start(stop_data, timeout=60)
            await self.assert_next_queue(salIndices=[I0+2, I0+4, I0+1, I0+3])

            stop_data = self.remote.cmd_stop.DataType()
            stop_data.salIndex = I0+4
            await self.remote.cmd_stop.start(stop_data, timeout=60)
            await self.assert_next_queue(salIndices=[I0+2, I0+1, I0+3])

            # try to stop a non-existent script
            stop_data = self.remote.cmd_stop.DataType()
            stop_data.salIndex = 5432
            with self.assertRaises(salobj.AckError):
                await self.remote.cmd_stop.start(stop_data, timeout=60)

            # make sure the failed stop did not affect the queue
            await self.remote.cmd_showQueue.start(self.remote.cmd_showQueue.DataType(), timeout=2)
            await self.assert_next_queue(salIndices=[I0+2, I0+1, I0+3])

            # make sure all scripts are runnable before starting the queue
            # so the queue data is more predictable (otherwise the queue
            # may start up with no script running)
            await self.wait_runnable(I0+1, I0+2, I0+3)

            # get script state for a script that has been configured but is not running
            showScript_data = self.remote.cmd_showScript.DataType()
            showScript_data.salIndex = I0+3
            await self.remote.cmd_showScript.start(showScript_data, timeout=2)
            script_data = self.remote.evt_script.get()
            self.assertEqual(script_data.salIndex, I0+3)
            self.assertEqual(script_data.cmdId, cmd_id3)
            self.assertEqual(script_data.isStandard, is_standard)
            self.assertEqual(script_data.path, path)
            self.assertEqual(script_data.processState, SALPY_ScriptQueue.script_Configured)
            self.assertGreater(script_data.timestamp, 0)
            self.assertEqual(script_data.duration, 0)

            await self.remote.cmd_resume.start(self.remote.cmd_resume.DataType(), timeout=60)
            await self.assert_next_queue(running=True, currentSalIndex=I0+2,
                                         salIndices=[I0+1, I0+3], pastSalIndices=[])

            # get script state for the script that is running
            showScript_data = self.remote.cmd_showScript.DataType()
            showScript_data.salIndex = I0+2
            await self.remote.cmd_showScript.start(showScript_data, timeout=2)
            script_data = self.remote.evt_script.get()
            self.assertEqual(script_data.salIndex, I0+2)
            self.assertEqual(script_data.cmdId, cmd_id2)
            self.assertEqual(script_data.isStandard, is_standard)
            self.assertEqual(script_data.path, path)
            self.assertEqual(script_data.processState, SALPY_ScriptQueue.script_Running)
            self.assertGreater(script_data.timestamp, 0)
            self.assertEqual(script_data.duration, 0)

            await self.assert_next_queue(running=True, currentSalIndex=I0+1,
                                         salIndices=[I0+3], pastSalIndices=[I0+2])
            await self.assert_next_queue(running=True, currentSalIndex=I0+3,
                                         salIndices=[], pastSalIndices=[I0+1, I0+2])
            await self.assert_next_queue(running=True, currentSalIndex=0,
                                         salIndices=[], pastSalIndices=[I0+3, I0+1, I0+2])

            # get script state for a script that has been run
            showScript_data = self.remote.cmd_showScript.DataType()
            showScript_data.salIndex = I0+1
            await self.remote.cmd_showScript.start(showScript_data, timeout=2)
            script_data = self.remote.evt_script.get()
            self.assertEqual(script_data.salIndex, I0+1)
            self.assertEqual(script_data.cmdId, cmd_id1)
            self.assertEqual(script_data.isStandard, is_standard)
            self.assertEqual(script_data.path, path)
            self.assertEqual(script_data.processState, SALPY_ScriptQueue.script_Done)
            self.assertGreater(script_data.timestamp, 0)
            self.assertGreater(script_data.duration, 0.9)  # wait time is 1

            # get script state for non-existent script
            showScript_data = self.remote.cmd_showScript.DataType()
            showScript_data.salIndex = 3579
            with self.assertRaises(salobj.AckError):
                await self.remote.cmd_showScript.start(showScript_data, timeout=2)

        asyncio.get_event_loop().run_until_complete(doit())

    def test_processState(self):
        """Test the processState value of the queue event.
        """
        is_standard = True
        path = "script1"

        async def doit():
            await self.assert_next_queue(enabled=False, running=True)

            await self.remote.cmd_enable.start(self.remote.cmd_enable.DataType(), timeout=2)
            await self.assert_next_queue(enabled=True, running=True)

            def make_add_data(config):
                add_data = self.remote.cmd_add.DataType()
                add_data.isStandard = is_standard
                add_data.path = path
                add_data.config = config
                add_data.location = SALPY_ScriptQueue.add_Last
                add_data.locationSalIndex = 0
                add_data.descr = "test_processState"
                return add_data

            # pause the queue so we know what to expect of queue state
            await self.remote.cmd_pause.start(self.remote.cmd_pause.DataType(), timeout=60)
            await self.assert_next_queue(running=False)

            # add script I0 that will fail, and so pause the queue
            add_data = make_add_data(config="fail_run: True")
            id_ack = await self.remote.cmd_add.start(add_data, timeout=60)
            cmd_id0 = id_ack.cmd_id
            await self.assert_next_queue(salIndices=[I0])

            # add script I0+1 that we will terminate
            add_data = make_add_data(config="")
            id_ack = await self.remote.cmd_add.start(add_data, timeout=60)
            cmd_id1 = id_ack.cmd_id
            await self.assert_next_queue(salIndices=[I0, I0+1])

            # add script I0+2 that we will allow to run normally
            add_data = make_add_data(config="")
            id_ack = await self.remote.cmd_add.start(add_data, timeout=60)
            cmd_id2 = id_ack.cmd_id
            await self.assert_next_queue(salIndices=[I0, I0+1, I0+2])

            # wait for both scripts to be runnable so future script output
            # is due to the scripts being run or terminated
            await self.wait_runnable(I0, I0+1, I0+2)

            # run the queue and let it pause on failure
            await self.remote.cmd_resume.start(self.remote.cmd_resume.DataType(), timeout=60)
            await self.assert_next_queue(running=True, currentSalIndex=I0,
                                         salIndices=[I0+1, I0+2], pastSalIndices=[])
            await self.assert_next_queue(running=False, currentSalIndex=I0,
                                         salIndices=[I0+1, I0+2], pastSalIndices=[])

            script_data0 = self.remote.evt_script.get()
            self.assertEqual(script_data0.cmdId, cmd_id0)
            self.assertEqual(script_data0.salIndex, I0)
            self.assertEqual(script_data0.processState, SALPY_ScriptQueue.script_Done)
            self.assertEqual(script_data0.scriptState, SALPY_Script.state_Failed)

            # terminate the next script
            terminate_data = self.remote.cmd_terminate.DataType()
            terminate_data.salIndex = I0+1
            await self.remote.cmd_terminate.start(terminate_data, timeout=2)
            await self.assert_next_queue(running=False, currentSalIndex=I0,
                                         salIndices=[I0+2], pastSalIndices=[])

            script_data1 = self.remote.evt_script.get()
            self.assertEqual(script_data1.cmdId, cmd_id1)
            self.assertEqual(script_data1.salIndex, I0+1)
            self.assertEqual(script_data1.processState, SALPY_ScriptQueue.script_Terminated)
            self.assertEqual(script_data1.scriptState, SALPY_Script.state_Configured)

            # resume the queue and let I0+2 run
            await self.remote.cmd_resume.start(self.remote.cmd_resume.DataType(), timeout=60)
            await self.assert_next_queue(running=True, currentSalIndex=I0+2,
                                         salIndices=[], pastSalIndices=[I0])
            await self.assert_next_queue(running=True, currentSalIndex=0,
                                         salIndices=[], pastSalIndices=[I0+2, I0])

            script_data2 = self.remote.evt_script.get()
            self.assertEqual(script_data2.cmdId, cmd_id2)
            self.assertEqual(script_data2.salIndex, I0+2)
            self.assertEqual(script_data2.processState, SALPY_ScriptQueue.script_Done)
            self.assertEqual(script_data2.scriptState, SALPY_Script.state_Done)

        asyncio.get_event_loop().run_until_complete(doit())

    def test_move(self):
        """Test move, pause and showQueue
        """
        async def doit():
            await self.assert_next_queue(enabled=False, running=True)

            # pause the queue so we know what to expect of queue state
            # also check that pause works while not enabled
            await self.remote.cmd_pause.start(self.remote.cmd_pause.DataType(), timeout=60)
            await self.assert_next_queue(enabled=False, running=False)

            await self.remote.cmd_enable.start(self.remote.cmd_enable.DataType(), timeout=2)
            await self.assert_next_queue(enabled=True, running=False)

            # queue scripts I0, I0+1 and I0+2
            sal_indices = [I0, I0+1, I0+2]
            for i, index in enumerate(sal_indices):
                add_data = self.remote.cmd_add.DataType()
                add_data.isStandard = True
                add_data.path = os.path.join("subdir", "script3")
                add_data.config = "wait_time: 0.1"
                add_data.location = SALPY_ScriptQueue.add_Last
                add_data.descr = f"test_move {i}"
                await self.remote.cmd_add.start(add_data, timeout=60)
                await self.assert_next_queue(salIndices=sal_indices[0:i+1])

            # move I0+2 first
            move_data = self.remote.cmd_move.DataType()
            move_data.salIndex = I0+2
            move_data.location = SALPY_ScriptQueue.add_First
            await self.remote.cmd_move.start(move_data, timeout=2)
            await self.assert_next_queue(salIndices=[I0+2, I0, I0+1])

            # move I0+2 first again (should be a no-op)
            move_data = self.remote.cmd_move.DataType()
            move_data.salIndex = I0+2
            move_data.location = SALPY_ScriptQueue.add_First
            await self.remote.cmd_move.start(move_data, timeout=2)
            await self.assert_next_queue(salIndices=[I0+2, I0, I0+1])

            # move I0 last
            move_data = self.remote.cmd_move.DataType()
            move_data.salIndex = I0
            move_data.location = SALPY_ScriptQueue.add_Last
            await self.remote.cmd_move.start(move_data, timeout=2)
            await self.assert_next_queue(salIndices=[I0+2, I0+1, I0])

            # move I0 last again (should be a no-op)
            move_data = self.remote.cmd_move.DataType()
            move_data.salIndex = I0
            move_data.location = SALPY_ScriptQueue.add_Last
            await self.remote.cmd_move.start(move_data, timeout=2)
            await self.assert_next_queue(salIndices=[I0+2, I0+1, I0])

            # move I0 before I0+2: before first
            move_data.salIndex = I0
            move_data.location = SALPY_ScriptQueue.add_Before
            move_data.locationSalIndex = I0+2
            await self.remote.cmd_move.start(move_data, timeout=2)
            await self.assert_next_queue(salIndices=[I0, I0+2, I0+1])

            # move I0+1 before I0+2: before not-first
            move_data.salIndex = I0+1
            move_data.location = SALPY_ScriptQueue.add_Before
            move_data.locationSalIndex = I0+2
            await self.remote.cmd_move.start(move_data, timeout=2)
            await self.assert_next_queue(salIndices=[I0, I0+1, I0+2])

            # move I0 after I0+2: after last
            move_data.salIndex = I0
            move_data.location = SALPY_ScriptQueue.add_After
            move_data.locationSalIndex = I0+2
            await self.remote.cmd_move.start(move_data, timeout=2)
            await self.assert_next_queue(salIndices=[I0+1, I0+2, I0])

            # move I0+1 after I0+2: after not-last
            move_data.salIndex = I0+1
            move_data.location = SALPY_ScriptQueue.add_After
            move_data.locationSalIndex = I0+2
            await self.remote.cmd_move.start(move_data, timeout=2)
            await self.assert_next_queue(salIndices=[I0+2, I0+1, I0])

            # move I0 after itself: this should be a no-op
            # but it should still output the queue event
            move_data.salIndex = I0
            move_data.location = SALPY_ScriptQueue.add_After
            move_data.locationSalIndex = I0
            await self.remote.cmd_move.start(move_data, timeout=2)
            await self.assert_next_queue(salIndices=[I0+2, I0+1, I0])

            # move I0+1 before itself: this should be a no-op
            # but it should still output the queue event
            move_data.salIndex = I0+1
            move_data.location = SALPY_ScriptQueue.add_After
            move_data.locationSalIndex = I0+1
            await self.remote.cmd_move.start(move_data, timeout=2)
            await self.assert_next_queue(salIndices=[I0+2, I0+1, I0])

            # try some incorrect moves
            move_data.salIndex = 1234  # no such script
            move_data.location = SALPY_ScriptQueue.add_Last
            with self.assertRaises(salobj.AckError):
                await self.remote.cmd_move.start(move_data, timeout=2)

            move_data.salIndex = I0+1
            move_data.location = 21  # no such location
            with self.assertRaises(salobj.AckError):
                await self.remote.cmd_move.start(move_data, timeout=2)

            move_data.salIndex = I0+1
            move_data.location = SALPY_ScriptQueue.add_Before
            move_data.locationSalIndex = 1234  # no such script
            with self.assertRaises(salobj.AckError):
                await self.remote.cmd_move.start(move_data, timeout=2)

            # try incorrect index and the same "before" locationSalIndex
            move_data.salIndex = 1234
            move_data.location = SALPY_ScriptQueue.add_Before
            move_data.locationSalIndex = 1234
            with self.assertRaises(salobj.AckError):
                await self.remote.cmd_move.start(move_data, timeout=2)

            # try incorrect index and the same "after" locationSalIndex
            move_data.salIndex = 1234
            move_data.location = SALPY_ScriptQueue.add_After
            move_data.locationSalIndex = 1234
            with self.assertRaises(salobj.AckError):
                await self.remote.cmd_move.start(move_data, timeout=2)

            # make sure those commands did not alter the queue
            await self.remote.cmd_showQueue.start(self.remote.cmd_showQueue.DataType(), timeout=2)
            await self.assert_next_queue(salIndices=[I0+2, I0+1, I0])

            await self.queue.model.wait_terminate_all(timeout=10)
            for i in range(len(sal_indices)):
                queue_data = await self.remote.evt_queue.next(flush=False, timeout=60)
            self.assertEqual(queue_data.length, 0)

        asyncio.get_event_loop().run_until_complete(doit())

    def test_requeue(self):
        """Test requeue, move and terminate
        """
        async def doit():
            await self.assert_next_queue(enabled=False, running=True)

            await self.remote.cmd_enable.start(self.remote.cmd_enable.DataType(), timeout=2)
            await self.assert_next_queue(enabled=True, running=True)

            # pause the queue so we know what to expect of queue state
            await self.remote.cmd_pause.start(self.remote.cmd_pause.DataType(), timeout=60)
            await self.assert_next_queue(running=False)

            # queue scripts I0, I0+1 and I0+2
            sal_indices = [I0, I0+1, I0+2]
            for i, index in enumerate(sal_indices):
                add_data = self.remote.cmd_add.DataType()
                add_data.isStandard = True
                add_data.path = "script2"
                add_data.config = "wait_time: 0.1"
                add_data.location = SALPY_ScriptQueue.add_Last
                add_data.descr = f"test_requeue {i}"
                await self.remote.cmd_add.start(add_data, timeout=60)
                await self.assert_next_queue(salIndices=sal_indices[0:i+1])

            # disable the queue and make sure requeue, move and resume fail
            # (I added some jobs before disabling so we have scripts
            # to try to requeue and move).
            await self.remote.cmd_disable.start(self.remote.cmd_disable.DataType(), timeout=2)
            await self.assert_next_queue(enabled=False, running=False, salIndices=sal_indices)

            requeue_data = self.remote.cmd_requeue.DataType()
            requeue_data.salIndex = I0
            requeue_data.location = SALPY_ScriptQueue.requeue_Last
            with self.assertRaises(salobj.AckError):
                await self.remote.cmd_requeue.start(requeue_data, timeout=60)

            move_data = self.remote.cmd_move.DataType()
            move_data.salIndex = I0+2
            move_data.location = SALPY_ScriptQueue.add_First
            with self.assertRaises(salobj.AckError):
                await self.remote.cmd_move.start(move_data, timeout=2)

            with self.assertRaises(salobj.AckError):
                await self.remote.cmd_resume.start(self.remote.cmd_resume.DataType(), timeout=2)

            # re-enable the queue and proceed with the rest of the test
            await self.remote.cmd_enable.start(self.remote.cmd_enable.DataType(), timeout=2)
            await self.assert_next_queue(enabled=True, running=False, salIndices=sal_indices)

            # requeue I0 to last
            requeue_data = self.remote.cmd_requeue.DataType()
            requeue_data.salIndex = I0
            requeue_data.location = SALPY_ScriptQueue.requeue_Last
            await self.remote.cmd_requeue.start(requeue_data, timeout=60)
            await self.assert_next_queue(salIndices=[I0, I0+1, I0+2, I0+3])

            # requeue I0+1 to first
            requeue_data = self.remote.cmd_requeue.DataType()
            requeue_data.salIndex = I0
            requeue_data.location = SALPY_ScriptQueue.requeue_First
            await self.remote.cmd_requeue.start(requeue_data, timeout=60)
            await self.assert_next_queue(salIndices=[I0+4, I0, I0+1, I0+2, I0+3])

            # requeue I0+2 to before I0+4: before first
            requeue_data = self.remote.cmd_requeue.DataType()
            requeue_data.salIndex = I0+2
            requeue_data.location = SALPY_ScriptQueue.requeue_Before
            requeue_data.locationSalIndex = I0+4
            await self.remote.cmd_requeue.start(requeue_data, timeout=60)
            await self.assert_next_queue(salIndices=[I0+5, I0+4, I0, I0+1, I0+2, I0+3])

            # requeue I0+3 to before I0+3: before not-first
            # and check that matching indices is not a problem
            requeue_data = self.remote.cmd_requeue.DataType()
            requeue_data.salIndex = I0+3
            requeue_data.location = SALPY_ScriptQueue.requeue_Before
            requeue_data.locationSalIndex = I0+3
            await self.remote.cmd_requeue.start(requeue_data, timeout=60)
            await self.assert_next_queue(salIndices=[I0+5, I0+4, I0, I0+1, I0+2, I0+6, I0+3])

            # requeue I0+3 to after I0+3: after last
            # and check that matching indices is not a problem
            requeue_data = self.remote.cmd_requeue.DataType()
            requeue_data.salIndex = I0+3
            requeue_data.location = SALPY_ScriptQueue.requeue_After
            requeue_data.locationSalIndex = I0+3
            await self.remote.cmd_requeue.start(requeue_data, timeout=60)
            await self.assert_next_queue(salIndices=[I0+5, I0+4, I0, I0+1, I0+2, I0+6, I0+3, I0+7])

            # requeue I0+5 to after I0+7: after last
            requeue_data = self.remote.cmd_requeue.DataType()
            requeue_data.salIndex = I0+5
            requeue_data.location = SALPY_ScriptQueue.requeue_Last
            requeue_data.locationSalIndex = I0+7
            await self.remote.cmd_requeue.start(requeue_data, timeout=60)
            await self.assert_next_queue(salIndices=[I0+5, I0+4, I0+0, I0+1, I0+2, I0+6, I0+3, I0+7, I0+8])

            # stop all scripts except I0+1 and I0+2
            sal_indices = [I0+5, I0+4, I0, I0+1, I0+2, I0+6, I0+3, I0+7, I0+8]
            for remove_index in sal_indices[:]:
                if remove_index in (I0+1, I0+2):
                    continue
                stop_data = self.remote.cmd_stop.DataType()
                stop_data.salIndex = remove_index
                await self.remote.cmd_stop.start(stop_data, timeout=60)

                sal_indices.remove(remove_index)
                await self.assert_next_queue(salIndices=sal_indices)

            await self.wait_runnable(I0+1, I0+2)

            # run the queue and let it finish
            await self.remote.cmd_resume.start(self.remote.cmd_resume.DataType(), timeout=60)
            await self.assert_next_queue(running=True, currentSalIndex=I0+1,
                                         salIndices=[I0+2], pastSalIndices=[])
            await self.assert_next_queue(running=True, currentSalIndex=I0+2,
                                         salIndices=[], pastSalIndices=[I0+1])
            await self.assert_next_queue(running=True, currentSalIndex=0,
                                         salIndices=[], pastSalIndices=[I0+2, I0+1])

            # pause while we requeue from history
            await self.remote.cmd_pause.start(self.remote.cmd_pause.DataType(), timeout=60)
            await self.assert_next_queue(running=False, currentSalIndex=0,
                                         salIndices=[], pastSalIndices=[I0+2, I0+1])

            # requeue a script from the history queue
            requeue_data = self.remote.cmd_requeue.DataType()
            requeue_data.salIndex = I0+1
            requeue_data.location = SALPY_ScriptQueue.requeue_First
            await self.remote.cmd_requeue.start(requeue_data, timeout=60)
            await self.assert_next_queue(running=False, currentSalIndex=0,
                                         salIndices=[I0+9], pastSalIndices=[I0+2, I0+1])

            # make sure all scripts are runnable before starting the queue
            # so the queue data is more predictable (otherwise the queue
            # may start up with no script running)
            await self.wait_runnable(I0+9)

            # run the queue and let it finish
            await self.remote.cmd_resume.start(self.remote.cmd_resume.DataType(), timeout=60)
            await self.assert_next_queue(running=True, currentSalIndex=I0+9,
                                         salIndices=[], pastSalIndices=[I0+2, I0+1])
            await self.assert_next_queue(running=True, currentSalIndex=0,
                                         salIndices=[], pastSalIndices=[I0+9, I0+2, I0+1])

        asyncio.get_event_loop().run_until_complete(doit())

    def test_showAvailableScripts(self):
        async def doit():
            # make sure showAvailableScripts fails when not enabled
            with self.assertRaises(salobj.AckError):
                await self.remote.cmd_showAvailableScripts.start(
                    self.remote.cmd_showAvailableScripts.DataType(), timeout=60)

            await self.remote.cmd_enable.start(self.remote.cmd_enable.DataType(), timeout=2)

            # this should output available scripts without sending the command
            available_scripts0 = await self.remote.evt_availableScripts.next(flush=False, timeout=2)
            with self.assertRaises(asyncio.TimeoutError):
                await self.remote.evt_availableScripts.next(flush=False, timeout=0.1)

            await self.remote.cmd_showAvailableScripts.start(
                self.remote.cmd_showAvailableScripts.DataType(), timeout=60)

            available_scripts1 = await self.remote.evt_availableScripts.next(flush=False, timeout=2)

            expected_std_set = set(["script1", "script2", "subdir/script3", "subdir/subsubdir/script4"])
            expected_ext_set = set(["script1", "script5", "subdir/script3", "subdir/script6"])
            for available_scripts in (available_scripts0, available_scripts1):
                standard_set = set(available_scripts.standard.split(":"))
                external_set = set(available_scripts.external.split(":"))
                self.assertEqual(standard_set, expected_std_set)
                self.assertEqual(external_set, expected_ext_set)

            # disable and again make sure showAvailableScripts fails
            await self.remote.cmd_disable.start(self.remote.cmd_disable.DataType(), timeout=2)

            with self.assertRaises(salobj.AckError):
                await self.remote.cmd_showAvailableScripts.start(
                    self.remote.cmd_showAvailableScripts.DataType(), timeout=2)

        asyncio.get_event_loop().run_until_complete(doit())

    def test_showQueue(self):
        async def doit():
            await self.assert_next_queue(enabled=False, running=True)

            # make sure showQueue fails when not enabled
            with self.assertRaises(salobj.AckError):
                await self.remote.cmd_showQueue.start(
                    self.remote.cmd_showQueue.DataType(), timeout=2)

            await self.remote.cmd_enable.start(self.remote.cmd_enable.DataType(), timeout=2)
            await self.assert_next_queue(enabled=True, running=True)

            # make sure we have no more queue events
            with self.assertRaises(asyncio.TimeoutError):
                await self.remote.evt_queue.next(flush=False, timeout=0.1)

            await self.remote.cmd_showQueue.start(self.remote.cmd_showQueue.DataType(), timeout=2)
            await self.assert_next_queue(enabled=True, running=True)

            # make sure disabling the queue outputs the queue event with runnable False
            # and disables the showQueue command.
            await self.remote.cmd_disable.start(self.remote.cmd_disable.DataType(), timeout=2)
            await self.assert_next_queue(enabled=False, running=True)
            with self.assertRaises(asyncio.TimeoutError):
                await self.remote.evt_queue.next(flush=False, timeout=0.1)
            with self.assertRaises(salobj.AckError):
                await self.remote.cmd_showQueue.start(self.remote.cmd_showQueue.DataType(), timeout=2)

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
            await asyncio.wait_for(script_info.start_task, 60)
            await asyncio.wait_for(script_info.config_task, 60)
            # this will fail if the script was already run
            self.assertTrue(script_info.runnable)


if __name__ == "__main__":
    unittest.main()
