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
import shutil
import unittest
import warnings
import yaml

from lsst.ts import salobj
from lsst.ts.idl.enums.ScriptQueue import Location, ScriptProcessState
from lsst.ts.idl.enums.Script import ScriptState
from lsst.ts import scriptqueue

STD_TIMEOUT = 10
START_TIMEOUT = 20
END_TIMEOUT = 10

I0 = scriptqueue.script_queue.SCRIPT_INDEX_MULT  # initial Script SAL index


class ScriptQueueConstructorTestCase(unittest.TestCase):
    def setUp(self):
        salobj.set_random_lsst_dds_domain()
        self.default_standardpath = scriptqueue.get_default_scripts_dir(is_standard=True)
        self.default_externalpath = scriptqueue.get_default_scripts_dir(is_standard=False)
        self.datadir = os.path.abspath(os.path.join(os.path.dirname(__file__), "data"))
        self.testdata_standardpath = os.path.join(self.datadir, "standard")
        self.testdata_externalpath = os.path.join(self.datadir, "external")
        self.badpath = os.path.join(self.datadir, "not_a_directory")

    def test_default_paths(self):
        async def doit():
            async with salobj.Domain() as domain:
                remote = salobj.Remote(domain, "ScriptQueue", index=1,
                                       evt_max_history=0, tel_max_history=0)
                await asyncio.wait_for(remote.start_task, timeout=STD_TIMEOUT)
                async with scriptqueue.ScriptQueue(index=1) as queue:
                    self.assertTrue(os.path.samefile(queue.model.standardpath, self.default_standardpath))
                    self.assertTrue(os.path.samefile(queue.model.externalpath, self.default_externalpath))
                    rootDir_data = await remote.evt_rootDirectories.next(flush=False, timeout=STD_TIMEOUT)
                    self.assertTrue(os.path.samefile(rootDir_data.standard, self.default_standardpath))
                    self.assertTrue(os.path.samefile(rootDir_data.external, self.default_externalpath))

                    # some tests rely on these being different, so verify that
                    self.assertNotEqual(self.testdata_standardpath, self.default_standardpath)
                    self.assertNotEqual(self.testdata_externalpath, self.default_externalpath)

        asyncio.get_event_loop().run_until_complete(doit())

    def test_explicit_paths(self):
        async def doit():
            async with salobj.Domain() as domain:
                remote = salobj.Remote(domain, "ScriptQueue", index=1,
                                       evt_max_history=0, tel_max_history=0)
                await asyncio.wait_for(remote.start_task, timeout=STD_TIMEOUT)
                async with scriptqueue.ScriptQueue(index=1, standardpath=self.testdata_standardpath,
                                                   externalpath=self.testdata_externalpath) as queue:
                    self.assertTrue(os.path.samefile(queue.model.standardpath, self.testdata_standardpath))
                    self.assertTrue(os.path.samefile(queue.model.externalpath, self.testdata_externalpath))
                    rootDir_data = await remote.evt_rootDirectories.next(flush=False, timeout=STD_TIMEOUT)
                    self.assertTrue(os.path.samefile(rootDir_data.standard, self.testdata_standardpath))
                    self.assertTrue(os.path.samefile(rootDir_data.external, self.testdata_externalpath))

        asyncio.get_event_loop().run_until_complete(doit())

    def test_default_standard_path(self):
        async def doit():
            async with salobj.Domain() as domain:
                remote = salobj.Remote(domain, "ScriptQueue", index=1,
                                       evt_max_history=0, tel_max_history=0)
                await asyncio.wait_for(remote.start_task, timeout=STD_TIMEOUT)
                async with scriptqueue.ScriptQueue(index=1, externalpath=self.testdata_externalpath) as queue:
                    self.assertTrue(os.path.samefile(queue.model.standardpath, self.default_standardpath))
                    self.assertTrue(os.path.samefile(queue.model.externalpath, self.testdata_externalpath))
                    rootDir_data = await remote.evt_rootDirectories.next(flush=False, timeout=STD_TIMEOUT)
                    self.assertTrue(os.path.samefile(rootDir_data.standard, self.default_standardpath))
                    self.assertTrue(os.path.samefile(rootDir_data.external, self.testdata_externalpath))

        asyncio.get_event_loop().run_until_complete(doit())

    def test_default_external_path(self):
        async def doit():
            async with salobj.Domain() as domain:
                remote = salobj.Remote(domain, "ScriptQueue", index=1,
                                       evt_max_history=0, tel_max_history=0)
                await asyncio.wait_for(remote.start_task, timeout=STD_TIMEOUT)
                async with scriptqueue.ScriptQueue(index=1, standardpath=self.testdata_standardpath) as queue:
                    self.assertTrue(os.path.samefile(queue.model.standardpath, self.testdata_standardpath))
                    self.assertTrue(os.path.samefile(queue.model.externalpath, self.default_externalpath))
                    rootDir_data = await remote.evt_rootDirectories.next(flush=False, timeout=STD_TIMEOUT)
                    self.assertTrue(os.path.samefile(rootDir_data.standard, self.testdata_standardpath))
                    self.assertTrue(os.path.samefile(rootDir_data.external, self.default_externalpath))

        asyncio.get_event_loop().run_until_complete(doit())

    def test_invalid_paths(self):
        with self.assertRaises(ValueError):
            scriptqueue.ScriptQueue(index=1, standardpath=self.badpath,
                                    externalpath=self.testdata_externalpath)
        with self.assertRaises(ValueError):
            scriptqueue.ScriptQueue(index=1, standardpath=self.testdata_standardpath,
                                    externalpath=self.badpath)
        with self.assertRaises(ValueError):
            scriptqueue.ScriptQueue(index=1, standardpath=self.badpath,
                                    externalpath=self.badpath)


class ScriptQueueTestCase(unittest.TestCase):
    def setUp(self):
        salobj.set_random_lsst_dds_domain()
        self.datadir = os.path.abspath(os.path.join(os.path.dirname(__file__), "data"))
        standardpath = os.path.join(self.datadir, "standard")
        externalpath = os.path.join(self.datadir, "external")
        # make a separate domain so we can create a remote that does not
        # read historical data (for startup speed)
        self.domain = salobj.Domain()
        self.remote = salobj.Remote(self.domain, "ScriptQueue", index=1,
                                    evt_max_history=0, tel_max_history=0)
        self.queue = scriptqueue.ScriptQueue(index=1,
                                             standardpath=standardpath,
                                             externalpath=externalpath,
                                             verbose=True)
        self.queue.summary_state = salobj.State.DISABLED

    async def close(self):
        nkilled = len(self.queue.model.terminate_all())
        if nkilled > 0:
            warnings.warn(f"Killed {nkilled} subprocesses")
        await self.domain.close()
        await self.queue.close()

    def tearDown(self):
        asyncio.get_event_loop().run_until_complete(self.close())

    def make_stop_data(self, stop_indices, terminate):
        """Make data for the stopScripts command.

        Parameters
        ----------
        stop_indices : ``iterable`` of `int`
            SAL indices of scripts to stop
        terminate : `bool`
            Terminate a running script instead of giving it time
            to stop gently?
        """
        stop_data = self.remote.cmd_stopScripts.DataType()
        stop_data.length = len(stop_indices)
        stop_data.salIndices[0:stop_data.length] = stop_indices
        stop_data.length = stop_data.length
        stop_data.terminate = False
        return stop_data

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
        queue_data = await self.remote.evt_queue.next(flush=False, timeout=START_TIMEOUT)
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
            await asyncio.wait_for(self.remote.start_task, timeout=START_TIMEOUT)
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
            add_data = make_add_data(location=Location.LAST)
            with self.assertRaises(salobj.AckError):
                await self.remote.cmd_add.start(add_data, timeout=STD_TIMEOUT)

            await self.remote.cmd_enable.start(timeout=STD_TIMEOUT)
            await self.assert_next_queue(enabled=True, running=True)

            # pause the queue so we know what to expect of queue state
            await self.remote.cmd_pause.start(timeout=START_TIMEOUT)
            await self.assert_next_queue(running=False)

            # add script I0; queue is empty, so location is irrelevant
            add_data = make_add_data(location=Location.LAST)
            ackcmd = await self.remote.cmd_add.start(add_data, timeout=START_TIMEOUT)
            seq_num_0 = ackcmd.private_seqNum
            self.assertEqual(int(ackcmd.result), I0)
            await self.assert_next_queue(salIndices=[I0])

            # run showScript for a script that has not been configured
            self.remote.evt_script.flush()
            ackcmd = await self.remote.cmd_showScript.set_start(salIndex=I0, timeout=STD_TIMEOUT)
            script_data = await self.remote.evt_script.next(flush=False, timeout=2)
            self.assertEqual(script_data.cmdId, seq_num_0)
            self.assertEqual(script_data.salIndex, I0)
            self.assertEqual(script_data.isStandard, is_standard)
            self.assertEqual(script_data.path, path)
            self.assertEqual(script_data.processState, ScriptProcessState.LOADING)
            self.assertGreater(script_data.timestampProcessStart, 0)
            self.assertEqual(script_data.timestampProcessEnd, 0)

            # add script I0+1 last: test add last
            add_data = make_add_data(location=Location.LAST)
            ackcmd = await self.remote.cmd_add.start(add_data, timeout=START_TIMEOUT)
            seq_num1 = ackcmd.private_seqNum
            self.assertEqual(int(ackcmd.result), I0+1)
            await self.assert_next_queue(salIndices=[I0, I0+1])

            # add script I0+2 first: test add first
            add_data = make_add_data(location=Location.FIRST)
            ackcmd = await self.remote.cmd_add.start(add_data, timeout=START_TIMEOUT)
            self.assertEqual(int(ackcmd.result), I0+2)
            await self.assert_next_queue(salIndices=[I0+2, I0, I0+1])

            # add script I0+3 after I0+1: test add after last
            add_data = make_add_data(location=Location.AFTER,
                                     locationSalIndex=I0+1)
            ackcmd = await self.remote.cmd_add.start(add_data, timeout=START_TIMEOUT)
            seq_num3 = ackcmd.private_seqNum
            await self.assert_next_queue(salIndices=[I0+2, I0, I0+1, I0+3])

            # add script I0+4 after I0+2: test add after not-last
            add_data = make_add_data(location=Location.AFTER,
                                     locationSalIndex=I0+2)
            await self.remote.cmd_add.start(add_data, timeout=START_TIMEOUT)
            await self.assert_next_queue(salIndices=[I0+2, I0+4, I0, I0+1, I0+3])

            # add script I0+5 before I0+2: test add before first
            add_data = make_add_data(location=Location.BEFORE,
                                     locationSalIndex=I0+2)
            await self.remote.cmd_add.start(add_data, timeout=START_TIMEOUT)
            await self.assert_next_queue(salIndices=[I0+5, I0+2, I0+4, I0, I0+1, I0+3])

            # add script I0+6 before I0: test add before not first
            add_data = make_add_data(location=Location.BEFORE,
                                     locationSalIndex=I0)
            await self.remote.cmd_add.start(add_data, timeout=START_TIMEOUT)
            await self.assert_next_queue(salIndices=[I0+5, I0+2, I0+4, I0+6, I0, I0+1, I0+3])

            # try some failed adds
            # incorrect path
            add_data = make_add_data(location=Location.FIRST)
            add_data.path = "bogus_script_name"
            with self.assertRaises(salobj.AckError):
                await self.remote.cmd_add.start(add_data, timeout=START_TIMEOUT)

            # incorrect location
            add_data = make_add_data(location=25)
            with self.assertRaises(salobj.AckError):
                await self.remote.cmd_add.start(add_data, timeout=START_TIMEOUT)

            # incorrect locationSalIndex
            add_data = make_add_data(location=Location.AFTER,
                                     locationSalIndex=4321)
            with self.assertRaises(salobj.AckError):
                await self.remote.cmd_add.start(add_data, timeout=START_TIMEOUT)

            # make sure the incorrect add commands did not alter the queue
            await self.remote.cmd_showQueue.start(timeout=STD_TIMEOUT)
            await self.assert_next_queue(salIndices=[I0+5, I0+2, I0+4, I0+6, I0, I0+1, I0+3])

            # stop a few scripts, including one non-existent script
            stop_data = self.make_stop_data([I0+6, I0+5, I0+4, I0, 5432], terminate=False)
            await self.remote.cmd_stopScripts.start(stop_data, timeout=START_TIMEOUT)
            await self.assert_next_queue(salIndices=[I0+2, I0+1, I0+3])

            # make sure all scripts are runnable before starting the queue
            # so the queue data is more predictable (otherwise the queue
            # may start up with no script running)
            await self.wait_runnable(I0+1, I0+2, I0+3)

            # get script state for a script that has been configured
            # but is not running
            self.remote.evt_script.flush()
            await self.remote.cmd_showScript.set_start(salIndex=I0+3, timeout=STD_TIMEOUT)
            while True:
                script_data = await self.remote.evt_script.next(flush=False, timeout=2)
                if script_data.salIndex == I0 + 3:
                    break
            self.assertEqual(script_data.salIndex, I0+3)
            self.assertEqual(script_data.cmdId, seq_num3)
            self.assertEqual(script_data.isStandard, is_standard)
            self.assertEqual(script_data.path, path)
            self.assertEqual(script_data.processState, ScriptProcessState.CONFIGURED)
            self.assertGreater(script_data.timestampProcessStart, 0)
            self.assertEqual(script_data.timestampProcessEnd, 0)

            await self.remote.cmd_resume.start(timeout=START_TIMEOUT)
            await self.assert_next_queue(running=True, currentSalIndex=I0+2,
                                         salIndices=[I0+1, I0+3], pastSalIndices=[])

            await self.assert_next_queue(running=True, currentSalIndex=I0+1,
                                         salIndices=[I0+3], pastSalIndices=[I0+2])
            await self.assert_next_queue(running=True, currentSalIndex=I0+3,
                                         salIndices=[], pastSalIndices=[I0+1, I0+2])
            await self.assert_next_queue(running=True, currentSalIndex=0,
                                         salIndices=[], pastSalIndices=[I0+3, I0+1, I0+2])

            # get script state for a script that has been run
            self.remote.evt_script.flush()
            await self.remote.cmd_showScript.set_start(salIndex=I0+1, timeout=STD_TIMEOUT)
            while True:
                script_data = await self.remote.evt_script.next(flush=False, timeout=2)
                if script_data.salIndex == I0 + 1:
                    break
            self.assertEqual(script_data.salIndex, I0+1)
            self.assertEqual(script_data.cmdId, seq_num1)
            self.assertEqual(script_data.isStandard, is_standard)
            self.assertEqual(script_data.path, path)
            self.assertEqual(script_data.processState, ScriptProcessState.DONE)
            self.assertGreater(script_data.timestampProcessStart, 0)
            self.assertGreater(script_data.timestampProcessEnd, 0)
            process_duration = script_data.timestampProcessEnd - script_data.timestampProcessStart
            self.assertGreater(process_duration, 0.9)  # wait time is 1

            # get script state for non-existent script
            with self.assertRaises(salobj.AckError):
                await self.remote.cmd_showScript.set_start(salIndex=3579, timeout=STD_TIMEOUT)

        asyncio.get_event_loop().run_until_complete(doit())

    def test_processState(self):
        """Test the processState value of the queue event.
        """
        is_standard = True
        path = "script1"

        async def doit():
            await asyncio.wait_for(self.remote.start_task, timeout=START_TIMEOUT)
            await self.assert_next_queue(enabled=False, running=True)

            await self.remote.cmd_enable.start(timeout=STD_TIMEOUT)
            await self.assert_next_queue(enabled=True, running=True)

            def make_add_data(config):
                add_data = self.remote.cmd_add.DataType()
                add_data.isStandard = is_standard
                add_data.path = path
                add_data.config = config
                add_data.location = Location.LAST
                add_data.locationSalIndex = 0
                add_data.descr = "test_processState"
                return add_data

            # pause the queue so we know what to expect of queue state
            await self.remote.cmd_pause.start(timeout=START_TIMEOUT)
            await self.assert_next_queue(running=False)

            # add script I0 that will fail, and so pause the queue
            add_data = make_add_data(config="fail_run: True")
            ackcmd = await self.remote.cmd_add.start(add_data, timeout=START_TIMEOUT)
            seq_num0 = ackcmd.private_seqNum
            await self.assert_next_queue(salIndices=[I0])

            # add script I0+1 that we will terminate
            add_data = make_add_data(config="")
            ackcmd = await self.remote.cmd_add.start(add_data, timeout=START_TIMEOUT)
            seq_num1 = ackcmd.private_seqNum
            await self.assert_next_queue(salIndices=[I0, I0+1])

            # add script I0+2 that we will allow to run normally
            add_data = make_add_data(config="")
            ackcmd = await self.remote.cmd_add.start(add_data, timeout=START_TIMEOUT)
            seq_num2 = ackcmd.private_seqNum
            await self.assert_next_queue(salIndices=[I0, I0+1, I0+2])

            # wait for all scripts to be runnable so future script output
            # is due to the scripts being run or terminated
            await self.wait_runnable(I0, I0+1, I0+2)

            # run the queue and let it pause on failure
            await self.remote.cmd_resume.start(timeout=START_TIMEOUT)
            await self.assert_next_queue(running=True, currentSalIndex=I0,
                                         salIndices=[I0+1, I0+2], pastSalIndices=[])
            await self.assert_next_queue(running=False, currentSalIndex=I0,
                                         salIndices=[I0+1, I0+2], pastSalIndices=[])

            script_data0 = self.remote.evt_script.get()
            self.assertEqual(script_data0.cmdId, seq_num0)
            self.assertEqual(script_data0.salIndex, I0)
            self.assertEqual(script_data0.processState, ScriptProcessState.DONE)
            self.assertEqual(script_data0.scriptState, ScriptState.FAILED)

            # terminate the next script
            stop_data = self.make_stop_data([I0+1], terminate=True)
            await self.remote.cmd_stopScripts.start(stop_data, timeout=START_TIMEOUT)
            await self.assert_next_queue(running=False, currentSalIndex=I0,
                                         salIndices=[I0+2], pastSalIndices=[])

            script_data1 = self.remote.evt_script.get()
            self.assertEqual(script_data1.cmdId, seq_num1)
            self.assertEqual(script_data1.salIndex, I0+1)
            self.assertEqual(script_data1.processState, ScriptProcessState.TERMINATED)
            self.assertEqual(script_data1.scriptState, ScriptState.CONFIGURED)

            # resume the queue and let I0+2 run
            await self.remote.cmd_resume.start(timeout=START_TIMEOUT)
            await self.assert_next_queue(running=True, currentSalIndex=I0+2,
                                         salIndices=[], pastSalIndices=[I0])
            await self.assert_next_queue(running=True, currentSalIndex=0,
                                         salIndices=[], pastSalIndices=[I0+2, I0])

            script_data2 = self.remote.evt_script.get()
            self.assertEqual(script_data2.cmdId, seq_num2)
            self.assertEqual(script_data2.salIndex, I0+2)
            self.assertEqual(script_data2.processState, ScriptProcessState.DONE)
            self.assertEqual(script_data2.scriptState, ScriptState.DONE)

        asyncio.get_event_loop().run_until_complete(doit())

    def test_unloadable_script(self):
        """Test adding a script that fails while loading.
        """
        async def doit():
            await asyncio.wait_for(self.remote.start_task, timeout=START_TIMEOUT)
            await self.assert_next_queue(enabled=False, running=True)

            await self.remote.cmd_enable.start(timeout=STD_TIMEOUT)
            await self.assert_next_queue(enabled=True, running=True)

            await self.remote.cmd_add.set_start(isStandard=True,
                                                path="unloadable",
                                                config="",
                                                location=Location.LAST,
                                                locationSalIndex=0,
                                                descr="test_unloadable_script",
                                                timeout=STD_TIMEOUT)

            await self.assert_next_queue(enabled=True, running=True, salIndices=[I0])

            script_data0 = await self.remote.evt_script.next(flush=False, timeout=STD_TIMEOUT)
            self.assertEqual(script_data0.processState, ScriptProcessState.LOADING)
            script_data0 = await self.remote.evt_script.next(flush=False, timeout=STD_TIMEOUT)
            self.assertEqual(script_data0.processState, ScriptProcessState.LOADFAILED)

            await self.assert_next_queue(enabled=True, running=True)

        asyncio.get_event_loop().run_until_complete(doit())

    def test_move(self):
        """Test move, pause and showQueue
        """
        async def doit():
            await asyncio.wait_for(self.remote.start_task, timeout=START_TIMEOUT)
            await self.assert_next_queue(enabled=False, running=True)

            # pause the queue so we know what to expect of queue state
            # also check that pause works while not enabled
            await self.remote.cmd_pause.start(timeout=START_TIMEOUT)
            await self.assert_next_queue(enabled=False, running=False)

            await self.remote.cmd_enable.start(timeout=STD_TIMEOUT)
            await self.assert_next_queue(enabled=True, running=False)

            # queue scripts I0, I0+1 and I0+2
            sal_indices = [I0, I0+1, I0+2]
            for i, index in enumerate(sal_indices):
                await self.remote.cmd_add.set_start(isStandard=True,
                                                    path=os.path.join("subdir", "script3"),
                                                    config="wait_time: 0.1",
                                                    location=Location.LAST,
                                                    descr=f"test_move {i}",
                                                    timeout=START_TIMEOUT)
                await self.assert_next_queue(salIndices=sal_indices[0:i+1])

            # move I0+2 first
            await self.remote.cmd_move.set_start(salIndex=I0+2,
                                                 location=Location.FIRST,
                                                 timeout=STD_TIMEOUT)
            await self.assert_next_queue(salIndices=[I0+2, I0, I0+1])

            # move I0+2 first again; this should be a no-op
            # but it should still output the queue event
            await self.remote.cmd_move.set_start(salIndex=I0+2,
                                                 location=Location.FIRST,
                                                 timeout=STD_TIMEOUT)
            await self.assert_next_queue(salIndices=[I0+2, I0, I0+1])

            # move I0 last
            await self.remote.cmd_move.set_start(salIndex=I0,
                                                 location=Location.LAST,
                                                 timeout=STD_TIMEOUT)
            await self.assert_next_queue(salIndices=[I0+2, I0+1, I0])

            # move I0 last again; this should be a no-op
            # but it should still output the queue event
            await self.remote.cmd_move.set_start(salIndex=I0,
                                                 location=Location.LAST,
                                                 timeout=STD_TIMEOUT)
            await self.assert_next_queue(salIndices=[I0+2, I0+1, I0])

            # move I0 before I0+2: before first
            await self.remote.cmd_move.set_start(salIndex=I0,
                                                 location=Location.BEFORE,
                                                 locationSalIndex=I0+2,
                                                 timeout=STD_TIMEOUT)
            await self.assert_next_queue(salIndices=[I0, I0+2, I0+1])

            # move I0+1 before I0+2: before not-first
            await self.remote.cmd_move.set_start(salIndex=I0+1,
                                                 location=Location.BEFORE,
                                                 locationSalIndex=I0+2,
                                                 timeout=STD_TIMEOUT)
            await self.assert_next_queue(salIndices=[I0, I0+1, I0+2])

            # move I0 after I0+2: after last
            await self.remote.cmd_move.set_start(salIndex=I0,
                                                 location=Location.AFTER,
                                                 locationSalIndex=I0+2,
                                                 timeout=STD_TIMEOUT)
            await self.assert_next_queue(salIndices=[I0+1, I0+2, I0])

            # move I0+1 after I0+2: after not-last
            await self.remote.cmd_move.set_start(salIndex=I0+1,
                                                 location=Location.AFTER,
                                                 locationSalIndex=I0+2,
                                                 timeout=STD_TIMEOUT)
            await self.assert_next_queue(salIndices=[I0+2, I0+1, I0])

            # move I0 after itself: this should be a no-op
            # but it should still output the queue event
            await self.remote.cmd_move.set_start(salIndex=I0,
                                                 location=Location.AFTER,
                                                 locationSalIndex=I0,
                                                 timeout=STD_TIMEOUT)
            await self.assert_next_queue(salIndices=[I0+2, I0+1, I0])

            # move I0+1 before itself: this should be a no-op
            # but it should still output the queue event
            await self.remote.cmd_move.set_start(salIndex=I0+1,
                                                 location=Location.BEFORE,
                                                 locationSalIndex=I0+1,
                                                 timeout=STD_TIMEOUT)
            await self.assert_next_queue(salIndices=[I0+2, I0+1, I0])

            # try some incorrect moves
            with self.assertRaises(salobj.AckError):
                # no such script
                await self.remote.cmd_move.set_start(salIndex=1234,
                                                     location=Location.LAST,
                                                     timeout=STD_TIMEOUT)

            with self.assertRaises(salobj.AckError):
                # no such location
                await self.remote.cmd_move.set_start(salIndex=I0+1,
                                                     location=21,
                                                     timeout=STD_TIMEOUT)

            with self.assertRaises(salobj.AckError):
                # no such locationSalIndex
                await self.remote.cmd_move.set_start(salIndex=I0+1,
                                                     location=Location.BEFORE,
                                                     locationSalIndex=1234,
                                                     timeout=STD_TIMEOUT)

            # try incorrect index and the same "before" locationSalIndex
            with self.assertRaises(salobj.AckError):
                # no such salIndex; no such locationSalIndex
                await self.remote.cmd_move.set_start(salIndex=1234,
                                                     location=Location.BEFORE,
                                                     locationSalIndex=1234,
                                                     timeout=STD_TIMEOUT)

            # try incorrect index and the same "after" locationSalIndex
            with self.assertRaises(salobj.AckError):
                # no such salIndex; no such locationSalIndex
                await self.remote.cmd_move.set_start(salIndex=1234,
                                                     location=Location.AFTER,
                                                     locationSalIndex=1234,
                                                     timeout=STD_TIMEOUT)

            # make sure those commands did not alter the queue
            await self.remote.cmd_showQueue.start(timeout=STD_TIMEOUT)
            await self.assert_next_queue(salIndices=[I0+2, I0+1, I0])

            await self.queue.model.wait_terminate_all(timeout=START_TIMEOUT)
            for i in range(len(sal_indices)):
                queue_data = await self.remote.evt_queue.next(flush=False, timeout=START_TIMEOUT)
            self.assertEqual(queue_data.length, 0)

        asyncio.get_event_loop().run_until_complete(doit())

    def test_requeue(self):
        """Test requeue, move and terminate
        """
        async def doit():
            await asyncio.wait_for(self.remote.start_task, timeout=START_TIMEOUT)
            await self.assert_next_queue(enabled=False, running=True)

            await self.remote.cmd_enable.start(timeout=STD_TIMEOUT)
            await self.assert_next_queue(enabled=True, running=True)

            # pause the queue so we know what to expect of queue state
            await self.remote.cmd_pause.start(timeout=START_TIMEOUT)
            await self.assert_next_queue(running=False)

            # queue scripts I0, I0+1 and I0+2
            sal_indices = [I0, I0+1, I0+2]
            for i, index in enumerate(sal_indices):
                await self.remote.cmd_add.set_start(isStandard=True,
                                                    path="script2",
                                                    config="wait_time: 0.1",
                                                    location=Location.LAST,
                                                    descr=f"test_requeue {i}",
                                                    timeout=START_TIMEOUT)
                await self.assert_next_queue(salIndices=sal_indices[0:i+1])

            # disable the queue and make sure requeue, move and resume fail
            # (I added some jobs before disabling so we have scripts
            # to try to requeue and move).
            await self.remote.cmd_disable.start(timeout=STD_TIMEOUT)
            await self.assert_next_queue(enabled=False, running=False, salIndices=sal_indices)

            with self.assertRaises(salobj.AckError):
                await self.remote.cmd_requeue.set_start(salIndex=I0,
                                                        location=Location.LAST,
                                                        timeout=START_TIMEOUT)

            with self.assertRaises(salobj.AckError):
                await self.remote.cmd_move.set_start(salIndex=I0+2,
                                                     location=Location.FIRST,
                                                     timeout=STD_TIMEOUT)

            with self.assertRaises(salobj.AckError):
                await self.remote.cmd_resume.start(timeout=STD_TIMEOUT)

            # re-enable the queue and proceed with the rest of the test
            await self.remote.cmd_enable.start(timeout=STD_TIMEOUT)
            await self.assert_next_queue(enabled=True, running=False, salIndices=sal_indices)

            # requeue I0 to last, creating I0+3
            await self.remote.cmd_requeue.set_start(salIndex=I0,
                                                    location=Location.LAST,
                                                    timeout=START_TIMEOUT)
            await self.assert_next_queue(salIndices=[I0, I0+1, I0+2, I0+3])

            # requeue I0 to first, creating I0+4
            await self.remote.cmd_requeue.set_start(salIndex=I0,
                                                    location=Location.FIRST,
                                                    timeout=START_TIMEOUT)
            await self.assert_next_queue(salIndices=[I0+4, I0, I0+1, I0+2, I0+3])

            # requeue I0+2 to before I0+4 (which is first), creating I0+5
            await self.remote.cmd_requeue.set_start(salIndex=I0+2,
                                                    location=Location.BEFORE,
                                                    locationSalIndex=I0+4,
                                                    timeout=START_TIMEOUT)
            await self.assert_next_queue(salIndices=[I0+5, I0+4, I0, I0+1, I0+2, I0+3])

            # requeue I0+3 to before itself (which is not first), creating I0+6
            await self.remote.cmd_requeue.set_start(salIndex=I0+3,
                                                    location=Location.BEFORE,
                                                    locationSalIndex=I0+3,
                                                    timeout=START_TIMEOUT)
            await self.assert_next_queue(salIndices=[I0+5, I0+4, I0, I0+1, I0+2, I0+6, I0+3])

            # requeue I0+3 to after itself (which is last), creating I0+7
            await self.remote.cmd_requeue.set_start(salIndex=I0+3,
                                                    location=Location.AFTER,
                                                    locationSalIndex=I0+3,
                                                    timeout=START_TIMEOUT)
            await self.assert_next_queue(salIndices=[I0+5, I0+4, I0, I0+1, I0+2, I0+6, I0+3, I0+7])

            # requeue I0+5 to last, creating I0+8
            await self.remote.cmd_requeue.set_start(salIndex=I0+5,
                                                    location=Location.LAST,
                                                    timeout=START_TIMEOUT)
            await self.assert_next_queue(salIndices=[I0+5, I0+4, I0+0, I0+1, I0+2, I0+6, I0+3, I0+7, I0+8])

            # stop all scripts except I0+1 and I0+2
            stop_data = self.make_stop_data([I0+5, I0+4, I0, I0+6, I0+3, I0+7, I0+8], terminate=False)
            await self.remote.cmd_stopScripts.start(stop_data, timeout=START_TIMEOUT)
            await self.assert_next_queue(salIndices=[I0+1, I0+2])

            await self.wait_runnable(I0+1, I0+2)

            # run the queue and let it finish
            await self.remote.cmd_resume.start(timeout=START_TIMEOUT)
            await self.assert_next_queue(running=True, currentSalIndex=I0+1,
                                         salIndices=[I0+2], pastSalIndices=[])
            await self.assert_next_queue(running=True, currentSalIndex=I0+2,
                                         salIndices=[], pastSalIndices=[I0+1])
            await self.assert_next_queue(running=True, currentSalIndex=0,
                                         salIndices=[], pastSalIndices=[I0+2, I0+1])

            # pause while we requeue from history
            await self.remote.cmd_pause.start(timeout=START_TIMEOUT)
            await self.assert_next_queue(running=False, currentSalIndex=0,
                                         salIndices=[], pastSalIndices=[I0+2, I0+1])

            # requeue a script from the history queue, creating I0+9
            await self.remote.cmd_requeue.set_start(salIndex=I0+1,
                                                    location=Location.FIRST,
                                                    timeout=START_TIMEOUT)
            await self.assert_next_queue(running=False, currentSalIndex=0,
                                         salIndices=[I0+9], pastSalIndices=[I0+2, I0+1])

            # make sure all scripts are runnable before starting the queue
            # so the queue data is more predictable (otherwise the queue
            # may start up with no script running)
            await self.wait_runnable(I0+9)

            # run the queue and let it finish
            await self.remote.cmd_resume.start(timeout=START_TIMEOUT)
            await self.assert_next_queue(running=True, currentSalIndex=I0+9,
                                         salIndices=[], pastSalIndices=[I0+2, I0+1])
            await self.assert_next_queue(running=True, currentSalIndex=0,
                                         salIndices=[], pastSalIndices=[I0+9, I0+2, I0+1])

        asyncio.get_event_loop().run_until_complete(doit())

    def test_showAvailableScripts(self):
        async def doit():
            await asyncio.wait_for(self.remote.start_task, timeout=START_TIMEOUT)

            # make sure showAvailableScripts fails when not enabled
            with self.assertRaises(salobj.AckError):
                await self.remote.cmd_showAvailableScripts.start(timeout=START_TIMEOUT)

            await self.remote.cmd_enable.start(timeout=STD_TIMEOUT)

            # this should output available scripts without sending the command
            available_scripts0 = await self.remote.evt_availableScripts.next(flush=False,
                                                                             timeout=STD_TIMEOUT)
            with self.assertRaises(asyncio.TimeoutError):
                await self.remote.evt_availableScripts.next(flush=False, timeout=0.1)

            await self.remote.cmd_showAvailableScripts.start(timeout=START_TIMEOUT)

            available_scripts1 = await self.remote.evt_availableScripts.next(flush=False,
                                                                             timeout=STD_TIMEOUT)

            expected_std_set = set(["script1", "script2", "unloadable",
                                    "subdir/script3", "subdir/subsubdir/script4"])
            expected_ext_set = set(["script1", "script5", "subdir/script3", "subdir/script6"])
            for available_scripts in (available_scripts0, available_scripts1):
                standard_set = set(available_scripts.standard.split(":"))
                external_set = set(available_scripts.external.split(":"))
                self.assertEqual(standard_set, expected_std_set)
                self.assertEqual(external_set, expected_ext_set)

            # disable and again make sure showAvailableScripts fails
            await self.remote.cmd_disable.start(timeout=STD_TIMEOUT)

            with self.assertRaises(salobj.AckError):
                await self.remote.cmd_showAvailableScripts.start(timeout=STD_TIMEOUT)

        asyncio.get_event_loop().run_until_complete(doit())

    def test_showSchema(self):
        async def doit():
            is_standard = False
            path = "script1"
            await asyncio.wait_for(self.remote.start_task, timeout=START_TIMEOUT)
            await self.assert_next_queue(enabled=False, running=True)
            self.remote.cmd_showSchema.set(isStandard=is_standard, path=path)

            # make sure showSchema fails when not enabled
            with self.assertRaises(salobj.AckError):
                await self.remote.cmd_showSchema.start(timeout=STD_TIMEOUT)

            await self.remote.cmd_enable.start(timeout=STD_TIMEOUT)
            await self.assert_next_queue(enabled=True, running=True)

            await self.remote.cmd_showSchema.start(timeout=START_TIMEOUT)
            data = await self.remote.evt_configSchema.next(flush=False, timeout=STD_TIMEOUT)
            self.assertEqual(data.isStandard, is_standard)
            self.assertEqual(data.path, path)
            schema = yaml.safe_load(data.configSchema)
            self.assertEqual(schema, salobj.TestScript.get_schema())

        asyncio.get_event_loop().run_until_complete(doit())

    def test_showQueue(self):
        async def doit():
            await asyncio.wait_for(self.remote.start_task, timeout=START_TIMEOUT)
            await self.assert_next_queue(enabled=False, running=True)

            # make sure showQueue fails when not enabled
            with self.assertRaises(salobj.AckError):
                await self.remote.cmd_showQueue.start(timeout=STD_TIMEOUT)

            await self.remote.cmd_enable.start(timeout=STD_TIMEOUT)
            await self.assert_next_queue(enabled=True, running=True)

            # make sure we have no more queue events
            with self.assertRaises(asyncio.TimeoutError):
                await self.remote.evt_queue.next(flush=False, timeout=0.1)

            await self.remote.cmd_showQueue.start(timeout=STD_TIMEOUT)
            await self.assert_next_queue(enabled=True, running=True)

            # make sure disabling the queue outputs the queue event,
            # with runnable False, and disables the showQueue command.
            await self.remote.cmd_disable.start(timeout=STD_TIMEOUT)
            await self.assert_next_queue(enabled=False, running=True)
            with self.assertRaises(asyncio.TimeoutError):
                await self.remote.evt_queue.next(flush=False, timeout=0.1)
            with self.assertRaises(salobj.AckError):
                await self.remote.cmd_showQueue.start(timeout=STD_TIMEOUT)

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


class CmdLineTestCase(unittest.TestCase):
    def setUp(self):
        salobj.set_random_lsst_dds_domain()
        self.index = 1
        self.default_standardpath = scriptqueue.get_default_scripts_dir(is_standard=True)
        self.default_externalpath = scriptqueue.get_default_scripts_dir(is_standard=False)
        self.datadir = os.path.abspath(os.path.join(os.path.dirname(__file__), "data"))
        self.testdata_standardpath = os.path.join(self.datadir, "standard")
        self.testdata_externalpath = os.path.join(self.datadir, "external")
        self.badpath = os.path.join(self.datadir, "not_a_directory")

    def test_run_with_standard_and_external(self):
        exe_name = "run_script_queue.py"
        exe_path = shutil.which(exe_name)
        if exe_path is None:
            self.fail(f"Could not find bin script {exe_name}; did you setup and scons this package?")

        async def doit():
            async with salobj.Domain() as domain:
                remote = salobj.Remote(domain, "ScriptQueue", index=self.index,
                                       evt_max_history=0, tel_max_history=0)
                await asyncio.wait_for(remote.start_task, timeout=START_TIMEOUT)

                process = await asyncio.create_subprocess_exec(
                    exe_name, str(self.index), "--standard", self.testdata_standardpath,
                    "--external", self.testdata_externalpath, "--verbose")
                try:

                    summaryState_data = await remote.evt_summaryState.next(flush=False, timeout=START_TIMEOUT)
                    self.assertEqual(summaryState_data.summaryState, salobj.State.STANDBY)

                    rootDir_data = await remote.evt_rootDirectories.next(flush=False, timeout=STD_TIMEOUT)
                    self.assertTrue(os.path.samefile(rootDir_data.standard, self.testdata_standardpath))
                    self.assertTrue(os.path.samefile(rootDir_data.external, self.testdata_externalpath))

                    ackcmd = await remote.cmd_exitControl.start(timeout=STD_TIMEOUT)
                    self.assertEqual(ackcmd.ack, salobj.SalRetCode.CMD_COMPLETE)
                    summaryState_data = await remote.evt_summaryState.next(flush=False, timeout=START_TIMEOUT)
                    self.assertEqual(summaryState_data.summaryState, salobj.State.OFFLINE)

                    await asyncio.wait_for(process.wait(), timeout=5)
                except Exception:
                    if process.returncode is None:
                        process.terminate()
                    raise

        asyncio.get_event_loop().run_until_complete(doit())

    def test_run_default_standard_external(self):
        exe_name = "run_script_queue.py"
        exe_path = shutil.which(exe_name)
        if exe_path is None:
            self.fail(f"Could not find bin script {exe_name}; did you setup and scons this package?")

        async def doit():
            async with salobj.Domain() as domain:
                remote = salobj.Remote(domain, "ScriptQueue", index=self.index,
                                       evt_max_history=0, tel_max_history=0)
                await asyncio.wait_for(remote.start_task, timeout=START_TIMEOUT)

                process = await asyncio.create_subprocess_exec(exe_name, str(self.index), "--verbose")
                try:

                    summaryState_data = await remote.evt_summaryState.next(flush=False, timeout=START_TIMEOUT)
                    self.assertEqual(summaryState_data.summaryState, salobj.State.STANDBY)

                    rootDir_data = await remote.evt_rootDirectories.next(flush=False, timeout=STD_TIMEOUT)
                    self.assertTrue(os.path.samefile(rootDir_data.standard, self.default_standardpath))
                    self.assertTrue(os.path.samefile(rootDir_data.external, self.default_externalpath))

                    ackcmd = await remote.cmd_exitControl.start(timeout=STD_TIMEOUT)
                    self.assertEqual(ackcmd.ack, salobj.SalRetCode.CMD_COMPLETE)
                    summaryState_data = await remote.evt_summaryState.next(flush=False, timeout=START_TIMEOUT)
                    self.assertEqual(summaryState_data.summaryState, salobj.State.OFFLINE)

                    await asyncio.wait_for(process.wait(), timeout=5)
                except Exception:
                    if process.returncode is None:
                        process.terminate()
                    raise

        asyncio.get_event_loop().run_until_complete(doit())


if __name__ == "__main__":
    unittest.main()
