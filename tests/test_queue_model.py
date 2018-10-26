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


class QueueModelTestCase(unittest.TestCase):
    def setUp(self):
        salobj.test_utils.set_random_lsst_dds_domain()
        self.datadir = os.path.abspath(os.path.join(os.path.dirname(__file__), "data"))
        self.standardpath = os.path.join(self.datadir, "standard")
        self.externalpath = os.path.join(self.datadir, "external")
        self.model = scriptloader.QueueModel(standardpath=self.standardpath,
                                             externalpath=self.externalpath,
                                             min_sal_index=1000)

    def tearDown(self):
        nkilled = self.model.terminate_all()
        if nkilled > 0:
            warnings.warn(f"Killed {nkilled} subprocesses")

    def assert_index_list(self, index_list):
        """Assert that the indices in the queue match index_list."""
        actual_index_list = [info.index for info in self.model.queue]
        self.assertEqual(actual_index_list, index_list)

    def assert_script_info_equal(self, info1, info2):
        """Assert two ScriptInfo are equal."""
        self.assertEqual(info1.index, info2.index)
        self.assertEqual(info1.cmd_id, info2.cmd_id)
        self.assertEqual(info1.is_standard, info2.is_standard)
        self.assertEqual(info1.path, info2.path)
        self.assertEqual(info1.config, info2.config)
        self.assertEqual(info1.descr, info2.descr)

    def test_constructor_errors(self):
        nonexistentpath = os.path.join(self.datadir, "garbage")
        with self.assertRaises(ValueError):
            scriptloader.QueueModel(standardpath=self.standardpath, externalpath=nonexistentpath)
        with self.assertRaises(ValueError):
            scriptloader.QueueModel(standardpath=nonexistentpath, externalpath=self.externalpath)
        with self.assertRaises(ValueError):
            scriptloader.QueueModel(standardpath=nonexistentpath, externalpath=nonexistentpath)

    def test_add_last(self):
        """Test adding scripts to the end of the queue.

        Also test running those scripts, the state callback
        and that get_script_info works for the queue and for history.
        """
        num_scripts = 3
        script_path = os.path.join("subdir", "script6")
        config = "wait_time: 0.1"
        self.model.running = False  # pause the queue

        async def doit():
            info_list = [
                scriptloader.ScriptInfo(
                    index=self.model.next_sal_index,
                    cmd_id=i + 10,  # arbitrary
                    is_standard=False,
                    path=script_path,
                    config=config,
                    descr="test_add_last",
                ) for i in range(num_scripts)]

            self.num_queue_calls = 0
            self.num_queue_failures = 0
            # total number of expected calls to the state function:
            # - one as we add each script
            # - one as we start each script
            # - one as the final script ends
            self.expected_total_calls = num_scripts*2 + 1

            def queue_callback():
                self.num_queue_calls += 1
                if self.num_queue_calls <= num_scripts:
                    # adding scripts to the queue
                    expected_queue_len = self.num_queue_calls
                    expect_current = False
                elif self.num_queue_calls <= num_scripts * 2:
                    # removing scripts from the queue and adding to current_script
                    expected_queue_len = self.expected_total_calls - self.num_queue_calls - 1
                    expect_current = True
                else:
                    # the last script has finished
                    expected_queue_len = 0
                    expect_current = False
                if len(self.model.queue) != expected_queue_len:
                    self.num_queue_failures += 1
                if expect_current != (self.model.current_script is not None):
                    self.num_queue_failures += 1

            self.model.queue_callback = queue_callback

            # add the scripts to the end of the queue
            for info in info_list:
                await self.model.add(script_info=info,
                                     location=SALPY_ScriptQueue.add_Last,
                                     location_sal_index=0)

            self.assert_index_list([1000, 1001, 1002])

            # check that get_script_info works for the queue
            for desired_info in info_list:
                get_info = self.model.get_script_info(desired_info.index)
                self.assert_script_info_equal(get_info, desired_info)

            # resume the queue and wait for the scripts to run
            self.model.running = True
            await asyncio.wait_for(asyncio.gather(*[info.process.wait() for info in info_list]), 20)

            # wait for final queue callback
            await asyncio.sleep(0.1)
            if self.num_queue_failures > 0:
                self.fail(f"queue_callback failed {self.num_queue_failures} times")
            self.assertEqual(self.num_queue_calls, self.expected_total_calls)

            # check that get_script_info works for the history
            for desired_info in info_list:
                get_info = self.model.get_script_info(desired_info.index)
                self.assert_script_info_equal(get_info, desired_info)

        asyncio.get_event_loop().run_until_complete(doit())

    def test_add_first(self):
        """Test adding to the front of the queue.

        Don't bother to run the scripts as test_add_first does that.
        """
        num_scripts = 3
        script_path = os.path.join("subdir", "script6")
        config = "wait_time: 0.1"
        self.model.running = False  # pause the queue

        async def doit():
            info_list = [
                scriptloader.ScriptInfo(
                    index=self.model.next_sal_index,
                    cmd_id=i + 10,  # arbitrary
                    is_standard=False,
                    path=script_path,
                    config=config,
                    descr="test_add_first",
                ) for i in range(num_scripts)]

            # add the scripts to the front of the queue
            for info in info_list:
                await self.model.add(script_info=info,
                                     location=SALPY_ScriptQueue.add_First,
                                     location_sal_index=0)
            self.assert_index_list([1002, 1001, 1000])
            for info in info_list:
                self.assertIsNone(info.process.returncode)
                info.terminate()

            # wait for processes to terminate
            await asyncio.wait_for(asyncio.gather(*[info.process.wait() for info in info_list]), 2)
            self.assertEqual(len(self.model.queue), 0)

        asyncio.get_event_loop().run_until_complete(doit())

    def test_add_relative(self):
        """Test adding scripts relative to other scripts

        Don't bother to run the scripts as test_add_first does that.
        """
        num_scripts = 3
        script_path = os.path.join("subdir", "script6")
        config = "wait_time: 0.1"
        self.model.running = False  # pause the queue

        async def doit():
            info_list = [
                scriptloader.ScriptInfo(
                    index=self.model.next_sal_index,
                    cmd_id=i + 10,  # arbitrary
                    is_standard=False,
                    path=script_path,
                    config=config,
                    descr="test_add_relative",
                ) for i in range(num_scripts)]

            # add the first script; location doesn't matter
            await self.model.add(script_info=info_list[0],
                                 location=SALPY_ScriptQueue.add_First,
                                 location_sal_index=0)

            # add the second script before the first script
            await self.model.add(script_info=info_list[1],
                                 location=SALPY_ScriptQueue.add_Before,
                                 location_sal_index=info_list[0].index)

            # add the third script after the second script
            await self.model.add(script_info=info_list[2],
                                 location=SALPY_ScriptQueue.add_After,
                                 location_sal_index=info_list[1].index)

            expected_sal_indices = [1001, 1002, 1000]
            self.assert_index_list(expected_sal_indices)

            # test error handling
            script_info = info_list[0]
            with self.assertRaises(ValueError):  # bad location_sal_index
                await self.model.add(script_info=script_info,
                                     location=SALPY_ScriptQueue.add_Before,
                                     location_sal_index=1)
            self.assert_index_list(expected_sal_indices)

            with self.assertRaises(ValueError):  # bad location_sal_index
                await self.model.add(script_info=script_info,
                                     location=SALPY_ScriptQueue.add_After,
                                     location_sal_index=1)
            self.assert_index_list(expected_sal_indices)

            with self.assertRaises(ValueError):  # bad location
                await self.model.add(script_info=script_info,
                                     location=-5,
                                     location_sal_index=1001)
            self.assert_index_list(expected_sal_indices)

            for info in info_list:
                self.assertIsNone(info.process.returncode)
                info.terminate()

            # wait for processes to terminate
            await asyncio.wait_for(asyncio.gather(*[info.process.wait() for info in info_list]), 2)
            self.assertEqual(len(self.model.queue), 0)

        asyncio.get_event_loop().run_until_complete(doit())

    def test_make_full_path(self):
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
                    self.model.make_full_path(is_standard=is_standard, path=badpath)

        for is_standard, goodpath in (
            (True, "subdir/subsubdir/script4"),
            (False, "subdir/script3"),
            (True, "script2"),
        ):
            with self.subTest(is_standard=is_standard, path=goodpath):
                root = self.standardpath if is_standard else self.externalpath
                fullpath = self.model.make_full_path(is_standard=is_standard, path=goodpath)
                expected_fullpath = os.path.join(root, goodpath)
                self.assertTrue(fullpath.samefile(expected_fullpath))

    def test_move(self):
        """Test moving scripts

        Don't bother to run the scripts as test_add_first does that.
        """
        num_scripts = 3
        script_path = os.path.join("subdir", "script6")
        config = "wait_time: 0.1"
        self.model.running = False  # pause the queue

        async def doit():
            info_list = [
                scriptloader.ScriptInfo(
                    index=self.model.next_sal_index,
                    cmd_id=i + 10,  # arbitrary
                    is_standard=False,
                    path=script_path,
                    config=config,
                    descr="test_move",
                ) for i in range(num_scripts)]

            # add the scripts to the end of the queue
            for info in info_list:
                await self.model.add(script_info=info,
                                     location=SALPY_ScriptQueue.add_Last,
                                     location_sal_index=0)
            self.assert_index_list([1000, 1001, 1002])

            # move scripts
            self.model.move(sal_index=1002, location=SALPY_ScriptQueue.add_First, location_sal_index=0)
            self.assert_index_list([1002, 1000, 1001])

            self.model.move(sal_index=1000, location=SALPY_ScriptQueue.add_Last, location_sal_index=0)
            self.assert_index_list([1002, 1001, 1000])

            self.model.move(sal_index=1002, location=SALPY_ScriptQueue.add_After, location_sal_index=1001)
            self.assert_index_list([1001, 1002, 1000])

            self.model.move(sal_index=1000, location=SALPY_ScriptQueue.add_Before, location_sal_index=1001)
            self.assert_index_list([1000, 1001, 1002])

            # moves that are no-ops
            expected_sal_indices = [1000, 1001, 1002]
            self.model.move(sal_index=1000, location=SALPY_ScriptQueue.add_First, location_sal_index=0)
            self.assert_index_list(expected_sal_indices)

            self.model.move(sal_index=1002, location=SALPY_ScriptQueue.add_Last, location_sal_index=0)
            self.assert_index_list(expected_sal_indices)

            self.model.move(sal_index=1000, location=SALPY_ScriptQueue.add_After, location_sal_index=1000)
            self.assert_index_list(expected_sal_indices)

            # test error handling
            expected_sal_indices = [1000, 1001, 1002]
            with self.assertRaises(ValueError):  # bad sal_index
                self.model.move(sal_index=1, location=SALPY_ScriptQueue.add_First, location_sal_index=0)
            self.assert_index_list(expected_sal_indices)

            with self.assertRaises(ValueError):  # bad location_sal_index
                self.model.move(sal_index=1001, location=SALPY_ScriptQueue.add_Before, location_sal_index=1)
            self.assert_index_list(expected_sal_indices)

            with self.assertRaises(ValueError):  # bad location_sal_index
                self.model.move(sal_index=1001, location=SALPY_ScriptQueue.add_After, location_sal_index=1)
            self.assert_index_list(expected_sal_indices)

            with self.assertRaises(ValueError):  # bad location
                self.model.move(sal_index=1001, location=-5, location_sal_index=1001)
            self.assert_index_list(expected_sal_indices)

            for info in info_list:
                self.assertIsNone(info.process.returncode)
                info.terminate()

            # wait for processes to terminate
            await asyncio.wait_for(asyncio.gather(*[info.process.wait() for info in info_list]), 2)
            self.assertEqual(len(self.model.queue), 0)

        asyncio.get_event_loop().run_until_complete(doit())

    def test_remove(self):
        """Test remove

        Also check that get_script_info works for the current task
        """
        num_scripts = 3
        script_path = os.path.join("subdir", "script6")
        config = "wait_time: 5"
        self.model.running = False  # pause the queue

        async def doit():
            info_list = [
                scriptloader.ScriptInfo(
                    index=self.model.next_sal_index,
                    cmd_id=i + 10,  # arbitrary
                    is_standard=False,
                    path=script_path,
                    config=config,
                    descr="test_move",
                ) for i in range(num_scripts)]
            script_1000 = info_list[0]
            script_1001 = info_list[1]
            script_1002 = info_list[2]

            # add the scripts to the end of the queue
            for info in info_list:
                await self.model.add(script_info=info,
                                     location=SALPY_ScriptQueue.add_Last,
                                     location_sal_index=0)
            self.assert_index_list([1000, 1001, 1002])

            # test error handling
            with self.assertRaises(ValueError):
                self.model.remove(1)

            self.model.remove(1002)
            self.assert_index_list([1000, 1001])
            # check that script 1002 is terminated
            await asyncio.wait_for(script_1002.start_task, 2)
            await asyncio.wait_for(script_1002.process_task, 2)

            info_list = info_list[0:2]  # ditch 1002

            # wait until the remaining tasks are ready to be run
            await asyncio.wait_for(asyncio.gather(*[info.start_task for info in info_list]), 2)
            await asyncio.wait_for(asyncio.gather(*[info.config_task for info in info_list]), 20)

            # start the queue and wait for the script 1000 to be running,
            # so we can test removing a running task
            self.model.running = True
            while True:
                state = await script_1000.remote.evt_state.next(flush=False, timeout=2)
                if state.state == scriptloader.ScriptState.RUNNING:
                    break

            # check that get_script_info works for the current script
            get_info = self.model.get_script_info(1000)
            self.assert_script_info_equal(get_info, script_1000)

            self.assertEqual(len(self.model.queue), 1)
            self.model.running = False  # don't let 1001 start
            self.model.remove(1000)  # running task
            await asyncio.wait_for(script_1000.process_task, 2)
            await asyncio.sleep(0.1)
            self.assertEqual(len(self.model.queue), 1)
            self.assertFalse(script_1001.done)
            self.model.remove(1001)  # queued task
            await asyncio.wait_for(script_1001.process_task, 2)
            self.assertEqual(len(self.model.queue), 0)

        asyncio.get_event_loop().run_until_complete(doit())

    def test_requeue(self):
        """Test requeue
        """
        num_scripts = 3
        script_path = os.path.join("subdir", "script6")
        config = "wait_time: 0.1"
        self.model.running = False  # pause the queue

        async def doit():
            info_list = [
                scriptloader.ScriptInfo(
                    index=self.model.next_sal_index,
                    cmd_id=i + 10,  # arbitrary
                    is_standard=False,
                    path=script_path,
                    config=config,
                    descr="test_requeue initial run",
                ) for i in range(num_scripts)]

            # add the scripts to the end of the queue
            for info in info_list:
                await self.model.add(script_info=info,
                                     location=SALPY_ScriptQueue.add_Last,
                                     location_sal_index=0)

            self.assert_index_list([1000, 1001, 1002])

            # resume the queue and wait for the scripts to run
            self.model.running = True
            await asyncio.wait_for(asyncio.gather(*[info.process.wait() for info in info_list]), 20)

            await asyncio.sleep(0.1)  # let queue status update
            self.assertEqual(len(self.model.queue), 0)
            self.assertIsNone(self.model.current_script)
            self.assertEqual(len(self.model.history), 3)

            # requeue in reverse order
            self.model.running = False
            rqi2 = self.model.requeue(sal_index=1002,
                                      cmd_id=32,
                                      location=SALPY_ScriptQueue.add_First,
                                      location_sal_index=0)
            rqi0 = self.model.requeue(sal_index=1000,
                                      cmd_id=30,
                                      location=SALPY_ScriptQueue.add_After,
                                      location_sal_index=1002)
            rqi1 = self.model.requeue(sal_index=1001,
                                      cmd_id=31,
                                      location=SALPY_ScriptQueue.add_Before,
                                      location_sal_index=1000)
            requeue_info_list = [rqi0, rqi1, rqi2]
            self.assert_index_list([1002, 1001, 1000])
            for requeue_info, info in zip(requeue_info_list, info_list):
                self.assertEqual(requeue_info.index, info.index)
                self.assertNotEqual(requeue_info.cmd_id, info.cmd_id)
                self.assertEqual(requeue_info.is_standard, info.is_standard)
                self.assertEqual(requeue_info.path, info.path)
                self.assertEqual(requeue_info.config, info.config)
                self.assertEqual(requeue_info.descr, info.descr)
                self.assertFalse(requeue_info.done)
                requeue_info.terminate()

        asyncio.get_event_loop().run_until_complete(doit())


if __name__ == "__main__":
    unittest.main()
