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

import asyncio
import os
import time
import unittest
import warnings

import SALPY_ScriptQueue
import salobj
import ts_scriptqueue


class QueueModelTestCase(unittest.TestCase):
    def setUp(self):
        salobj.test_utils.set_random_lsst_dds_domain()
        self.datadir = os.path.abspath(os.path.join(os.path.dirname(__file__), "data"))
        self.standardpath = os.path.join(self.datadir, "standard")
        self.externalpath = os.path.join(self.datadir, "external")
        self.model = ts_scriptqueue.QueueModel(standardpath=self.standardpath,
                                               externalpath=self.externalpath,
                                               queue_callback=self.queue_callback,
                                               script_callback=self.script_callback,
                                               min_sal_index=1000,
                                               )
        # support assert_next_queue using a future and a queue callback
        self.queue_task = asyncio.Future()
        self.model.queue_callback = self.queue_callback
        self.model.enabled = True

    def tearDown(self):
        nkilled = len(self.model.terminate_all())
        if nkilled > 0:
            warnings.warn(f"Killed {nkilled} subprocesses")

    async def assert_next_queue(self, enabled=True, running=False, current_sal_index=0,
                                sal_indices=(), past_sal_indices=(), wait=False):
        """Assert that the queue is in a particular state.

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
        wait : `bool`
            If True then wait for queue_task.
        """
        if wait:
            await asyncio.wait_for(self.queue_task, 20)
        self.assertEqual(self.model.running, running)
        self.assertEqual(self.model.current_index, current_sal_index)
        self.assertEqual([info.index for info in self.model.queue], list(sal_indices))
        self.assertEqual([info.index for info in self.model.history], list(past_sal_indices))
        self.queue_task = asyncio.Future()

    def assert_script_info_equal(self, info1, info2, is_requeue=False):
        """Assert two ScriptInfo are equal.

        If is_requeue (indicating that we are comparing a requeued
        version of a script to its original) then the index and cmd_id
        must differ between the two scripts.
        """
        if is_requeue:
            self.assertNotEqual(info1.index, info2.index)
            self.assertNotEqual(info1.cmd_id, info2.cmd_id)
        else:
            self.assertEqual(info1.index, info2.index)
            self.assertEqual(info1.cmd_id, info2.cmd_id)
        self.assertEqual(info1.is_standard, info2.is_standard)
        self.assertEqual(info1.path, info2.path)
        self.assertEqual(info1.config, info2.config)
        self.assertEqual(info1.descr, info2.descr)

    def queue_callback(self):
        print(f"queue_callback(): enabled={self.model.enabled}; "
              f"running={self.model.running}; "
              f"current={self.model.current_index}; "
              f"queue={[info.index for info in self.model.queue]}; "
              f"history={[info.index for info in self.model.history]}")
        if not self.queue_task.done():
            self.queue_task.set_result(None)

    def script_callback(self, script_info):
        print(f"script_callback for {script_info.index} at {time.time():0.1f}: "
              f"started={script_info.start_task.done()}; "
              f"configured={script_info.configured}; done={script_info.done}")

    def test_add(self):
        """Test add."""
        def make_add_kwargs(location, location_sal_index=0):
            sal_index = self.model.next_sal_index
            return dict(
                script_info=ts_scriptqueue.ScriptInfo(
                    index=sal_index,
                    cmd_id=sal_index*2,  # arbitrary
                    is_standard=False,
                    path=os.path.join("subdir", "script6"),
                    config="wait_time: 0.1",
                    descr=f"test_add {sal_index}",
                ),
                location=location,
                location_sal_index=location_sal_index,
            )

        async def doit():
            await self.assert_next_queue(enabled=False, running=True)

            self.model.enable = True
            await self.assert_next_queue(enabled=True, running=True)

            # pause the queue so we know what to expect of queue state
            self.model.running = False
            await self.assert_next_queue(running=False)

            # add script 1000; queue is empty to location is irrelevant
            add_kwargs = make_add_kwargs(location=SALPY_ScriptQueue.add_Last)
            await self.model.add(**add_kwargs)
            await self.assert_next_queue(sal_indices=[1000])

            # add script 1001 last: test add last
            add_kwargs = make_add_kwargs(location=SALPY_ScriptQueue.add_Last)
            await self.model.add(**add_kwargs)
            await self.assert_next_queue(sal_indices=[1000, 1001])

            # add script 1002 first: test add first
            add_kwargs = make_add_kwargs(location=SALPY_ScriptQueue.add_First)
            await self.model.add(**add_kwargs)
            await self.assert_next_queue(sal_indices=[1002, 1000, 1001])

            # add script 1003 after 1001: test add after last
            add_kwargs = make_add_kwargs(location=SALPY_ScriptQueue.add_After,
                                         location_sal_index=1001)
            await self.model.add(**add_kwargs)
            await self.assert_next_queue(sal_indices=[1002, 1000, 1001, 1003])

            # add script 1004 after 1002: test add after not-last
            add_kwargs = make_add_kwargs(location=SALPY_ScriptQueue.add_After,
                                         location_sal_index=1002)
            await self.model.add(**add_kwargs)
            await self.assert_next_queue(sal_indices=[1002, 1004, 1000, 1001, 1003])

            # add script 1005 before 1002: test add before first
            add_kwargs = make_add_kwargs(location=SALPY_ScriptQueue.add_Before,
                                         location_sal_index=1002)
            await self.model.add(**add_kwargs)
            await self.assert_next_queue(sal_indices=[1005, 1002, 1004, 1000, 1001, 1003])

            # add script 1006 before 1000: test add before not first
            add_kwargs = make_add_kwargs(location=SALPY_ScriptQueue.add_Before,
                                         location_sal_index=1000)
            await self.model.add(**add_kwargs)
            await self.assert_next_queue(sal_indices=[1005, 1002, 1004, 1006, 1000, 1001, 1003])

            # try some failed adds
            # incorrect path
            add_kwargs = make_add_kwargs(location=SALPY_ScriptQueue.add_First)
            add_kwargs["script_info"].path = "bogus_script_name"
            with self.assertRaises(ValueError):
                await self.model.add(**add_kwargs)
            await self.assert_next_queue(sal_indices=[1005, 1002, 1004, 1006, 1000, 1001, 1003])

            # incorrect location
            add_kwargs = make_add_kwargs(location=25)
            with self.assertRaises(ValueError):
                await self.model.add(**add_kwargs)
            await self.assert_next_queue(sal_indices=[1005, 1002, 1004, 1006, 1000, 1001, 1003])

            # incorrect location_sal_index
            add_kwargs = make_add_kwargs(location=SALPY_ScriptQueue.add_After,
                                         location_sal_index=4321)
            with self.assertRaises(ValueError):
                await self.model.add(**add_kwargs)
            await self.assert_next_queue(sal_indices=[1005, 1002, 1004, 1006, 1000, 1001, 1003])

            # remove a few scripts
            self.model.remove(1006)
            await self.assert_next_queue(sal_indices=[1005, 1002, 1004, 1000, 1001, 1003])

            self.model.remove(1005)
            await self.assert_next_queue(sal_indices=[1002, 1004, 1000, 1001, 1003])

            self.model.remove(1000)
            await self.assert_next_queue(sal_indices=[1002, 1004, 1001, 1003])

            self.model.remove(1004)
            await self.assert_next_queue(sal_indices=[1002, 1001, 1003])

            # try to remove a non-existent script
            with self.assertRaises(ValueError):
                self.model.remove(5432)
            await self.assert_next_queue(sal_indices=[1002, 1001, 1003])

            # make sure all scripts are runnable before starting the queue
            # so the queue data is more predictable (otherwise the queue
            # may start up with no script running)
            await self.wait_runnable(1001, 1002, 1003)

            # disable the queue, then set running True and check that
            # the queue does not start running until we enable it again
            self.model.enabled = False
            await self.assert_next_queue(enabled=False, running=False, sal_indices=[1002, 1001, 1003])

            self.model.running = True
            await self.assert_next_queue(enabled=False, running=True, sal_indices=[1002, 1001, 1003])

            self.model.enabled = True
            await self.assert_next_queue(running=True, current_sal_index=1002,
                                         sal_indices=[1001, 1003], past_sal_indices=[], wait=True)

            await self.assert_next_queue(running=True, current_sal_index=1001,
                                         sal_indices=[1003], past_sal_indices=[1002], wait=True)

            await self.assert_next_queue(running=True, current_sal_index=1003,
                                         sal_indices=[], past_sal_indices=[1001, 1002], wait=True)

            await self.assert_next_queue(running=True, current_sal_index=0,
                                         sal_indices=[], past_sal_indices=[1003, 1001, 1002], wait=True)

        asyncio.get_event_loop().run_until_complete(doit())

    def test_constructor_errors(self):
        nonexistentpath = os.path.join(self.datadir, "garbage")
        with self.assertRaises(ValueError):
            ts_scriptqueue.QueueModel(standardpath=self.standardpath, externalpath=nonexistentpath)
        with self.assertRaises(ValueError):
            ts_scriptqueue.QueueModel(standardpath=nonexistentpath, externalpath=self.externalpath)
        with self.assertRaises(ValueError):
            ts_scriptqueue.QueueModel(standardpath=nonexistentpath, externalpath=nonexistentpath)

    def test_get_script_info(self):
        async def doit():
            await self.assert_next_queue(enabled=False, running=True)

            self.model.enabled = True
            await self.assert_next_queue(enabled=True, running=True)

            # pause the queue so we know what to expect of queue state
            self.model.running = False
            await self.assert_next_queue(running=False)

            info_dict = {}
            for i in range(3):
                script_info = ts_scriptqueue.ScriptInfo(
                    index=self.model.next_sal_index,
                    cmd_id=i + 10,  # arbitrary
                    is_standard=False,
                    path=os.path.join("subdir", "script6"),
                    config="wait_time: 0.5" if i == 1 else "",
                    descr=f"test_get_script_info {i}",
                )
                info_dict[script_info.index] = script_info
                await self.model.add(script_info=script_info,
                                     location=SALPY_ScriptQueue.add_Last,
                                     location_sal_index=0)

            await self.assert_next_queue(sal_indices=[1000, 1001, 1002])

            await self.wait_runnable(1000, 1001, 1002)

            # resume the queue and wait for the second script to start
            # running. At that point we have one running script, one in
            # history and one on the queue. Run get_script_info on each.
            self.model.running = True
            await self.assert_next_queue(running=True, current_sal_index=1000,
                                         sal_indices=[1001, 1002], past_sal_indices=[], wait=True)
            await self.assert_next_queue(running=True, current_sal_index=1001,
                                         sal_indices=[1002], past_sal_indices=[1000], wait=True)

            for sal_index, expected_script_info in info_dict.items():
                script_info = self.model.get_script_info(sal_index=sal_index)
                self.assert_script_info_equal(script_info, expected_script_info)

            await self.model.wait_terminate_all()

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
                with self.assertRaises(ValueError):
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
        """Test move, pause and showQueue
        """
        async def doit():
            await self.assert_next_queue(enabled=False, running=True)

            self.model.enabled = True
            await self.assert_next_queue(enabled=True, running=True)

            # pause the queue so we know what to expect of queue state
            self.model.running = False
            await self.assert_next_queue(running=False)

            # queue scripts 1000, 1001 and 1002
            sal_indices = [1000, 1001, 1002]
            for i, index in enumerate(sal_indices):
                sal_index = self.model.next_sal_index
                script_info = ts_scriptqueue.ScriptInfo(
                    index=sal_index,
                    cmd_id=sal_index*2,  # arbitrary
                    is_standard=True,
                    path=os.path.join("subdir", "script3"),
                    config="wait_time: 0.1",
                    descr=f"test_move {i}",
                )
                await self.model.add(script_info=script_info,
                                     location=SALPY_ScriptQueue.add_Last,
                                     location_sal_index=0)
                await self.assert_next_queue(sal_indices=sal_indices[0:i+1])

            # move 1002 first
            self.model.move(sal_index=1002,
                            location=SALPY_ScriptQueue.add_First,
                            location_sal_index=0)
            await self.assert_next_queue(sal_indices=[1002, 1000, 1001])

            # move 1002 first again (should be a no-op)
            self.model.move(sal_index=1002,
                            location=SALPY_ScriptQueue.add_First,
                            location_sal_index=0)
            await self.assert_next_queue(sal_indices=[1002, 1000, 1001])

            # move 1000 last
            self.model.move(sal_index=1000,
                            location=SALPY_ScriptQueue.add_Last,
                            location_sal_index=0)
            await self.assert_next_queue(sal_indices=[1002, 1001, 1000])

            # move 1000 last again (should be a no-op)
            self.model.move(sal_index=1000,
                            location=SALPY_ScriptQueue.add_Last,
                            location_sal_index=0)
            await self.assert_next_queue(sal_indices=[1002, 1001, 1000])

            # move 1000 before 1002: before first
            self.model.move(sal_index=1000,
                            location=SALPY_ScriptQueue.add_Before,
                            location_sal_index=1002)
            await self.assert_next_queue(sal_indices=[1000, 1002, 1001])

            # move 1001 before 1002: before not-first
            self.model.move(sal_index=1001,
                            location=SALPY_ScriptQueue.add_Before,
                            location_sal_index=1002)
            await self.assert_next_queue(sal_indices=[1000, 1001, 1002])

            # move 1000 after 1002: after last
            self.model.move(sal_index=1000,
                            location=SALPY_ScriptQueue.add_After,
                            location_sal_index=1002)
            await self.assert_next_queue(sal_indices=[1001, 1002, 1000])

            # move 1001 after 1002: after not-last
            self.model.move(sal_index=1001,
                            location=SALPY_ScriptQueue.add_After,
                            location_sal_index=1002)
            await self.assert_next_queue(sal_indices=[1002, 1001, 1000])

            # move 1000 after itself: this should be a no-op
            # but it should still output the queue event
            self.model.move(sal_index=1000,
                            location=SALPY_ScriptQueue.add_After,
                            location_sal_index=1000)
            await self.assert_next_queue(sal_indices=[1002, 1001, 1000])

            # move 1001 before itself: this should be a no-op
            # but it should still output the queue event
            self.model.move(sal_index=1001,
                            location=SALPY_ScriptQueue.add_After,
                            location_sal_index=1001)
            await self.assert_next_queue(sal_indices=[1002, 1001, 1000])

            # try some incorrect moves
            with self.assertRaises(ValueError):
                self.model.move(sal_index=1234,  # no such script
                                location=SALPY_ScriptQueue.add_Last,
                                location_sal_index=0)
            await self.assert_next_queue(sal_indices=[1002, 1001, 1000])

            with self.assertRaises(ValueError):
                self.model.move(sal_index=1001,
                                location=21,  # no such location
                                location_sal_index=0)
            await self.assert_next_queue(sal_indices=[1002, 1001, 1000])

            with self.assertRaises(ValueError):
                self.model.move(sal_index=1001,
                                location=SALPY_ScriptQueue.add_Before,
                                location_sal_index=1234)  # no such script)
            await self.assert_next_queue(sal_indices=[1002, 1001, 1000])

            # try incorrect index and the same "before" locationSalIndex
            with self.assertRaises(ValueError):
                self.model.move(sal_index=1234,
                                location=SALPY_ScriptQueue.add_Before,
                                location_sal_index=1234)
            await self.assert_next_queue(sal_indices=[1002, 1001, 1000])

            # try incorrect index and the same "after" locationSalIndex
            with self.assertRaises(ValueError):
                self.model.move(sal_index=1234,
                                location=SALPY_ScriptQueue.add_After,
                                location_sal_index=1234)
            await self.assert_next_queue(sal_indices=[1002, 1001, 1000])

            await self.model.wait_terminate_all()

        asyncio.get_event_loop().run_until_complete(doit())

    def test_remove(self):
        async def doit():
            await self.assert_next_queue(enabled=False, running=True)

            self.model.enabled = True
            await self.assert_next_queue(enabled=True, running=True)

            # pause the queue so we know what to expect of queue state
            self.model.running = False
            await self.assert_next_queue(running=False)

            for i in range(3):
                script_info = ts_scriptqueue.ScriptInfo(
                    index=self.model.next_sal_index,
                    cmd_id=i + 10,  # arbitrary
                    is_standard=False,
                    path=os.path.join("subdir", "script6"),
                    config="wait_time: 0.5" if i == 1 else "",
                    descr=f"test_requeue {i}",
                )
                await self.model.add(script_info=script_info,
                                     location=SALPY_ScriptQueue.add_Last,
                                     location_sal_index=0)

            await self.assert_next_queue(sal_indices=[1000, 1001, 1002])

            await self.wait_runnable(1000, 1001)

            # resume the queue and wait for the second script to start
            # running. At that point we have one running script, one in
            # history and one on the queue. Remove the ones not done.
            self.model.running = True
            await self.assert_next_queue(running=True, current_sal_index=1000,
                                         sal_indices=[1001, 1002], past_sal_indices=[], wait=True)
            await self.assert_next_queue(running=True, current_sal_index=1001,
                                         sal_indices=[1002], past_sal_indices=[1000], wait=True)

            self.model.remove(sal_index=1002)
            await self.assert_next_queue(running=True, current_sal_index=1001,
                                         sal_indices=[], past_sal_indices=[1000], wait=True)

            self.model.remove(sal_index=1001)
            await self.assert_next_queue(running=True, current_sal_index=0,
                                         sal_indices=[], past_sal_indices=[1000], wait=True)

            # try removing a script that doesn't exist
            with self.assertRaises(ValueError):
                self.model.remove(sal_index=333)

        asyncio.get_event_loop().run_until_complete(doit())

    def test_requeue(self):
        """Test requeue
        """
        async def doit():
            await self.assert_next_queue(enabled=False, running=True)

            self.model.enabled = True
            await self.assert_next_queue(enabled=True, running=True)

            # pause the queue so we know what to expect of queue state
            self.model.running = False
            await self.assert_next_queue(running=False)

            info_list = [
                ts_scriptqueue.ScriptInfo(
                    index=self.model.next_sal_index,
                    cmd_id=i + 10,  # arbitrary
                    is_standard=False,
                    path=os.path.join("subdir", "script6"),
                    config="wait_time: 1" if i == 1 else "",
                    descr=f"test_requeue {i}",
                ) for i in range(3)]

            # add the scripts to the end of the queue
            for info in info_list:
                print(f"add {info}")
                await self.model.add(script_info=info,
                                     location=SALPY_ScriptQueue.add_Last,
                                     location_sal_index=0)

            await self.assert_next_queue(sal_indices=[1000, 1001, 1002])

            await self.wait_runnable(1000, 1001)

            # resume the queue and wait for the second script to start
            # running. At that point we have one running script,
            # one in history and one on the queue; requeue each.
            self.model.running = True
            await self.assert_next_queue(running=True, current_sal_index=1000,
                                         sal_indices=[1001, 1002], past_sal_indices=[], wait=True)
            await self.assert_next_queue(running=True, current_sal_index=1001,
                                         sal_indices=[1002], past_sal_indices=[1000], wait=True)

            rq1001 = await self.model.requeue(sal_index=1001,
                                              cmd_id=32,
                                              location=SALPY_ScriptQueue.add_First,
                                              location_sal_index=0)
            await self.assert_next_queue(running=True, current_sal_index=1001,
                                         sal_indices=[1003, 1002], past_sal_indices=[1000])

            rq1002 = await self.model.requeue(sal_index=1002,
                                              cmd_id=30,
                                              location=SALPY_ScriptQueue.add_After,
                                              location_sal_index=1003)
            await self.assert_next_queue(running=True, current_sal_index=1001,
                                         sal_indices=[1003, 1004, 1002], past_sal_indices=[1000])

            rq1000 = await self.model.requeue(sal_index=1000,
                                              cmd_id=31,
                                              location=SALPY_ScriptQueue.add_Before,
                                              location_sal_index=1003)
            await self.assert_next_queue(running=True, current_sal_index=1001, wait=True,
                                         sal_indices=[1005, 1003, 1004, 1002], past_sal_indices=[1000])

            # now pause the queue and wait for all remaining scripts to be runnable, then resume
            self.model.running = False
            await self.wait_runnable(1002, 1003, 1004, 1005)
            self.model.running = True
            await self.assert_next_queue(running=True, current_sal_index=1005, wait=True,
                                         sal_indices=[1003, 1004, 1002], past_sal_indices=[1001, 1000])
            await self.assert_next_queue(running=True, current_sal_index=1003, wait=True,
                                         sal_indices=[1004, 1002], past_sal_indices=[1005, 1001, 1000])
            await self.assert_next_queue(running=True, current_sal_index=1004, wait=True,
                                         sal_indices=[1002], past_sal_indices=[1003, 1005, 1001, 1000])
            await self.assert_next_queue(running=True, current_sal_index=1002, wait=True,
                                         sal_indices=[], past_sal_indices=[1004, 1003, 1005, 1001, 1000])
            await self.assert_next_queue(running=True, current_sal_index=0, wait=True,
                                         sal_indices=[],
                                         past_sal_indices=[1002, 1004, 1003, 1005, 1001, 1000])

            requeue_info_list = [rq1000, rq1001, rq1002]
            for requeue_info, info in zip(requeue_info_list, info_list):
                self.assert_script_info_equal(requeue_info, info, is_requeue=True)

        asyncio.get_event_loop().run_until_complete(doit())

    def test_resume_before_first_script_runnable(self):
        async def doit():
            await self.assert_next_queue(enabled=False, running=True)

            self.model.enabled = True
            await self.assert_next_queue(enabled=True, running=True)

            # pause the queue so we know what to expect of queue state
            self.model.running = False
            await self.assert_next_queue(running=False)

            script_info = ts_scriptqueue.ScriptInfo(
                index=self.model.next_sal_index,
                cmd_id=25,  # arbitrary
                is_standard=False,
                path=os.path.join("subdir", "script6"),
                config="wait_time: 0.1",
                descr="test_resume_before_first_script_runnable",
            )
            await self.model.add(script_info=script_info,
                                 location=SALPY_ScriptQueue.add_Last,
                                 location_sal_index=0)
            await self.assert_next_queue(sal_indices=[1000])

            self.model.running = True
            await self.assert_next_queue(running=True, current_sal_index=0,
                                         sal_indices=[1000], past_sal_indices=[], wait=True)

            await self.assert_next_queue(running=True, current_sal_index=1000,
                                         sal_indices=[], past_sal_indices=[], wait=True)

            await self.assert_next_queue(running=True, current_sal_index=0,
                                         sal_indices=[], past_sal_indices=[1000], wait=True)

        asyncio.get_event_loop().run_until_complete(doit())

    def test_run_immediately(self):
        async def doit():
            await self.assert_next_queue(enabled=False, running=True)

            self.model.enabled = True
            await self.assert_next_queue(enabled=True, running=True)

            script_info = ts_scriptqueue.ScriptInfo(
                index=self.model.next_sal_index,
                cmd_id=25,  # arbitrary
                is_standard=False,
                path=os.path.join("subdir", "script6"),
                config="",
                descr="test_run_immediately",
            )
            await self.model.add(script_info=script_info,
                                 location=SALPY_ScriptQueue.add_Last,
                                 location_sal_index=0)

            await self.assert_next_queue(running=True, current_sal_index=0,
                                         sal_indices=[1000], past_sal_indices=[], wait=True)

            await self.assert_next_queue(running=True, current_sal_index=1000,
                                         sal_indices=[], past_sal_indices=[], wait=True)

            await self.assert_next_queue(running=True, current_sal_index=0,
                                         sal_indices=[], past_sal_indices=[1000], wait=True)

        asyncio.get_event_loop().run_until_complete(doit())

    async def wait_runnable(self, *indices):
        """Wait for the specified scripts to be runnable.

        Call this before running the queue if you want the queue data
        to be predictable; otherwise the queue may start up with
        no script running.
        """
        for sal_index in indices:
            print(f"waiting for script {sal_index} to be runnable")
            script_info = self.model.get_script_info(sal_index)
            await asyncio.wait_for(script_info.start_task, 20)
            await asyncio.wait_for(script_info.config_task, 20)
            # this will fail if the script was already run
            self.assertTrue(script_info.runnable)


if __name__ == "__main__":
    unittest.main()
