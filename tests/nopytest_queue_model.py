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
import time
import unittest
import warnings

import SALPY_Script
import SALPY_ScriptQueue
from lsst.ts import salobj
from lsst.ts import scriptqueue


def _min_sal_index_generator():
    min_sal_index = 1000
    while True:
        yield min_sal_index
        min_sal_index += 100


make_min_sal_index = _min_sal_index_generator()


class QueueModelTestCase(unittest.TestCase):
    def setUp(self):
        self.t0 = time.time()
        self.min_sal_index = next(make_min_sal_index)
        salobj.set_random_lsst_dds_domain()
        self.datadir = os.path.abspath(os.path.join(os.path.dirname(__file__), "data"))
        self.standardpath = os.path.join(self.datadir, "standard")
        self.externalpath = os.path.join(self.datadir, "external")
        self.model = scriptqueue.QueueModel(standardpath=self.standardpath,
                                            externalpath=self.externalpath,
                                            queue_callback=self.queue_callback,
                                            script_callback=self.script_callback,
                                            min_sal_index=self.min_sal_index,
                                            verbose=True)
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
            await asyncio.wait_for(self.queue_task, 60)
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

    def make_add_kwargs(self, location=SALPY_ScriptQueue.add_Last, location_sal_index=0,
                        is_standard=False, path=None, config="wait_time: 0.1"):
        """Make keyword arguments for QueueModel.add.

        Parameters
        ----------
        location : `int` (optional)
            One of SALPY_ScriptQueue.add_First, Last, Before or After.
        location_sal_index : `int` (optional)
            SAL index of script that ``location`` is relative to.
        is_standard : `bool`
            Is this a standard (True) or external (False) script?
        path : `str`, `bytes` or `os.PathLike` (optional)
            Path to script, relative to standard or external root dir;
            defaults to "subdir/script6".
        config : `str` (optional)
            Configuration data as a YAML encoded string.
        """
        if path is None:
            path = os.path.join("subdir", "script6")
        return dict(
            script_info=self.make_script_info(is_standard=is_standard, path=path, config=config),
            location=location,
            location_sal_index=location_sal_index,
        )

    def make_script_info(self, is_standard=False, path=None, config="wait_time: 0.1"):
        """Make a `ScriptInfo`.

        Parameters
        ----------
        is_standard : `bool`
            Is this a standard (True) or external (False) script?
        path : `str`, `bytes` or `os.PathLike` (optional)
            Path to script, relative to standard or external root dir;
            defaults to "subdir/script6".
        config : `str` (optional)
            Configuration data as a YAML encoded string.
        """
        sal_index = self.model.next_sal_index
        return scriptqueue.ScriptInfo(
            index=sal_index,
            cmd_id=sal_index*2,  # arbitrary
            is_standard=is_standard,
            path=path,
            config=config,
            descr=f"{sal_index}",
            verbose=True,
        )

    def queue_callback(self):
        dt = time.time() - self.t0
        print(f"queue_callback(): enabled={self.model.enabled}; "
              f"running={self.model.running}; "
              f"current={self.model.current_index}; "
              f"queue={[info.index for info in self.model.queue]}; "
              f"history={[info.index for info in self.model.history]}; "
              f"elapsed time={dt:0.1f}")
        if not self.queue_task.done():
            self.queue_task.set_result(None)

    def script_callback(self, script_info):
        dt = time.time() - self.t0
        print(f"script_callback for {script_info.index} at {time.time():0.1f}: "
              f"started={script_info.start_task.done()}; "
              f"configured={script_info.configured}; "
              f"process_done={script_info.process_done}; "
              f"terminated={script_info.terminated}; "
              f"script_state={script_info.script_state}; "
              f"elapsed time={dt:0.1f}")

    def test_add(self):
        """Test add."""
        async def doit():
            await self.assert_next_queue(enabled=False, running=True)

            self.model.enable = True
            await self.assert_next_queue(enabled=True, running=True)

            # pause the queue so we know what to expect of queue state
            self.model.running = False
            await self.assert_next_queue(running=False)

            # add script i0; queue is empty, so location is irrelevant
            add_kwargs = self.make_add_kwargs(location=SALPY_ScriptQueue.add_Last)
            i0 = add_kwargs["script_info"].index
            await asyncio.wait_for(self.model.add(**add_kwargs), timeout=60)
            await self.assert_next_queue(sal_indices=[i0])

            # add script i0+1 last: test add last
            add_kwargs = self.make_add_kwargs(location=SALPY_ScriptQueue.add_Last)
            await asyncio.wait_for(self.model.add(**add_kwargs), timeout=60)
            await self.assert_next_queue(sal_indices=[i0, i0+1])

            # add script i0+2 first: test add first
            add_kwargs = self.make_add_kwargs(location=SALPY_ScriptQueue.add_First)
            await asyncio.wait_for(self.model.add(**add_kwargs), timeout=60)
            await self.assert_next_queue(sal_indices=[i0+2, i0, i0+1])

            # add script i0+3 after i0+1: test add after last
            add_kwargs = self.make_add_kwargs(location=SALPY_ScriptQueue.add_After,
                                              location_sal_index=i0+1)
            await asyncio.wait_for(self.model.add(**add_kwargs), timeout=60)
            await self.assert_next_queue(sal_indices=[i0+2, i0, i0+1, i0+3])

            # add script i0+4 after i0+2: test add after not-last
            add_kwargs = self.make_add_kwargs(location=SALPY_ScriptQueue.add_After,
                                              location_sal_index=i0+2)
            await asyncio.wait_for(self.model.add(**add_kwargs), timeout=60)
            await self.assert_next_queue(sal_indices=[i0+2, i0+4, i0, i0+1, i0+3])

            # add script i0+5 before i0+2: test add before first
            add_kwargs = self.make_add_kwargs(location=SALPY_ScriptQueue.add_Before,
                                              location_sal_index=i0+2)
            await asyncio.wait_for(self.model.add(**add_kwargs), timeout=60)
            await self.assert_next_queue(sal_indices=[i0+5, i0+2, i0+4, i0, i0+1, i0+3])

            # add script i0+6 before i0: test add before not first
            add_kwargs = self.make_add_kwargs(location=SALPY_ScriptQueue.add_Before,
                                              location_sal_index=i0)
            await asyncio.wait_for(self.model.add(**add_kwargs), timeout=60)
            await self.assert_next_queue(sal_indices=[i0+5, i0+2, i0+4, i0+6, i0, i0+1, i0+3])

            # try some failed adds
            # incorrect path
            add_kwargs = self.make_add_kwargs(location=SALPY_ScriptQueue.add_First)
            add_kwargs["script_info"].path = "bogus_script_name"
            with self.assertRaises(ValueError):
                await asyncio.wait_for(self.model.add(**add_kwargs), timeout=60)
            await self.assert_next_queue(sal_indices=[i0+5, i0+2, i0+4, i0+6, i0, i0+1, i0+3])

            # incorrect location
            add_kwargs = self.make_add_kwargs(location=25)
            with self.assertRaises(ValueError):
                await asyncio.wait_for(self.model.add(**add_kwargs), timeout=60)
            await self.assert_next_queue(sal_indices=[i0+5, i0+2, i0+4, i0+6, i0, i0+1, i0+3])

            # incorrect location_sal_index
            add_kwargs = self.make_add_kwargs(location=SALPY_ScriptQueue.add_After,
                                              location_sal_index=4321)
            with self.assertRaises(ValueError):
                await asyncio.wait_for(self.model.add(**add_kwargs), timeout=60)
            await self.assert_next_queue(sal_indices=[i0+5, i0+2, i0+4, i0+6, i0, i0+1, i0+3])

            # stop a few scripts
            await asyncio.wait_for(
                self.model.stop_scripts(sal_indices=[i0+6, i0+5, i0, i0+4], terminate=True), timeout=5)
            await self.assert_next_queue(sal_indices=[i0+2, i0+1, i0+3])

            # make sure all scripts are runnable before starting the queue
            # so the queue data is more predictable (otherwise the queue
            # may start up with no script running)
            await self.wait_runnable(i0+1, i0+2, i0+3)

            # disable the queue, then set running True and check that
            # the queue does not start running until we enable it again
            self.model.enabled = False
            await self.assert_next_queue(enabled=False, running=False, sal_indices=[i0+2, i0+1, i0+3])

            self.model.running = True
            await self.assert_next_queue(enabled=False, running=True, sal_indices=[i0+2, i0+1, i0+3])

            self.model.enabled = True
            await self.assert_next_queue(running=True, current_sal_index=i0+2,
                                         sal_indices=[i0+1, i0+3], past_sal_indices=[], wait=True)

            await self.assert_next_queue(running=True, current_sal_index=i0+1,
                                         sal_indices=[i0+3], past_sal_indices=[i0+2], wait=True)

            await self.assert_next_queue(running=True, current_sal_index=i0+3,
                                         sal_indices=[], past_sal_indices=[i0+1, i0+2], wait=True)

            await self.assert_next_queue(running=True, current_sal_index=0,
                                         sal_indices=[], past_sal_indices=[i0+3, i0+1, i0+2], wait=True)

        asyncio.get_event_loop().run_until_complete(doit())

    def test_add_badconfig(self):
        """Test adding a script with invalid configuration.
        """
        async def doit():
            await self.assert_next_queue(enabled=False, running=True)

            self.model.enable = True
            await self.assert_next_queue(enabled=True, running=True)

            # add script i0 with invalid config
            add_kwargs = self.make_add_kwargs(config="invalid: True")
            script0 = add_kwargs["script_info"]
            i0 = script0.index
            add_coro = asyncio.wait_for(self.model.add(**add_kwargs), timeout=60)
            assert_coro = self.assert_next_queue(sal_indices=[i0], running=True, wait=True)
            await asyncio.gather(add_coro, assert_coro)
            await self.assert_next_queue(current_sal_index=0, sal_indices=[], past_sal_indices=[],
                                         running=True, wait=True)
            await script0.process_task
            self.assertTrue(script0.configure_failed)
            self.assertFalse(script0.configured)
            self.assertEqual(script0.process_done, True)
            self.assertEqual(script0.process_state, SALPY_ScriptQueue.script_ConfigureFailed)

        asyncio.get_event_loop().run_until_complete(doit())

    def check_add_then_stop_script(self, terminate):
        """Test adding a script immediately followed by stoppping it.
        """
        async def doit():
            await self.assert_next_queue(enabled=False, running=True)

            self.model.enable = True
            await self.assert_next_queue(enabled=True, running=True)

            # add script i0
            add_kwargs = self.make_add_kwargs()
            script0 = add_kwargs["script_info"]
            i0 = script0.index
            add_task = asyncio.ensure_future(asyncio.wait_for(self.model.add(**add_kwargs), timeout=60))
            await self.assert_next_queue(sal_indices=[i0], running=True, wait=True)
            await self.model.stop_scripts(sal_indices=[i0], terminate=terminate)
            await self.assert_next_queue(sal_indices=[], running=True, wait=True)
            with self.assertRaises(asyncio.CancelledError):
                await add_task
            self.assertFalse(script0.process_done)
            self.assertTrue(script0.terminated)
            self.assertFalse(script0.configure_failed)
            self.assertFalse(script0.configured)
            self.assertEqual(script0.process_state, SALPY_ScriptQueue.script_Terminated)

        asyncio.get_event_loop().run_until_complete(doit())

    def test_add_then_stop_script(self):
        self.check_add_then_stop_script(terminate=False)

    def test_add_then_terminate_script(self):
        self.check_add_then_stop_script(terminate=True)

    def test_constructor_errors(self):
        nonexistentpath = os.path.join(self.datadir, "garbage")
        with self.assertRaises(ValueError):
            scriptqueue.QueueModel(standardpath=self.standardpath, externalpath=nonexistentpath)
        with self.assertRaises(ValueError):
            scriptqueue.QueueModel(standardpath=nonexistentpath, externalpath=self.externalpath)
        with self.assertRaises(ValueError):
            scriptqueue.QueueModel(standardpath=nonexistentpath, externalpath=nonexistentpath)

    def test_get_script_info(self):
        async def doit():
            await self.assert_next_queue(enabled=False, running=True)

            self.model.enabled = True
            await self.assert_next_queue(enabled=True, running=True)

            # pause the queue so we know what to expect of queue state
            self.model.running = False
            await self.assert_next_queue(running=False)

            info_dict = {}
            i0 = None
            for i in range(3):

                script_info = self.make_script_info(is_standard=False,
                                                    path=os.path.join("subdir", "script6"),
                                                    config="wait_time: 0.5" if i == 1 else "")
                if i0 is None:
                    i0 = script_info.index
                info_dict[script_info.index] = script_info
                await asyncio.wait_for(self.model.add(script_info=script_info,
                                                      location=SALPY_ScriptQueue.add_Last,
                                                      location_sal_index=0), timeout=60)

            await self.assert_next_queue(sal_indices=[i0, i0+1, i0+2])

            await self.wait_runnable(i0, i0+1, i0+2)

            # resume the queue and wait for the second script to start
            # running. At that point we have one running script, one in
            # history and one on the queue. Run get_script_info on each.
            self.model.running = True
            await self.assert_next_queue(running=True, current_sal_index=i0,
                                         sal_indices=[i0+1, i0+2], past_sal_indices=[], wait=True)
            await self.assert_next_queue(running=True, current_sal_index=i0+1,
                                         sal_indices=[i0+2], past_sal_indices=[i0], wait=True)

            info2 = self.model.get_script_info(sal_index=i0+2, search_history=False)
            self.assert_script_info_equal(info2, info_dict[i0+2])
            with self.assertRaises(ValueError):
                self.model.get_script_info(sal_index=i0, search_history=False)
            for sal_index, expected_script_info in info_dict.items():
                script_info = self.model.get_script_info(sal_index=sal_index, search_history=True)
                self.assert_script_info_equal(script_info, expected_script_info)

            await self.model.wait_terminate_all(timeout=10)

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

            # queue scripts i0, i0+1 and i0+2
            sal_indices = []
            for i in range(3):
                script_info = self.make_script_info(is_standard=True,
                                                    path=os.path.join("subdir", "script3"))
                sal_indices.append(script_info.index)
                await asyncio.wait_for(self.model.add(script_info=script_info,
                                                      location=SALPY_ScriptQueue.add_Last,
                                                      location_sal_index=0), timeout=60)
                await self.assert_next_queue(sal_indices=sal_indices)
            i0 = sal_indices[0]

            # move i0+2 first
            self.model.move(sal_index=i0+2,
                            location=SALPY_ScriptQueue.add_First,
                            location_sal_index=0)
            await self.assert_next_queue(sal_indices=[i0+2, i0, i0+1])

            # move i0+2 first again (should be a no-op)
            self.model.move(sal_index=i0+2,
                            location=SALPY_ScriptQueue.add_First,
                            location_sal_index=0)
            await self.assert_next_queue(sal_indices=[i0+2, i0, i0+1])

            # move i0 last
            self.model.move(sal_index=i0,
                            location=SALPY_ScriptQueue.add_Last,
                            location_sal_index=0)
            await self.assert_next_queue(sal_indices=[i0+2, i0+1, i0])

            # move i0 last again (should be a no-op)
            self.model.move(sal_index=i0,
                            location=SALPY_ScriptQueue.add_Last,
                            location_sal_index=0)
            await self.assert_next_queue(sal_indices=[i0+2, i0+1, i0])

            # move i0 before i0+2: before first
            self.model.move(sal_index=i0,
                            location=SALPY_ScriptQueue.add_Before,
                            location_sal_index=i0+2)
            await self.assert_next_queue(sal_indices=[i0, i0+2, i0+1])

            # move i0+1 before i0+2: before not-first
            self.model.move(sal_index=i0+1,
                            location=SALPY_ScriptQueue.add_Before,
                            location_sal_index=i0+2)
            await self.assert_next_queue(sal_indices=[i0, i0+1, i0+2])

            # move i0 after i0+2: after last
            self.model.move(sal_index=i0,
                            location=SALPY_ScriptQueue.add_After,
                            location_sal_index=i0+2)
            await self.assert_next_queue(sal_indices=[i0+1, i0+2, i0])

            # move i0+1 after i0+2: after not-last
            self.model.move(sal_index=i0+1,
                            location=SALPY_ScriptQueue.add_After,
                            location_sal_index=i0+2)
            await self.assert_next_queue(sal_indices=[i0+2, i0+1, i0])

            # move i0 after itself: this should be a no-op
            # but it should still output the queue event
            self.model.move(sal_index=i0,
                            location=SALPY_ScriptQueue.add_After,
                            location_sal_index=i0)
            await self.assert_next_queue(sal_indices=[i0+2, i0+1, i0])

            # move i0+1 before itself: this should be a no-op
            # but it should still output the queue event
            self.model.move(sal_index=i0+1,
                            location=SALPY_ScriptQueue.add_After,
                            location_sal_index=i0+1)
            await self.assert_next_queue(sal_indices=[i0+2, i0+1, i0])

            # try some incorrect moves
            with self.assertRaises(ValueError):
                self.model.move(sal_index=1234,  # no such script
                                location=SALPY_ScriptQueue.add_Last,
                                location_sal_index=0)
            await self.assert_next_queue(sal_indices=[i0+2, i0+1, i0])

            with self.assertRaises(ValueError):
                self.model.move(sal_index=i0+1,
                                location=21,  # no such location
                                location_sal_index=0)
            await self.assert_next_queue(sal_indices=[i0+2, i0+1, i0])

            with self.assertRaises(ValueError):
                self.model.move(sal_index=i0+1,
                                location=SALPY_ScriptQueue.add_Before,
                                location_sal_index=1234)  # no such script)
            await self.assert_next_queue(sal_indices=[i0+2, i0+1, i0])

            # try incorrect index and the same "before" locationSalIndex
            with self.assertRaises(ValueError):
                self.model.move(sal_index=1234,
                                location=SALPY_ScriptQueue.add_Before,
                                location_sal_index=1234)
            await self.assert_next_queue(sal_indices=[i0+2, i0+1, i0])

            # try incorrect index and the same "after" locationSalIndex
            with self.assertRaises(ValueError):
                self.model.move(sal_index=1234,
                                location=SALPY_ScriptQueue.add_After,
                                location_sal_index=1234)
            await self.assert_next_queue(sal_indices=[i0+2, i0+1, i0])

            await self.model.wait_terminate_all(timeout=10)

        asyncio.get_event_loop().run_until_complete(doit())

    def test_pause_on_failure(self):
        """Test that a failed script pauses the queue.
        """
        async def doit():
            await self.assert_next_queue(enabled=False, running=True)

            self.model.enable = True
            await self.assert_next_queue(enabled=True, running=True)

            # pause the queue so we know what to expect of queue state
            self.model.running = False
            await self.assert_next_queue(running=False)

            # add scripts i0, i0+1, i0+2; i0+1 fails
            add_kwargs = self.make_add_kwargs()
            i0 = add_kwargs["script_info"].index
            await asyncio.wait_for(self.model.add(**add_kwargs), timeout=60)
            await self.assert_next_queue(sal_indices=[i0])

            add_kwargs = self.make_add_kwargs(config="wait_time: 0.1\nfail_run: True")
            await asyncio.wait_for(self.model.add(**add_kwargs), timeout=60)
            await self.assert_next_queue(sal_indices=[i0, i0+1])

            add_kwargs = self.make_add_kwargs()
            await asyncio.wait_for(self.model.add(**add_kwargs), timeout=60)
            await self.assert_next_queue(sal_indices=[i0, i0+1, i0+2])

            # make sure all scripts are runnable before starting the queue
            # so the queue data is more predictable (otherwise the queue
            # may start up with no script running)
            await self.wait_runnable(i0, i0+1, i0+2)

            # start the queue; it should pause when i0+1 fails
            self.model.running = True
            await self.assert_next_queue(running=True, current_sal_index=i0,
                                         sal_indices=[i0+1, i0+2], past_sal_indices=[], wait=True)

            await self.assert_next_queue(running=True, current_sal_index=i0+1,
                                         sal_indices=[i0+2], past_sal_indices=[i0], wait=True)

            await self.assert_next_queue(running=False, current_sal_index=i0+1,
                                         sal_indices=[i0+2], past_sal_indices=[i0], wait=True)

            # assert that the process return code is positive for failure
            # and that the final script state Failed is sent and recorded
            script_info = self.model.get_script_info(i0+1, search_history=False)
            self.assertTrue(script_info.process_done)
            self.assertGreater(script_info.process.returncode, 0)
            self.assertEqual(script_info.script_state, SALPY_Script.state_Failed)

            # resume the queue; this should move i0+1 to history and keep going
            self.model.running = True
            await self.assert_next_queue(running=True, current_sal_index=i0+2,
                                         sal_indices=[], past_sal_indices=[i0+1, i0], wait=True)

            await self.assert_next_queue(running=True, current_sal_index=0,
                                         sal_indices=[], past_sal_indices=[i0+2, i0+1, i0], wait=True)

            # assert that the process return code is 0 for success
            # and that the final script state Done is sent and recorded
            script_info = self.model.get_script_info(i0+2, search_history=True)
            self.assertTrue(script_info.process_done)
            self.assertEqual(script_info.process.returncode, 0)
            self.assertEqual(script_info.script_state, SALPY_Script.state_Done)

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
                scriptqueue.ScriptInfo(
                    index=self.model.next_sal_index,
                    cmd_id=i + 10,  # arbitrary
                    is_standard=False,
                    path=os.path.join("subdir", "script6"),
                    config="wait_time: 1" if i == 1 else "",
                    descr=f"test_requeue {i}",
                    verbose=True,
                ) for i in range(3)]
            i0 = info_list[0].index

            # add the scripts to the end of the queue
            for info in info_list:
                await asyncio.wait_for(self.model.add(script_info=info,
                                                      location=SALPY_ScriptQueue.add_Last,
                                                      location_sal_index=0), timeout=60)

            await self.assert_next_queue(sal_indices=[i0, i0+1, i0+2])

            await self.wait_runnable(i0, i0+1)

            # resume the queue and wait for the second script to start
            # running. At that point we have one running script,
            # one in history and one on the queue; requeue each.
            self.model.running = True
            await self.assert_next_queue(running=True, current_sal_index=i0,
                                         sal_indices=[i0+1, i0+2], past_sal_indices=[], wait=True)
            await self.assert_next_queue(running=True, current_sal_index=i0+1,
                                         sal_indices=[i0+2], past_sal_indices=[i0], wait=True)

            rq1 = await asyncio.wait_for(self.model.requeue(sal_index=i0+1,
                                                            cmd_id=32,  # arbitrary but unique
                                                            location=SALPY_ScriptQueue.add_First,
                                                            location_sal_index=0), timeout=60)
            await self.assert_next_queue(running=True, current_sal_index=i0+1,
                                         sal_indices=[i0+3, i0+2], past_sal_indices=[i0])

            rq2 = await asyncio.wait_for(self.model.requeue(sal_index=i0+2,
                                                            cmd_id=30,
                                                            location=SALPY_ScriptQueue.add_After,
                                                            location_sal_index=i0+3), timeout=60)
            await self.assert_next_queue(running=True, current_sal_index=i0+1,
                                         sal_indices=[i0+3, i0+4, i0+2], past_sal_indices=[i0])

            rq0 = await asyncio.wait_for(self.model.requeue(sal_index=i0,
                                                            cmd_id=31,
                                                            location=SALPY_ScriptQueue.add_Before,
                                                            location_sal_index=i0+3), timeout=60)
            await self.assert_next_queue(running=True, current_sal_index=i0+1, wait=True,
                                         sal_indices=[i0+5, i0+3, i0+4, i0+2], past_sal_indices=[i0])

            # now pause the queue and wait for all remaining scripts to be runnable, then resume
            self.model.running = False
            await self.wait_runnable(i0+2, i0+3, i0+4, i0+5)
            self.model.running = True
            await self.assert_next_queue(running=True, current_sal_index=i0+5, wait=True,
                                         sal_indices=[i0+3, i0+4, i0+2], past_sal_indices=[i0+1, i0])
            await self.assert_next_queue(running=True, current_sal_index=i0+3, wait=True,
                                         sal_indices=[i0+4, i0+2], past_sal_indices=[i0+5, i0+1, i0])
            await self.assert_next_queue(running=True, current_sal_index=i0+4, wait=True,
                                         sal_indices=[i0+2], past_sal_indices=[i0+3, i0+5, i0+1, i0])
            await self.assert_next_queue(running=True, current_sal_index=i0+2, wait=True,
                                         sal_indices=[], past_sal_indices=[i0+4, i0+3, i0+5, i0+1, i0])
            await self.assert_next_queue(running=True, current_sal_index=0, wait=True,
                                         sal_indices=[],
                                         past_sal_indices=[i0+2, i0+4, i0+3, i0+5, i0+1, i0])

            requeue_info_list = [rq0, rq1, rq2]
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

            info0 = self.make_script_info(is_standard=False,
                                          path=os.path.join("subdir", "script6"),
                                          config="wait_time: 0.1")
            i0 = info0.index
            await asyncio.wait_for(self.model.add(script_info=info0,
                                                  location=SALPY_ScriptQueue.add_Last,
                                                  location_sal_index=0), timeout=60)
            await self.assert_next_queue(sal_indices=[i0])

            self.model.running = True
            await self.assert_next_queue(running=True, current_sal_index=0,
                                         sal_indices=[i0], past_sal_indices=[], wait=True)

            await self.assert_next_queue(running=True, current_sal_index=i0,
                                         sal_indices=[], past_sal_indices=[], wait=True)

            await self.assert_next_queue(running=True, current_sal_index=0,
                                         sal_indices=[], past_sal_indices=[i0], wait=True)

        asyncio.get_event_loop().run_until_complete(doit())

    def test_run_immediately(self):
        async def doit():
            await self.assert_next_queue(enabled=False, running=True)

            self.model.enabled = True
            await self.assert_next_queue(enabled=True, running=True)

            info0 = self.make_script_info(is_standard=False,
                                          path=os.path.join("subdir", "script6"),
                                          config="")
            i0 = info0.index
            await asyncio.wait_for(self.model.add(script_info=info0,
                                                  location=SALPY_ScriptQueue.add_Last,
                                                  location_sal_index=0), timeout=60)

            await self.assert_next_queue(running=True, current_sal_index=0,
                                         sal_indices=[i0], past_sal_indices=[], wait=True)

            await self.assert_next_queue(running=True, current_sal_index=i0,
                                         sal_indices=[], past_sal_indices=[], wait=True)

            await self.assert_next_queue(running=True, current_sal_index=0,
                                         sal_indices=[], past_sal_indices=[i0], wait=True)

        asyncio.get_event_loop().run_until_complete(doit())

    async def check_stop_scripts(self, terminate):
        await self.assert_next_queue(enabled=False, running=True)

        self.model.enabled = True
        await self.assert_next_queue(enabled=True, running=True)

        # pause the queue so we know what to expect of queue state
        self.model.running = False
        await self.assert_next_queue(running=False)

        info_dict = dict()
        i0 = None
        for i in range(4):
            script_info = self.make_script_info(is_standard=False,
                                                path=os.path.join("subdir", "script6"),
                                                config="wait_time: 10" if i == 1 else "")
            if i0 is None:
                i0 = script_info.index
            info_dict[script_info.index] = script_info
            await asyncio.wait_for(self.model.add(script_info=script_info,
                                                  location=SALPY_ScriptQueue.add_Last,
                                                  location_sal_index=0), timeout=60)

        await self.assert_next_queue(sal_indices=[i0, i0+1, i0+2, i0+3])

        await self.wait_runnable(i0+1, i0+2, i0+3, i0)

        # resume the queue and wait for the second script to start
        # running. At that point we have one running script, one in
        # history and one on the queue. Remove the ones not done.
        self.model.running = True
        await self.assert_next_queue(running=True, current_sal_index=i0,
                                     sal_indices=[i0+1, i0+2, i0+3], past_sal_indices=[], wait=True)
        await self.assert_next_queue(running=True, current_sal_index=i0+1,
                                     sal_indices=[i0+2, i0+3], past_sal_indices=[i0], wait=True)

        # wait for script i0+1 to actally start running
        await self.wait_running(i0+1)

        # stop the current script and a queued script
        await asyncio.wait_for(self.model.stop_scripts(sal_indices=[i0+1, i0+3], terminate=terminate),
                               timeout=5)

        script_info1 = info_dict[i0+1]
        script_info2 = info_dict[i0+2]
        script_info3 = info_dict[i0+3]

        await asyncio.wait_for(asyncio.gather(script_info1.process_task,
                                              script_info2.process_task,
                                              script_info3.process_task), timeout=10)
        # i0 and i0+2 both ran; i0+1 was stopped while it was running
        # and i0+3 was stopped while on the queue
        # (so it also goes on the history)

        # script i0+1 was running, so it was stopped gently
        # if terminate False, else terminated abruptly
        self.assertTrue(script_info1.process_done)
        self.assertFalse(script_info1.failed)
        self.assertFalse(script_info1.running)
        if terminate:
            self.assertTrue(script_info1.terminated)
        else:
            self.assertFalse(script_info1.terminated)
            self.assertEqual(script_info1.script_state, SALPY_Script.state_Stopped)
            self.assertEqual(script_info1.process_state, SALPY_ScriptQueue.script_Done)

        # script i0+2 ran normally
        self.assertTrue(script_info2.process_done)
        self.assertFalse(script_info2.failed)
        self.assertFalse(script_info2.running)
        self.assertFalse(script_info2.terminated)
        self.assertEqual(script_info2.process_state, SALPY_ScriptQueue.script_Done)
        self.assertEqual(script_info2.script_state, SALPY_Script.state_Done)

        # script i0+3 was stopped while queued, so it was terminated,
        # regardless of the `terminate` argument
        self.assertTrue(script_info3.process_done)
        self.assertFalse(script_info3.failed)
        self.assertFalse(script_info3.running)
        self.assertTrue(script_info3.terminated)
        self.assertEqual(script_info3.process_state, SALPY_ScriptQueue.script_Terminated)
        self.assertEqual(script_info3.script_state, SALPY_Script.state_Configured)
        await self.assert_next_queue(running=True, current_sal_index=0,
                                     sal_indices=[], past_sal_indices=[i0+2, i0+1, i0], wait=True)

        # try to stop a script that doesn't exist
        await asyncio.wait_for(self.model.stop_scripts(sal_indices=[333], terminate=terminate),
                               timeout=2)

    def test_stop_scripts(self):
        asyncio.get_event_loop().run_until_complete(self.check_stop_scripts(terminate=False))

    def test_stop_scripts_terminate(self):
        asyncio.get_event_loop().run_until_complete(self.check_stop_scripts(terminate=True))

    async def wait_runnable(self, *indices):
        """Wait for the specified scripts to be runnable.

        Call this before running the queue if you want the queue data
        to be predictable; otherwise the queue may start up with
        no script running.
        """
        for sal_index in indices:
            t0 = time.time()
            print(f"waiting for script {sal_index} to be runnable")
            did_start = False
            did_config = False
            try:
                script_info = self.model.get_script_info(sal_index, search_history=False)
                await asyncio.wait_for(script_info.start_task, 60)
                did_start = True
                await asyncio.wait_for(script_info.config_task, 60)
                did_config = True
                # this will fail if the script was already run
                self.assertTrue(script_info.runnable)
            except Exception as e:
                dt = time.time() - t0
                raise RuntimeError(f"Script {sal_index} did not become runnable in time; "
                                   f"did_start={did_start}; did_config={did_config}; "
                                   f"elapsed time={dt:0.1f}") from e

    async def wait_running(self, sal_index, timeout=5):
        """Wait for the specified script to report that it is running.

        Parameters
        ----------
        sal_index : `int`
            SAL index of script to wait for.
        timeout : `float` (optional)
            Time limit, in seconds. The default is generous
            assuming the script is already runnable and the queue
            either has, or is about to, run it.

        Raises
        ------
        asyncio.TimeoutError
            If the wait times out.
        """
        script_info = self.model.get_script_info(sal_index, search_history=False)
        sleep_time = 0.05
        niter = int(timeout // sleep_time) + 1
        for i in range(niter):
            if script_info.script_state == SALPY_Script.state_Running:
                return
            await asyncio.sleep(sleep_time)
        else:
            raise asyncio.TimeoutError(f"Timed out waiting for script {script_info.index} to start running")


if __name__ == "__main__":
    unittest.main()
