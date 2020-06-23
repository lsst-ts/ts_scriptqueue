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
import copy
import logging
import os
import time
import unittest
import warnings

import asynctest

from lsst.ts import salobj
from lsst.ts.idl.enums.ScriptQueue import Location, ScriptProcessState
from lsst.ts.idl.enums.Script import ScriptState
from lsst.ts import scriptqueue

# Long enough to perform any reasonable operation
# including starting a CSC or loading a script (seconds)
STD_TIMEOUT = 60


def _min_sal_index_generator():
    min_sal_index = 1000
    while True:
        yield min_sal_index
        min_sal_index += 100


make_min_sal_index = _min_sal_index_generator()


class QueueInfo:
    """Information about the queue. Used by assert_next_queue."""

    def __init__(self, model):
        self.enabled = model.enabled
        self.running = model.running
        self.current_index = model.current_index
        self.queue = copy.copy(model.queue)
        self.history = copy.copy(model.history)


class QueueModelTestCase(asynctest.TestCase):
    async def setUp(self):
        self.t0 = time.monotonic()
        self.min_sal_index = next(make_min_sal_index)
        salobj.set_random_lsst_dds_domain()
        self.datadir = os.path.abspath(os.path.join(os.path.dirname(__file__), "data"))
        self.standardpath = os.path.join(self.datadir, "standard")
        self.externalpath = os.path.join(self.datadir, "external")
        self.domain = salobj.Domain()
        self.log = logging.getLogger()
        self.log.addHandler(logging.StreamHandler())
        self.log.setLevel(logging.DEBUG)
        # Queue of (sal_index, group_id) set by next_visit_callback
        # and used by assert_next_next_visit
        self.next_visit_queue = asyncio.Queue()
        # Queue of (sal_index, group_id) set by next_visit_canceled_callback
        # and used by assert_next_next_visit_canceled
        self.next_visit_canceled_queue = asyncio.Queue()
        # Queue of script queue information;
        # used by assert_next_queue
        self.queue_info_queue = asyncio.Queue()
        self.model = scriptqueue.QueueModel(
            domain=self.domain,
            log=self.log,
            standardpath=self.standardpath,
            externalpath=self.externalpath,
            next_visit_callback=self.next_visit_callback,
            next_visit_canceled_callback=self.next_visit_canceled_callback,
            queue_callback=self.queue_callback,
            script_callback=self.script_callback,
            min_sal_index=self.min_sal_index,
            verbose=True,
        )
        self.model.enabled = True
        await self.model.start_task

    async def tearDown(self):
        nkilled = len(await self.model.wait_terminate_all())
        if nkilled > 0:
            warnings.warn(f"Killed {nkilled} subprocesses")

        await self.domain.close()

    async def assert_next_next_visit(self, sal_index):
        """Assert that the next next_visit callback is for the specified index.

        Parameters
        ----------
        index : `int`
            SAL index of script.
        """
        next_sal_index, next_group_id = await asyncio.wait_for(
            self.next_visit_queue.get(), timeout=STD_TIMEOUT
        )
        self.assertEqual(next_sal_index, sal_index)
        self.assertNotEqual(next_group_id, "")

    async def assert_next_next_visit_canceled(self, sal_index):
        """Assert that the next next_visit_canceled callback
        is for the specified index.

        Parameters
        ----------
        index : `int`
            SAL index of script.
        """
        next_sal_index, next_group_id = await asyncio.wait_for(
            self.next_visit_canceled_queue.get(), timeout=STD_TIMEOUT
        )
        self.assertEqual(next_sal_index, sal_index)
        self.assertNotEqual(next_group_id, "")

    async def assert_next_queue(
        self,
        enabled=True,
        running=False,
        current_sal_index=0,
        sal_indices=(),
        past_sal_indices=(),
        wait=True,
    ):
        """Check next or current queue state.

        The defaults are appropriate to an enabled, paused queue
        with no scripts.

        Skips one queue event if necessary; see Notes.

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
            If True then check the next queue state on a queue of states
            that is loaded by the queue_callback, waiting if necessary.
            If False check the current queue state.

        Notes
        -----
        There is a race condition whereby the top script may need its group ID
        set before it can be run, or its group ID may have been set in time.
        In order to handle this, this test will skip one queue event
        before testing, if all of the following are true:

        * The queue is enabled and running
        * The specified ``current_sal_index != 0``
        * The actual current SAL index is 0 and the queue is not empty
        """
        if wait:
            queue_info = await asyncio.wait_for(
                self.queue_info_queue.get(), timeout=STD_TIMEOUT
            )
        else:
            queue_info = QueueInfo(self.model)
        self.assertEqual(self.model.enabled, enabled)
        self.assertEqual(self.model.running, running)
        if (
            enabled
            and running
            and current_sal_index != 0
            and queue_info.current_index == 0
            and queue_info.queue
        ):
            # Top script not running yet; its group ID is probably being set.
            # Skip this queue info and check the next.
            queue_info = await asyncio.wait_for(
                self.queue_info_queue.get(), timeout=STD_TIMEOUT
            )
        self.assertEqual(queue_info.current_index, current_sal_index)
        self.assertEqual([info.index for info in queue_info.queue], list(sal_indices))
        self.assertEqual(
            [info.index for info in queue_info.history], list(past_sal_indices)
        )

    def assert_script_info_equal(self, info1, info2, is_requeue=False):
        """Assert two ScriptInfo are equal.

        If is_requeue (indicating that we are comparing a requeued
        version of a script to its original) then the index and seq_num
        must differ between the two scripts.
        """
        if is_requeue:
            self.assertNotEqual(info1.index, info2.index)
            self.assertNotEqual(info1.seq_num, info2.seq_num)
        else:
            self.assertEqual(info1.index, info2.index)
            self.assertEqual(info1.seq_num, info2.seq_num)
        self.assertEqual(info1.is_standard, info2.is_standard)
        self.assertEqual(info1.path, info2.path)
        self.assertEqual(info1.config, info2.config)
        self.assertEqual(info1.descr, info2.descr)

    def make_add_kwargs(
        self,
        location=Location.LAST,
        location_sal_index=0,
        is_standard=False,
        path=None,
        config="wait_time: 0.1",
    ):
        """Make keyword arguments for QueueModel.add.

        Parameters
        ----------
        location : `Location` (optional)
            Location of script.
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
            script_info=self.make_script_info(
                is_standard=is_standard, path=path, config=config
            ),
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
            log=self.log,
            remote=self.model.remote,
            index=sal_index,
            seq_num=sal_index * 2,  # arbitrary
            is_standard=is_standard,
            path=path,
            config=config,
            descr=f"{sal_index}",
            verbose=True,
        )

    def next_visit_callback(self, script_info):
        dt = time.monotonic() - self.t0
        print(
            f"next_visit_callback() for {script_info.index}: "
            f"group_id={script_info.group_id}; "
            f"elapsed time={dt:0.1f}; "
        )
        asyncio.create_task(
            self.next_visit_queue.put((script_info.index, script_info.group_id))
        )

    def next_visit_canceled_callback(self, script_info):
        dt = time.monotonic() - self.t0
        print(
            f"next_visit_canceled_callback() for {script_info.index}: "
            f"group_id={script_info.group_id}; "
            f"elapsed time={dt:0.1f}; "
        )
        asyncio.create_task(
            self.next_visit_canceled_queue.put(
                (script_info.index, script_info.group_id)
            )
        )

    def queue_callback(self):
        dt = time.monotonic() - self.t0
        print(
            f"queue_callback(): enabled={self.model.enabled}; "
            f"running={self.model.running}; "
            f"current={self.model.current_index}; "
            f"queue={[info.index for info in self.model.queue]}; "
            f"history={[info.index for info in self.model.history]}; "
            f"elapsed time={dt:0.1f}"
        )
        asyncio.create_task(self.queue_info_queue.put(QueueInfo(self.model)))

    def script_callback(self, script_info):
        curr_time = time.monotonic()
        dt = curr_time - self.t0
        print(
            f"script_callback for {script_info.index} at {curr_time:0.1f}: "
            f"started={script_info.start_task.done()}; "
            f"configured={script_info.configured}; "
            f"process_done={script_info.process_done}; "
            f"terminated={script_info.terminated}; "
            f"script_state={ScriptState(script_info.script_state)!r}; "
            f"group_id={script_info.group_id}; "
            f"elapsed time={dt:0.1f}; "
            f"state_delay={script_info.state_delay:0.1f}"
        )

    async def test_add_scripts(self):
        """Test add."""
        await self.assert_next_queue(enabled=True, running=True)

        # Pause the queue so we know what to expect of queue state.
        self.model.running = False
        await self.assert_next_queue(running=False)

        # Add script i0; queue is empty, so location is irrelevant.
        add_kwargs = self.make_add_kwargs(location=Location.LAST)
        i0 = add_kwargs["script_info"].index
        await asyncio.wait_for(self.model.add(**add_kwargs), timeout=STD_TIMEOUT)
        await self.assert_next_queue(sal_indices=[i0])

        # Add script i0+1 last: test add last.
        add_kwargs = self.make_add_kwargs(location=Location.LAST)
        await asyncio.wait_for(self.model.add(**add_kwargs), timeout=STD_TIMEOUT)
        await self.assert_next_queue(sal_indices=[i0, i0 + 1])

        # Add script i0+2 first: test add first.
        add_kwargs = self.make_add_kwargs(location=Location.FIRST)
        await asyncio.wait_for(self.model.add(**add_kwargs), timeout=STD_TIMEOUT)
        await self.assert_next_queue(sal_indices=[i0 + 2, i0, i0 + 1])

        # Add script i0+3 after i0+1: test add after last.
        add_kwargs = self.make_add_kwargs(
            location=Location.AFTER, location_sal_index=i0 + 1
        )
        await asyncio.wait_for(self.model.add(**add_kwargs), timeout=STD_TIMEOUT)
        await self.assert_next_queue(sal_indices=[i0 + 2, i0, i0 + 1, i0 + 3])

        # Add script i0+4 after i0+2: test add after not-last.
        add_kwargs = self.make_add_kwargs(
            location=Location.AFTER, location_sal_index=i0 + 2
        )
        await asyncio.wait_for(self.model.add(**add_kwargs), timeout=STD_TIMEOUT)
        await self.assert_next_queue(sal_indices=[i0 + 2, i0 + 4, i0, i0 + 1, i0 + 3])

        # Add script i0+5 before i0+2: test add before first.
        add_kwargs = self.make_add_kwargs(
            location=Location.BEFORE, location_sal_index=i0 + 2
        )
        await asyncio.wait_for(self.model.add(**add_kwargs), timeout=STD_TIMEOUT)
        await self.assert_next_queue(
            sal_indices=[i0 + 5, i0 + 2, i0 + 4, i0, i0 + 1, i0 + 3]
        )

        # Add script i0+6 before i0: test add before not first.
        add_kwargs = self.make_add_kwargs(
            location=Location.BEFORE, location_sal_index=i0
        )
        await asyncio.wait_for(self.model.add(**add_kwargs), timeout=STD_TIMEOUT)
        await self.assert_next_queue(
            sal_indices=[i0 + 5, i0 + 2, i0 + 4, i0 + 6, i0, i0 + 1, i0 + 3]
        )

        # Try some failed adds...
        # Fail add due to incorrect path
        add_kwargs = self.make_add_kwargs(location=Location.FIRST)
        add_kwargs["script_info"].path = "bogus_script_name"
        with self.assertRaises(ValueError):
            await asyncio.wait_for(self.model.add(**add_kwargs), timeout=STD_TIMEOUT)
        await self.assert_next_queue(
            sal_indices=[i0 + 5, i0 + 2, i0 + 4, i0 + 6, i0, i0 + 1, i0 + 3], wait=False
        )

        # Fail add due to incorrect location.
        add_kwargs = self.make_add_kwargs(location=25)
        with self.assertRaises(ValueError):
            await asyncio.wait_for(self.model.add(**add_kwargs), timeout=STD_TIMEOUT)
        await self.assert_next_queue(
            sal_indices=[i0 + 5, i0 + 2, i0 + 4, i0 + 6, i0, i0 + 1, i0 + 3], wait=False
        )

        # Fail add due to incorrect location_sal_index.
        add_kwargs = self.make_add_kwargs(
            location=Location.AFTER, location_sal_index=4321
        )
        with self.assertRaises(ValueError):
            await asyncio.wait_for(self.model.add(**add_kwargs), timeout=STD_TIMEOUT)
        await self.assert_next_queue(
            sal_indices=[i0 + 5, i0 + 2, i0 + 4, i0 + 6, i0, i0 + 1, i0 + 3], wait=False
        )

        # Stop a few scripts.
        await asyncio.wait_for(
            self.model.stop_scripts(
                sal_indices=[i0 + 6, i0 + 5, i0, i0 + 4], terminate=True
            ),
            timeout=STD_TIMEOUT,
        )
        await self.assert_next_queue(sal_indices=[i0 + 2, i0 + 1, i0 + 3])

        # Make sure all scripts are runnable before starting the queue,
        # for predictability.
        await self.wait_configured(i0 + 1, i0 + 2, i0 + 3)

        # Disable the queue, then set running True and check that
        # the queue does not start running until we enable it again.
        self.model.enabled = False
        await self.assert_next_queue(
            enabled=False, running=False, sal_indices=[i0 + 2, i0 + 1, i0 + 3]
        )

        self.model.running = True
        await self.assert_next_queue(
            enabled=False, running=True, sal_indices=[i0 + 2, i0 + 1, i0 + 3]
        )

        self.model.enabled = True
        await self.assert_next_next_visit(sal_index=i0 + 2)
        await self.assert_next_queue(
            running=True,
            current_sal_index=i0 + 2,
            sal_indices=[i0 + 1, i0 + 3],
            past_sal_indices=[],
        )
        await self.assert_next_next_visit(sal_index=i0 + 1)
        await self.assert_next_queue(
            running=True,
            current_sal_index=i0 + 1,
            sal_indices=[i0 + 3],
            past_sal_indices=[i0 + 2],
        )
        await self.assert_next_next_visit(sal_index=i0 + 3)
        await self.assert_next_queue(
            running=True,
            current_sal_index=i0 + 3,
            sal_indices=[],
            past_sal_indices=[i0 + 1, i0 + 2],
        )
        await self.assert_next_queue(
            running=True,
            current_sal_index=0,
            sal_indices=[],
            past_sal_indices=[i0 + 3, i0 + 1, i0 + 2],
        )

        # Make sure that next_visit_canceled_callback was not called
        self.assertTrue(self.next_visit_canceled_queue.empty())

    async def test_add_bad_config(self):
        """Test adding a script with invalid configuration.
        """
        await self.assert_next_queue(enabled=True, running=True)

        # Add script i0 with invalid config.
        add_kwargs = self.make_add_kwargs(config="invalid: True")
        script0 = add_kwargs["script_info"]
        i0 = script0.index
        await asyncio.wait_for(self.model.add(**add_kwargs), timeout=STD_TIMEOUT)
        await self.assert_next_queue(running=True, sal_indices=[i0])
        await self.assert_next_queue(
            running=True, current_sal_index=0, sal_indices=[], past_sal_indices=[],
        )
        await script0.process_task
        self.assertTrue(script0.configure_failed)
        self.assertFalse(script0.configured)
        self.assertEqual(script0.process_done, True)
        self.assertEqual(script0.process_state, ScriptProcessState.CONFIGUREFAILED)

    async def check_add_then_stop_script(self, terminate):
        """Test adding a script immediately followed by stoppping it.
        """
        await self.assert_next_queue(enabled=True, running=True)

        # Add script i0.
        add_kwargs = self.make_add_kwargs()
        script0 = add_kwargs["script_info"]
        i0 = script0.index
        add_task = asyncio.create_task(
            asyncio.wait_for(self.model.add(**add_kwargs), timeout=STD_TIMEOUT)
        )
        await self.assert_next_queue(sal_indices=[i0], running=True)
        await asyncio.wait_for(
            self.model.stop_scripts(sal_indices=[i0], terminate=terminate),
            timeout=STD_TIMEOUT,
        )
        await self.assert_next_queue(sal_indices=[], running=True)
        if not add_task.done():
            with self.assertRaises(asyncio.CancelledError):
                await add_task
        self.assertFalse(script0.process_done)
        self.assertTrue(script0.terminated)
        self.assertFalse(script0.configure_failed)
        self.assertFalse(script0.configured)
        self.assertEqual(script0.process_state, ScriptProcessState.TERMINATED)

    async def test_add_then_stop_script(self):
        await self.check_add_then_stop_script(terminate=False)

    async def test_add_then_terminate_script(self):
        await self.check_add_then_stop_script(terminate=True)

    def test_constructor_errors(self):
        nonexistentpath = os.path.join(self.datadir, "garbage")
        with self.assertRaises(ValueError):
            scriptqueue.QueueModel(
                domain=self.domain,
                log=self.log,
                standardpath=self.standardpath,
                externalpath=nonexistentpath,
            )
        with self.assertRaises(ValueError):
            scriptqueue.QueueModel(
                domain=self.domain,
                log=self.log,
                standardpath=nonexistentpath,
                externalpath=self.externalpath,
            )
        with self.assertRaises(ValueError):
            scriptqueue.QueueModel(
                domain=self.domain,
                log=self.log,
                standardpath=nonexistentpath,
                externalpath=nonexistentpath,
            )

    async def test_get_script_info(self):
        await self.assert_next_queue(enabled=True, running=True)

        # Pause the queue so we know what to expect of queue state.
        self.model.running = False
        await self.assert_next_queue(running=False)

        info_dict = dict()
        i0 = None
        for i in range(3):

            script_info = self.make_script_info(
                is_standard=False,
                path=os.path.join("subdir", "script6"),
                config="wait_time: 0.5" if i == 1 else "",
            )
            if i0 is None:
                i0 = script_info.index
            info_dict[script_info.index] = script_info
            await asyncio.wait_for(
                self.model.add(
                    script_info=script_info,
                    location=Location.LAST,
                    location_sal_index=0,
                ),
                timeout=STD_TIMEOUT,
            )
            await self.assert_next_queue(
                sal_indices=[info.index for info in info_dict.values()]
            )

        await self.wait_configured(i0, i0 + 1, i0 + 2)

        # Resume the queue and wait for the second script to start
        # running. At that point we have one running script, one in
        # history and one on the queue. Run get_script_info on each.
        self.model.running = True
        await self.assert_next_queue(
            running=True,
            current_sal_index=i0,
            sal_indices=[i0 + 1, i0 + 2],
            past_sal_indices=[],
        )
        await self.assert_next_queue(
            running=True,
            current_sal_index=i0 + 1,
            sal_indices=[i0 + 2],
            past_sal_indices=[i0],
        )

        info2 = self.model.get_script_info(sal_index=i0 + 2, search_history=False)
        self.assert_script_info_equal(info2, info_dict[i0 + 2])
        with self.assertRaises(ValueError):
            self.model.get_script_info(sal_index=i0, search_history=False)
        for sal_index, expected_script_info in info_dict.items():
            script_info = self.model.get_script_info(
                sal_index=sal_index, search_history=True
            )
            self.assert_script_info_equal(script_info, expected_script_info)

        await self.assert_next_queue(
            running=True,
            current_sal_index=i0 + 2,
            sal_indices=[],
            past_sal_indices=[i0 + 1, i0],
        )
        await self.assert_next_queue(
            running=True,
            current_sal_index=0,
            sal_indices=[],
            past_sal_indices=[i0 + 2, i0 + 1, i0],
        )

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
                fullpath = self.model.make_full_path(
                    is_standard=is_standard, path=goodpath
                )
                expected_fullpath = os.path.join(root, goodpath)
                self.assertTrue(fullpath.samefile(expected_fullpath))

    async def test_move(self):
        """Test move, pause and showQueue
        """
        await self.assert_next_queue(enabled=True, running=True)

        # Pause the queue so we know what to expect of queue state.
        self.model.running = False
        await self.assert_next_queue(running=False)

        # Queue scripts i0, i0+1 and i0+2.
        sal_indices = []
        for i in range(3):
            script_info = self.make_script_info(
                is_standard=True, path=os.path.join("subdir", "script3")
            )
            sal_indices.append(script_info.index)
            await asyncio.wait_for(
                self.model.add(
                    script_info=script_info,
                    location=Location.LAST,
                    location_sal_index=0,
                ),
                timeout=STD_TIMEOUT,
            )
            await self.assert_next_queue(sal_indices=sal_indices)
        i0 = sal_indices[0]

        # Move i0+2 first.
        self.model.move(sal_index=i0 + 2, location=Location.FIRST, location_sal_index=0)
        await self.assert_next_queue(sal_indices=[i0 + 2, i0, i0 + 1])

        # Move i0+2 first again. This should be a no-op, but should still
        # trigger a queue event.
        self.model.move(sal_index=i0 + 2, location=Location.FIRST, location_sal_index=0)
        await self.assert_next_queue(sal_indices=[i0 + 2, i0, i0 + 1])

        # Move i0 last.
        self.model.move(sal_index=i0, location=Location.LAST, location_sal_index=0)
        await self.assert_next_queue(sal_indices=[i0 + 2, i0 + 1, i0])

        # Move i0 last again. This should be a no-op, but should still
        # trigger a queue event.
        self.model.move(sal_index=i0, location=Location.LAST, location_sal_index=0)
        await self.assert_next_queue(sal_indices=[i0 + 2, i0 + 1, i0])

        # Move i0 before i0+2: before first.
        self.model.move(
            sal_index=i0, location=Location.BEFORE, location_sal_index=i0 + 2
        )
        await self.assert_next_queue(sal_indices=[i0, i0 + 2, i0 + 1])

        # Move i0+1 before i0+2: before not-first.
        self.model.move(
            sal_index=i0 + 1, location=Location.BEFORE, location_sal_index=i0 + 2
        )
        await self.assert_next_queue(sal_indices=[i0, i0 + 1, i0 + 2])

        # Move i0 after i0+2: after last.
        self.model.move(
            sal_index=i0, location=Location.AFTER, location_sal_index=i0 + 2
        )
        await self.assert_next_queue(sal_indices=[i0 + 1, i0 + 2, i0])

        # Move i0+1 after i0+2: after not-last.
        self.model.move(
            sal_index=i0 + 1, location=Location.AFTER, location_sal_index=i0 + 2
        )
        await self.assert_next_queue(sal_indices=[i0 + 2, i0 + 1, i0])

        # Move i0 after itself. This should be a no-op, but should still
        # trigger a queue event.
        self.model.move(sal_index=i0, location=Location.AFTER, location_sal_index=i0)
        await self.assert_next_queue(sal_indices=[i0 + 2, i0 + 1, i0])

        # Move i0+1 before itself. This should be a no-op, but should still
        # trigger a queue event.
        self.model.move(
            sal_index=i0 + 1, location=Location.AFTER, location_sal_index=i0 + 1
        )
        await self.assert_next_queue(sal_indices=[i0 + 2, i0 + 1, i0])

        # Try some incorrect moves.
        with self.assertRaises(ValueError):
            self.model.move(
                sal_index=1234,  # no such script
                location=Location.LAST,
                location_sal_index=0,
            )
        await self.assert_next_queue(sal_indices=[i0 + 2, i0 + 1, i0], wait=False)

        with self.assertRaises(ValueError):
            self.model.move(
                sal_index=i0 + 1, location=21, location_sal_index=0  # no such location
            )
        await self.assert_next_queue(sal_indices=[i0 + 2, i0 + 1, i0], wait=False)

        with self.assertRaises(ValueError):
            self.model.move(
                sal_index=i0 + 1, location=Location.BEFORE, location_sal_index=1234
            )  # no such script)
        await self.assert_next_queue(sal_indices=[i0 + 2, i0 + 1, i0], wait=False)

        # Incorrect index and the same "before" locationSalIndex.
        with self.assertRaises(ValueError):
            self.model.move(
                sal_index=1234, location=Location.BEFORE, location_sal_index=1234
            )
        await self.assert_next_queue(sal_indices=[i0 + 2, i0 + 1, i0], wait=False)

        # Incorrect index and the same "after" locationSalIndex.
        with self.assertRaises(ValueError):
            self.model.move(
                sal_index=1234, location=Location.AFTER, location_sal_index=1234
            )
        await self.assert_next_queue(sal_indices=[i0 + 2, i0 + 1, i0], wait=False)

        # Don't wait for the scripts to finish loading; termination is faster.
        await asyncio.wait_for(
            self.model.stop_scripts(sal_indices=[i0 + 2, i0 + 1, i0], terminate=True),
            timeout=STD_TIMEOUT,
        )

    async def test_clear_group_id(self):
        """Test that a script at the top of the queue has its group ID cleared
        if it is moved elsewhere.
        """
        await self.assert_next_queue(enabled=True, running=True)

        # Pause the queue so we know what to expect of queue state.
        self.model.running = False
        await self.assert_next_queue(running=False)

        # Queue scripts i0, i0+1 and i0+2.
        sal_indices = []
        for i in range(3):
            script_info = self.make_script_info(
                is_standard=True,
                path=os.path.join("subdir", "script3"),
                config="wait_time: 2",
            )
            sal_indices.append(script_info.index)
            await asyncio.wait_for(
                self.model.add(
                    script_info=script_info,
                    location=Location.LAST,
                    location_sal_index=0,
                ),
                timeout=STD_TIMEOUT,
            )
            await self.assert_next_queue(sal_indices=sal_indices)
        i0 = sal_indices[0]

        await self.wait_configured(i0, i0 + 1, i0 + 2)

        # Start the queue and wait for i0+1's group ID to be set
        # then move i0+1 last and check that its group ID is cleared
        # and that i0+2's group ID is set.
        self.model.running = True
        print(f"*** wait for i0={i0} group ID")
        await self.assert_next_next_visit(sal_index=i0)
        print(f"*** wait for i0={i0} to be running")
        await self.assert_next_queue(
            running=True, current_sal_index=i0, sal_indices=[i0 + 1, i0 + 2]
        )
        print(f"*** wait for i0+1={i0+1} group ID")
        await self.assert_next_next_visit(sal_index=i0 + 1)
        print(f"*** move i0+1={i0+1}")
        self.model.move(sal_index=i0 + 1, location=Location.LAST, location_sal_index=0)
        await self.assert_next_queue(
            running=True, current_sal_index=i0, sal_indices=[i0 + 2, i0 + 1]
        )
        await self.assert_next_next_visit_canceled(sal_index=i0 + 1)
        await self.assert_next_next_visit(sal_index=i0 + 2)
        await self.assert_next_queue(
            running=True,
            current_sal_index=i0 + 2,
            sal_indices=[i0 + 1],
            past_sal_indices=[i0],
        )
        await self.assert_next_queue(
            running=True,
            current_sal_index=i0 + 1,
            sal_indices=[],
            past_sal_indices=[i0 + 2, i0],
        )
        await self.assert_next_queue(
            running=True,
            current_sal_index=0,
            sal_indices=[],
            past_sal_indices=[i0 + 1, i0 + 2, i0],
        )

    async def test_pause_on_failure(self):
        """Test that a failed script pauses the queue.
        """
        await self.assert_next_queue(enabled=True, running=True)

        # Pause the queue so we know what to expect of queue state.
        self.model.running = False
        await self.assert_next_queue(running=False)

        # Add scripts i0, i0+1, i0+2; i0+1 fails.
        add_kwargs = self.make_add_kwargs()
        i0 = add_kwargs["script_info"].index
        await asyncio.wait_for(self.model.add(**add_kwargs), timeout=STD_TIMEOUT)
        await self.assert_next_queue(sal_indices=[i0])

        add_kwargs = self.make_add_kwargs(config="wait_time: 0.1\nfail_run: True")
        await asyncio.wait_for(self.model.add(**add_kwargs), timeout=STD_TIMEOUT)
        await self.assert_next_queue(sal_indices=[i0, i0 + 1])

        add_kwargs = self.make_add_kwargs()
        await asyncio.wait_for(self.model.add(**add_kwargs), timeout=STD_TIMEOUT)
        await self.assert_next_queue(sal_indices=[i0, i0 + 1, i0 + 2])

        # Make sure all scripts are runnable before starting the queue.
        await self.wait_configured(i0, i0 + 1, i0 + 2)

        # Start the queue; it should pause when i0+1 fails.
        self.model.running = True
        await self.assert_next_queue(
            running=True,
            current_sal_index=i0,
            sal_indices=[i0 + 1, i0 + 2],
            past_sal_indices=[],
        )
        await self.assert_next_queue(
            running=True,
            current_sal_index=i0 + 1,
            sal_indices=[i0 + 2],
            past_sal_indices=[i0],
        )
        await self.assert_next_queue(
            running=False,
            current_sal_index=i0 + 1,
            sal_indices=[i0 + 2],
            past_sal_indices=[i0],
        )

        # Assert that the process return code is positive for failure
        # and that the final script state Failed is sent and recorded.
        script_info = self.model.get_script_info(i0 + 1, search_history=False)
        self.assertTrue(script_info.process_done)
        self.assertGreater(script_info.process.returncode, 0)
        self.assertEqual(script_info.script_state, ScriptState.FAILED)

        # Resume the queue; this should move i0+1 to history and keep going.
        self.model.running = True
        await self.assert_next_queue(
            running=True,
            current_sal_index=i0 + 2,
            sal_indices=[],
            past_sal_indices=[i0 + 1, i0],
        )
        await self.assert_next_queue(
            running=True,
            current_sal_index=0,
            sal_indices=[],
            past_sal_indices=[i0 + 2, i0 + 1, i0],
        )

        # Assert that the process return code is 0 for success
        # and that the final script state Done is sent and recorded.
        script_info = self.model.get_script_info(i0 + 2, search_history=True)
        self.assertTrue(script_info.process_done)
        self.assertEqual(script_info.process.returncode, 0)
        self.assertEqual(script_info.script_state, ScriptState.DONE)

    async def test_requeue(self):
        """Test requeue
        """
        await self.assert_next_queue(enabled=True, running=True)

        # Pause the queue so we know what to expect of queue state.
        self.model.running = False
        await self.assert_next_queue(running=False)

        # Add the scripts to the end of the queue.
        i0 = None
        info_list = list()
        for i in range(3):
            script_info = self.make_script_info(
                is_standard=False,
                path=os.path.join("subdir", "script6"),
                config="wait_time: 1" if i == 1 else "",
            )
            if i0 is None:
                i0 = script_info.index
            info_list.append(script_info)

            await asyncio.wait_for(
                self.model.add(
                    script_info=script_info,
                    location=Location.LAST,
                    location_sal_index=0,
                ),
                timeout=STD_TIMEOUT,
            )
            await self.assert_next_queue(sal_indices=[info.index for info in info_list])

        await self.wait_configured(i0, i0 + 1)

        # Resume the queue and wait for the second script to start
        # running. At that point we have one running script,
        # one in history and one on the queue; requeue each.
        self.model.running = True
        await self.assert_next_queue(
            running=True,
            current_sal_index=i0,
            sal_indices=[i0 + 1, i0 + 2],
            past_sal_indices=[],
        )
        await self.assert_next_queue(
            running=True,
            current_sal_index=i0 + 1,
            sal_indices=[i0 + 2],
            past_sal_indices=[i0],
        )

        rq1 = await asyncio.wait_for(
            self.model.requeue(
                sal_index=i0 + 1,
                seq_num=32,  # arbitrary but unique
                location=Location.FIRST,
                location_sal_index=0,
            ),
            timeout=STD_TIMEOUT,
        )
        await self.assert_next_queue(
            running=True,
            current_sal_index=i0 + 1,
            sal_indices=[i0 + 3, i0 + 2],
            past_sal_indices=[i0],
        )

        rq2 = await asyncio.wait_for(
            self.model.requeue(
                sal_index=i0 + 2,
                seq_num=30,
                location=Location.AFTER,
                location_sal_index=i0 + 3,
            ),
            timeout=STD_TIMEOUT,
        )
        await self.assert_next_queue(
            running=True,
            current_sal_index=i0 + 1,
            sal_indices=[i0 + 3, i0 + 4, i0 + 2],
            past_sal_indices=[i0],
        )

        rq0 = await asyncio.wait_for(
            self.model.requeue(
                sal_index=i0,
                seq_num=31,
                location=Location.BEFORE,
                location_sal_index=i0 + 3,
            ),
            timeout=STD_TIMEOUT,
        )
        await self.assert_next_queue(
            running=True,
            current_sal_index=i0 + 1,
            sal_indices=[i0 + 5, i0 + 3, i0 + 4, i0 + 2],
            past_sal_indices=[i0],
        )

        # Now pause the queue and wait for the current script to finish
        # and all remaining scripts to be runnable, then resume.
        self.model.running = False
        await self.assert_next_queue(
            running=False,
            current_sal_index=i0 + 1,
            sal_indices=[i0 + 5, i0 + 3, i0 + 4, i0 + 2],
            past_sal_indices=[i0],
        )
        await self.assert_next_queue(
            running=False,
            current_sal_index=0,
            sal_indices=[i0 + 5, i0 + 3, i0 + 4, i0 + 2],
            past_sal_indices=[i0 + 1, i0],
        )
        await self.wait_configured(i0 + 2, i0 + 3, i0 + 4, i0 + 5)
        self.model.running = True
        await self.assert_next_queue(
            running=True,
            current_sal_index=i0 + 5,
            sal_indices=[i0 + 3, i0 + 4, i0 + 2],
            past_sal_indices=[i0 + 1, i0],
        )
        await self.assert_next_queue(
            running=True,
            current_sal_index=i0 + 3,
            sal_indices=[i0 + 4, i0 + 2],
            past_sal_indices=[i0 + 5, i0 + 1, i0],
        )
        await self.assert_next_queue(
            running=True,
            current_sal_index=i0 + 4,
            sal_indices=[i0 + 2],
            past_sal_indices=[i0 + 3, i0 + 5, i0 + 1, i0],
        )
        await self.assert_next_queue(
            running=True,
            current_sal_index=i0 + 2,
            sal_indices=[],
            past_sal_indices=[i0 + 4, i0 + 3, i0 + 5, i0 + 1, i0],
        )
        await self.assert_next_queue(
            running=True,
            current_sal_index=0,
            sal_indices=[],
            past_sal_indices=[i0 + 2, i0 + 4, i0 + 3, i0 + 5, i0 + 1, i0],
        )

        requeue_info_list = [rq0, rq1, rq2]
        for requeue_info, info in zip(requeue_info_list, info_list):
            self.assert_script_info_equal(requeue_info, info, is_requeue=True)

    async def test_resume_before_first_script_runnable(self):
        await self.assert_next_queue(enabled=True, running=True)

        # pause the queue so we know what to expect of queue state
        self.model.running = False
        await self.assert_next_queue(running=False)

        info0 = self.make_script_info(
            is_standard=False,
            path=os.path.join("subdir", "script6"),
            config="wait_time: 0.1",
        )
        i0 = info0.index
        await asyncio.wait_for(
            self.model.add(
                script_info=info0, location=Location.LAST, location_sal_index=0
            ),
            timeout=STD_TIMEOUT,
        )
        await self.assert_next_queue(sal_indices=[i0])

        self.model.running = True
        await self.assert_next_queue(
            running=True, current_sal_index=i0, sal_indices=[], past_sal_indices=[],
        )
        await self.assert_next_queue(
            running=True, current_sal_index=0, sal_indices=[], past_sal_indices=[i0]
        )

    async def test_run_immediately(self):
        await self.assert_next_queue(enabled=True, running=True)

        info0 = self.make_script_info(
            is_standard=False, path=os.path.join("subdir", "script6"), config=""
        )
        i0 = info0.index
        await asyncio.wait_for(
            self.model.add(
                script_info=info0, location=Location.LAST, location_sal_index=0
            ),
            timeout=STD_TIMEOUT,
        )

        await self.assert_next_queue(
            running=True, current_sal_index=i0, sal_indices=[], past_sal_indices=[],
        )
        await self.assert_next_queue(
            running=True, current_sal_index=0, sal_indices=[], past_sal_indices=[i0]
        )

    async def check_stop_scripts(self, terminate):
        await self.assert_next_queue(enabled=True, running=True)

        # pause the queue so we know what to expect of queue state
        self.model.running = False
        await self.assert_next_queue(running=False)

        info_dict = dict()
        i0 = None
        for i in range(4):
            script_info = self.make_script_info(
                is_standard=False,
                path=os.path.join("subdir", "script6"),
                config="wait_time: 10" if i == 1 else "",
            )
            if i0 is None:
                i0 = script_info.index
            info_dict[script_info.index] = script_info
            await asyncio.wait_for(
                self.model.add(
                    script_info=script_info,
                    location=Location.LAST,
                    location_sal_index=0,
                ),
                timeout=STD_TIMEOUT,
            )
            await self.assert_next_queue(
                sal_indices=[info.index for info in info_dict.values()]
            )

        await self.wait_configured(i0, i0 + 1, i0 + 2, i0 + 3)

        # Resume the queue and wait for the second script to start running.
        # At that point we have one script running, one in history,
        # and two on the queue.
        self.model.running = True
        await self.assert_next_queue(
            running=True,
            current_sal_index=i0,
            sal_indices=[i0 + 1, i0 + 2, i0 + 3],
            past_sal_indices=[],
        )
        await self.assert_next_queue(
            running=True,
            current_sal_index=i0 + 1,
            sal_indices=[i0 + 2, i0 + 3],
            past_sal_indices=[i0],
        )

        # Wait for script i0+1 to actally start running.
        await self.wait_running(i0 + 1)

        # Stop the current script and a queued script.
        # The current script is added to the history,
        # but the queued script is not.
        print(f"stop {i0+1} and {i0+3}")
        await asyncio.wait_for(
            self.model.stop_scripts(sal_indices=[i0 + 1, i0 + 3], terminate=terminate),
            timeout=STD_TIMEOUT,
        )
        # After a queue callback or two, i0 + 2 should be the current script,
        # and the queue should be empty.
        while True:
            queue_info = await asyncio.wait_for(
                self.queue_info_queue.get(), timeout=STD_TIMEOUT
            )
            if queue_info.current_index == i0 + 2 and not queue_info.queue:
                break
        await self.assert_next_queue(
            running=True,
            current_sal_index=i0 + 2,
            sal_indices=[],
            past_sal_indices=[i0 + 1, i0],
            wait=False,
        )

        script_info1 = info_dict[i0 + 1]
        script_info2 = info_dict[i0 + 2]
        script_info3 = info_dict[i0 + 3]

        print(f"wait for {i0+1}, {i0+2} and {i0+3} to finish")
        t0 = time.monotonic()
        await asyncio.wait_for(
            asyncio.gather(
                script_info1.process_task,
                script_info2.process_task,
                script_info3.process_task,
                return_exceptions=False,
            ),
            timeout=STD_TIMEOUT,
        )
        dt = time.monotonic() - t0
        print(f"waited {dt:0.2f} seconds")
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
            self.assertEqual(script_info1.script_state, ScriptState.STOPPED)
            self.assertEqual(script_info1.process_state, ScriptProcessState.DONE)

        # script i0+2 ran normally
        self.assertTrue(script_info2.process_done)
        self.assertFalse(script_info2.failed)
        self.assertFalse(script_info2.running)
        self.assertFalse(script_info2.terminated)
        self.assertEqual(script_info2.process_state, ScriptProcessState.DONE)
        self.assertEqual(script_info2.script_state, ScriptState.DONE)

        # script i0+3 was stopped while queued, so it was terminated,
        # regardless of the `terminate` argument
        self.assertTrue(script_info3.process_done)
        self.assertFalse(script_info3.failed)
        self.assertFalse(script_info3.running)
        self.assertTrue(script_info3.terminated)
        self.assertEqual(script_info3.process_state, ScriptProcessState.TERMINATED)
        self.assertEqual(script_info3.script_state, ScriptState.CONFIGURED)
        await self.assert_next_queue(
            running=True,
            current_sal_index=0,
            sal_indices=[],
            past_sal_indices=[i0 + 2, i0 + 1, i0],
        )

        # try to stop a script that doesn't exist
        await asyncio.wait_for(
            self.model.stop_scripts(sal_indices=[333], terminate=terminate),
            timeout=STD_TIMEOUT,
        )

    async def test_stop_scripts_noterminate(self):
        await self.check_stop_scripts(terminate=False)

    async def test_stop_scripts_terminate(self):
        await self.check_stop_scripts(terminate=True)

    async def wait_done(self, *indices):
        """Wait for the specified scripts finish running (succeed or fail).

        Return the result of each task.
        """
        print(f"waiting for scripts {indices} to finish running")
        process_tasks = []
        for sal_index in indices:
            script_info = self.model.get_script_info(sal_index, search_history=False)
            process_tasks.append(script_info.process_task)
        try:
            return await asyncio.wait_for(
                asyncio.gather(*process_tasks, return_exceptions=True),
                timeout=STD_TIMEOUT,
            )
        except asyncio.TimeoutError:
            late_scripts = [
                ind for task, ind in zip(process_tasks, indices) if not task.done()
            ]
            raise RuntimeError(f"Scripts {late_scripts} did not finish in 60 seconds")

    async def wait_configured(self, *indices):
        """Wait for the specified scripts to be configured.

        Call this before running the queue if you want the queue data
        to be predictable; otherwise the queue may start up with
        no script running.
        """
        print(f"wait_configured(*{indices})")
        for sal_index in indices:
            t0 = time.monotonic()
            did_start = False
            did_config = False
            try:
                script_info = self.model.get_script_info(
                    sal_index, search_history=False
                )
                print(f"wait_configured: waiting for script {sal_index} to load")
                await asyncio.wait_for(script_info.start_task, timeout=STD_TIMEOUT)
                did_start = True
                print(
                    f"wait_configured: waiting for script {sal_index} to be configured"
                )
                await asyncio.wait_for(script_info.config_task, timeout=STD_TIMEOUT)
                did_config = True
            except Exception as e:
                dt = time.monotonic() - t0
                raise RuntimeError(
                    f"Script {sal_index} did not become runnable in time; "
                    f"did_start={did_start}; did_config={did_config}; "
                    f"elapsed time={dt:0.1f}"
                ) from e

    async def wait_running(self, sal_index):
        """Wait for the specified script to report that it is running.

        Parameters
        ----------
        sal_index : `int`
            SAL index of script to wait for.

        Raises
        ------
        asyncio.TimeoutError
            If the wait times out.
        """
        print(f"wait_running({sal_index})")
        script_info = self.model.get_script_info(sal_index, search_history=False)
        sleep_time = 0.05
        niter = int(STD_TIMEOUT // sleep_time) + 1
        for i in range(niter):
            if script_info.script_state == ScriptState.RUNNING:
                return
            await asyncio.sleep(sleep_time)
        else:
            raise asyncio.TimeoutError(
                f"Timed out waiting for script {script_info.index} to start running"
            )


if __name__ == "__main__":
    unittest.main()
