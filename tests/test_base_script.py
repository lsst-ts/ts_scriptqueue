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
import logging
import os
import time
import unittest
import warnings

import yaml

import SALPY_Script
import salobj
from lsst.ts.scriptqueue import ScriptState
from lsst.ts.scriptqueue.test_utils import TestScript

index_gen = salobj.index_generator()


class BaseScriptTestCase(unittest.TestCase):
    def setUp(self):
        salobj.test_utils.set_random_lsst_dds_domain()
        self.datadir = os.path.abspath(os.path.join(os.path.dirname(__file__), "data"))
        self.index = next(index_gen)
        self.process = None

    def tearDown(self):
        if self.process is not None and self.process.returncode is None:
            self.process.terminate()
            warnings.warn("A process was not properly terminated")

    async def configure_script(self, script, **kwargs):
        """Configure a script by calling do_configure

        Parameters
        ----------
        script : `ts.scriptqueue.TestScript`
            A test script
        kwargs : `dict`
            A dict with one or more of the following keys:

            * ``wait_time`` (a float): how long to wait, in seconds
            * ``fail_run`` (bool): fail before waiting?
            * ``fail_cleanup`` (bool): fail in cleanup?

        Raises
        ------
        salobj.ExpectedError
            If ``kwargs`` includes other keywords than those
            documented above (``script.do_configure`` will raise
            that error). This can be useful for unit testing,
            but to try non-dict values you'll have to encode
            the yaml and call ``script.do_configure`` yourself.

        Notes
        -----
        If no keyword arguments are provided then ``script.do_configure``
        will be called with no config data (an empty string).
        This can be useful for unit testing.
        """
        if kwargs:
            # strip to remove final trailing newline
            config = yaml.safe_dump(kwargs).strip()
        else:
            config = ""
        configure_id_data = salobj.CommandIdData(cmd_id=1, data=script.cmd_configure.DataType())
        configure_id_data.data.config = config
        await script.do_configure(configure_id_data)
        self.assertEqual(script.wait_time, kwargs.get("wait_time", 0))
        self.assertEqual(script.fail_run, kwargs.get("fail_run", False))
        self.assertEqual(script.fail_cleanup, kwargs.get("fail_cleanup", False))
        self.assertEqual(script.state.state, ScriptState.CONFIGURED)

    def test_setCheckpoints(self):
        script = TestScript(index=self.index)

        # try valid values
        checkpoints_data = script.cmd_setCheckpoints.DataType()
        id_data = salobj.CommandIdData(1, checkpoints_data)
        for pause, stop in (
            ("something", ""),
            ("", "something_else"),
            (".*", "start|end"),
        ):
            id_data.data.pause = pause
            id_data.data.stop = stop
            script.do_setCheckpoints(id_data)
            self.assertEqual(script.checkpoints.pause, pause)
            self.assertEqual(script.checkpoints.stop, stop)

        # try with at least one checkpoint not a valid regex;
        # do_setCheckpoints should raise and not change the checkpoints
        initial_pause = "initial_pause"
        initial_stop = "initial_stop"
        id_data.data.pause = initial_pause
        id_data.data.stop = initial_stop
        script.do_setCheckpoints(id_data)
        for bad_pause, bad_stop in (
            ("(", ""),
            ("", "("),
            ("[", "["),
        ):
            id_data.data.pause = bad_pause
            id_data.data.stop = bad_stop
            with self.assertRaises(salobj.ExpectedError):
                script.do_setCheckpoints(id_data)
            self.assertEqual(script.checkpoints.pause, initial_pause)
            self.assertEqual(script.checkpoints.stop, initial_stop)

    def test_set_state_and_attributes(self):
        script = TestScript(index=self.index)

        # check keep_old_reason argument of set_state
        reason = "initial reason"
        additional_reason = "check append"
        script.set_state(reason=reason)
        script.set_state(reason=additional_reason, keep_old_reason=True)
        self.assertEqual(script.state.reason, reason + "; " + additional_reason)

        async def doit():
            bad_state = 1 + max(s.value for s in ScriptState)
            with self.assertRaises(ValueError):
                script.set_state(bad_state)
            script.state.state = bad_state
            self.assertEqual(script.state_name, f"UNKNOWN({bad_state})")
            self.assertFalse(script._is_exiting)

            script.set_state(ScriptState.CONFIGURED)
            self.assertEqual(script.state_name, "CONFIGURED")

            # check assert_states
            all_states = set(ScriptState)
            for state in ScriptState:
                script.set_state(state)
                self.assertEqual(script.state_name, state.name)
                with self.assertRaises(salobj.ExpectedError):
                    script.assert_state("should fail because state not in allowed states",
                                        all_states - set([state]))

                script.assert_state("should pass", [state])
                script._is_exiting = True
                with self.assertRaises(salobj.ExpectedError):
                    script.assert_state("should fail because exiting", [state])
                script._is_exiting = False

                # check that checkpoint is prohibited unless state is RUNNING
                if state == ScriptState.RUNNING:
                    continue
                with self.assertRaises(RuntimeError):
                    await script.checkpoint("foo")

        # check final_state_future
        future1 = script.final_state_future
        future2 = script.final_state_future
        self.assertIs(future1, future2)
        self.assertFalse(future1.done())

        asyncio.get_event_loop().run_until_complete(doit())

    def test_pause(self):
        script = TestScript(index=self.index)

        async def doit():
            # cannot run in UNCONFIGURED state
            run_id_data = salobj.CommandIdData(cmd_id=1, data=script.cmd_run.DataType())
            with self.assertRaises(salobj.ExpectedError):
                await script.do_run(run_id_data)

            # test configure with data for a non-existent argument
            configure_id_data = salobj.CommandIdData(cmd_id=1, data=script.cmd_configure.DataType())
            configure_id_data.data.config = "no_such_arg: 1"
            with self.assertRaises(salobj.ExpectedError):
                await script.do_configure(configure_id_data)
            self.assertEqual(script.state.state, ScriptState.UNCONFIGURED)

            # test configure with invalid yaml
            configure_id_data = salobj.CommandIdData(cmd_id=1, data=script.cmd_configure.DataType())
            configure_id_data.data.config = "a : : 2"
            with self.assertRaises(salobj.ExpectedError):
                await script.do_configure(configure_id_data)
            self.assertEqual(script.state.state, ScriptState.UNCONFIGURED)

            # test configure with yaml that makes a string, not a dict
            configure_id_data = salobj.CommandIdData(cmd_id=1, data=script.cmd_configure.DataType())
            configure_id_data.data.config = "just_a_string"
            with self.assertRaises(salobj.ExpectedError):
                await script.do_configure(configure_id_data)
            self.assertEqual(script.state.state, ScriptState.UNCONFIGURED)

            # test configure with yaml that makes a list, not a dict
            configure_id_data = salobj.CommandIdData(cmd_id=1, data=script.cmd_configure.DataType())
            configure_id_data.data.config = "['not', 'a', 'dict']"
            with self.assertRaises(salobj.ExpectedError):
                await script.do_configure(configure_id_data)
            self.assertEqual(script.state.state, ScriptState.UNCONFIGURED)

            # now real configuration
            wait_time = 0.5
            await self.configure_script(script, wait_time=wait_time)

            # set a pause checkpoint
            setCheckpoints_id_data = salobj.CommandIdData(cmd_id=2,
                                                          data=script.cmd_setCheckpoints.DataType())
            checkpoint_named_start = "start"
            checkpoint_that_does_not_exist = "nonexistent checkpoint"
            setCheckpoints_id_data.data.pause = checkpoint_named_start
            setCheckpoints_id_data.data.stop = checkpoint_that_does_not_exist
            script.do_setCheckpoints(setCheckpoints_id_data)
            self.assertEqual(script.checkpoints.pause, checkpoint_named_start)
            self.assertEqual(script.checkpoints.stop, checkpoint_that_does_not_exist)

            start_time = time.time()
            run_id_data = salobj.CommandIdData(cmd_id=3, data=script.cmd_run.DataType())
            run_task = asyncio.ensure_future(script.do_run(run_id_data))
            niter = 0
            while script.state.state != ScriptState.PAUSED:
                niter += 1
                await asyncio.sleep(0)
            self.assertEqual(script.state.lastCheckpoint, checkpoint_named_start)
            self.assertEqual(script.checkpoints.pause, checkpoint_named_start)
            self.assertEqual(script.checkpoints.stop, checkpoint_that_does_not_exist)
            resume_id_data = salobj.CommandIdData(cmd_id=4, data=script.cmd_resume.DataType())
            script.do_resume(resume_id_data)
            await asyncio.wait_for(run_task, 2)
            await asyncio.wait_for(script.final_state_future, 2)
            duration = time.time() - start_time
            desired_duration = wait_time + script.final_state_delay
            print(f"test_pause duration={duration:0.2f}")
            self.assertLess(abs(duration - desired_duration), 0.2)

        asyncio.get_event_loop().run_until_complete(doit())

    def test_stop_at_checkpoint(self):
        script = TestScript(index=self.index)

        async def doit():
            wait_time = 0.1
            await self.configure_script(script, wait_time=wait_time)

            # set a stop checkpoint
            setCheckpoints_id_data = salobj.CommandIdData(cmd_id=2,
                                                          data=script.cmd_setCheckpoints.DataType())
            checkpoint_named_end = "end"
            setCheckpoints_id_data.data.stop = checkpoint_named_end
            script.do_setCheckpoints(setCheckpoints_id_data)
            self.assertEqual(script.checkpoints.pause, "")
            self.assertEqual(script.checkpoints.stop, checkpoint_named_end)

            start_time = time.time()
            run_id_data = salobj.CommandIdData(cmd_id=3, data=script.cmd_run.DataType())
            await asyncio.wait_for(script.do_run(run_id_data), 2)
            final_state = await asyncio.wait_for(script.final_state_future, 2)
            self.assertEqual(script.state.lastCheckpoint, checkpoint_named_end)
            self.assertEqual(final_state.state, ScriptState.STOPPED)
            self.assertEqual(script.state.state, ScriptState.STOPPED)
            duration = time.time() - start_time
            desired_duration = wait_time + script.final_state_delay
            # waited and then stopped at the "end" checkpoint
            print(f"test_stop_at_checkpoint duration={duration:0.2f}")
            self.assertLess(abs(duration - desired_duration), 0.2)

        asyncio.get_event_loop().run_until_complete(doit())

    def test_stop_while_paused(self):
        script = TestScript(index=self.index)

        async def doit():
            wait_time = 5
            await self.configure_script(script, wait_time=wait_time)

            # set a stop checkpoint
            setCheckpoints_id_data = salobj.CommandIdData(cmd_id=2,
                                                          data=script.cmd_setCheckpoints.DataType())
            checkpoint_named_start = "start"
            setCheckpoints_id_data.data.pause = checkpoint_named_start
            script.do_setCheckpoints(setCheckpoints_id_data)
            self.assertEqual(script.checkpoints.pause, checkpoint_named_start)
            self.assertEqual(script.checkpoints.stop, "")

            start_time = time.time()
            run_id_data = salobj.CommandIdData(cmd_id=3, data=script.cmd_run.DataType())
            asyncio.ensure_future(script.do_run(run_id_data))
            while script.state.lastCheckpoint != "start":
                await asyncio.sleep(0)
            self.assertEqual(script.state.state, ScriptState.PAUSED)
            stop_id_data = salobj.CommandIdData(cmd_id=4, data=script.cmd_stop.DataType())
            await script.do_stop(stop_id_data)
            final_state = await asyncio.wait_for(script.final_state_future, 2)
            self.assertEqual(script.state.lastCheckpoint, checkpoint_named_start)
            self.assertEqual(final_state.state, ScriptState.STOPPED)
            self.assertEqual(script.state.state, ScriptState.STOPPED)
            duration = time.time() - start_time
            desired_duration = script.final_state_delay
            # we did not wait because the script right after pausing at the "start" checkpoint
            print(f"test_stop_while_paused duration={duration:0.2f}")
            self.assertGreater(duration, 0.0)
            self.assertLess(abs(duration - desired_duration), 0.2)

        asyncio.get_event_loop().run_until_complete(doit())

    def test_stop_while_running(self):
        script = TestScript(index=self.index)

        async def doit():
            wait_time = 5
            pause_time = 0.5
            await self.configure_script(script, wait_time=wait_time)

            checkpoint_named_start = "start"
            start_time = time.time()
            run_id_data = salobj.CommandIdData(cmd_id=3, data=script.cmd_run.DataType())
            asyncio.ensure_future(script.do_run(run_id_data))
            while script.state.lastCheckpoint != checkpoint_named_start:
                await asyncio.sleep(0)
            self.assertEqual(script.state.state, ScriptState.RUNNING)
            await asyncio.sleep(pause_time)
            stop_id_data = salobj.CommandIdData(cmd_id=4, data=script.cmd_stop.DataType())
            await script.do_stop(stop_id_data)
            final_state = await asyncio.wait_for(script.final_state_future, 2)
            self.assertEqual(script.state.lastCheckpoint, checkpoint_named_start)
            self.assertEqual(final_state.state, ScriptState.STOPPED)
            self.assertEqual(script.state.state, ScriptState.STOPPED)
            duration = time.time() - start_time
            # we waited `pause_time` seconds after the "start" checkpoint
            print(f"test_stop_while_running duration={duration:0.2f}")
            desired_duration = pause_time + script.final_state_delay
            self.assertLess(abs(duration - desired_duration), 0.2)

        asyncio.get_event_loop().run_until_complete(doit())

    def test_fail(self):
        wait_time = 0.1
        for fail_run in (False, True):  # vs fail_cleanup
            script = TestScript(index=self.index)

            async def doit():
                if fail_run:
                    await self.configure_script(script, fail_run=True)
                else:
                    await self.configure_script(script, fail_cleanup=True)

                desired_checkpoint = "start" if fail_run else "end"
                start_time = time.time()
                run_id_data = salobj.CommandIdData(cmd_id=3, data=script.cmd_run.DataType())
                await asyncio.wait_for(script.do_run(run_id_data), 2)
                final_state = await asyncio.wait_for(script.final_state_future, 2)
                self.assertEqual(script.state.lastCheckpoint, desired_checkpoint)
                self.assertEqual(final_state.state, ScriptState.FAILED)
                self.assertEqual(script.state.state, ScriptState.FAILED)
                duration = time.time() - start_time
                # if fail_run then failed before waiting, otherwise failed after
                overhead = 0.2  # seconds to output final state
                desired_duration = (0 if fail_run else wait_time) + overhead
                print(f"test_fail duration={duration:0.2f} with fail_run={fail_run}")
                self.assertLess(abs(duration - desired_duration), 0.2)

        asyncio.get_event_loop().run_until_complete(doit())

    def test_script_process(self):
        """Test running a script as a subprocess.
        """
        script_path = os.path.join(self.datadir, "standard", "script1")

        async def doit():
            for fail in (None, "fail_run", "fail_cleanup"):
                with self.subTest(fail=fail):
                    index = next(index_gen)
                    remote = salobj.Remote(SALPY_Script, index)

                    self.process = await asyncio.create_subprocess_exec(script_path, str(index))
                    self.assertIsNone(self.process.returncode)

                    state = await remote.evt_state.next(flush=False, timeout=60)
                    self.assertEqual(state.state, ScriptState.UNCONFIGURED)

                    setLogging_data = remote.cmd_setLogging.DataType()
                    setLogging_data.level = logging.INFO
                    await remote.cmd_setLogging.start(setLogging_data, timeout=2)

                    wait_time = 0.1
                    configure_data = remote.cmd_configure.DataType()
                    config = f"wait_time: {wait_time}"
                    if fail:
                        config = config + f"\n{fail}: True"
                    print(f"config={config}")
                    configure_data.config = config
                    await remote.cmd_configure.start(configure_data, timeout=2)

                    metadata = remote.evt_metadata.get()
                    self.assertEqual(metadata.duration, wait_time)
                    await asyncio.sleep(0.2)
                    log_msg = remote.evt_logMessage.get()
                    self.assertEqual(log_msg.message, "Configure succeeded")

                    run_data = remote.cmd_run.DataType()
                    await remote.cmd_run.start(run_data, timeout=3)

                    await asyncio.wait_for(self.process.wait(), timeout=2)
                    if fail:
                        self.assertEqual(self.process.returncode, 1)
                    else:
                        self.assertEqual(self.process.returncode, 0)

        asyncio.get_event_loop().run_until_complete(doit())


if __name__ == "__main__":
    unittest.main()
