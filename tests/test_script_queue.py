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
import shutil
import unittest
from unittest.mock import patch

import pytest
import yaml
from lsst.ts import salobj, scriptqueue, utils
from lsst.ts.xml.enums.Script import ScriptState
from lsst.ts.xml.enums.ScriptQueue import Location, SalIndex, ScriptProcessState

try:
    from lsst.ts import standardscripts
except ImportError:
    standardscripts = None

try:
    from lsst.ts import externalscripts
except ImportError:
    externalscripts = None

# Standard timeout (seconds). Long enough to perform any reasonable operation,
# including starting a CSC or loading a script (seconds)
STD_TIMEOUT = 60

I0 = scriptqueue.script_queue.SCRIPT_INDEX_MULT  # initial Script SAL index


class MakeKWargs:
    """Functor to make a set of keyword arguments.

    The constructor specifies the default argument names and values.
    Call the functor with optional overrides.
    """

    def __init__(self, **defaults):
        self.defaults = defaults

    def __call__(self, **kwargs):
        ret = copy.copy(self.defaults)
        ret.update(kwargs)
        return ret


class MakeAddKwargs(MakeKWargs):
    """Functor to create keyword argument for the add command,
    with useful defaults.

    Parameters
    ----------
    isStandard : `bool` (optional)
        Is this a standard (True) or external (False) script?
    path : `str`, `bytes` or `os.PathLike` (optional)
        Path to script, relative to standard or external root dir.
    config : `str` (optional)
        Default configuration data, as a YAML encoded string.
    descr : `str` (optional)
        A short explanation of why this script is being run.
    """

    def __init__(
        self,
        isStandard="True",
        path="script1",
        config="wait_time: 0.1",
        descr="a description",
    ):
        super().__init__(
            isStandard=isStandard,
            path=path,
            location=Location.LAST,
            locationSalIndex=0,
            config=config,
            descr=descr,
        )


class ScriptQueueConstructorTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        salobj.set_random_lsst_dds_partition_prefix()
        try:
            self.default_standardpath = scriptqueue.get_default_scripts_dir(
                is_standard=True
            )
        except ImportError:
            self.default_standardpath = None

        try:
            self.default_externalpath = scriptqueue.get_default_scripts_dir(
                is_standard=False
            )
        except ImportError:
            self.default_externalpath = None

        self.datadir = os.path.abspath(os.path.join(os.path.dirname(__file__), "data"))
        self.testdata_standardpath = os.path.join(self.datadir, "standard")
        self.testdata_externalpath = os.path.join(self.datadir, "external")
        self.badpath = os.path.join(self.datadir, "not_a_directory")

    @unittest.skipIf(
        standardscripts is None or externalscripts is None,
        "Could not import ts_standardscripts and/or ts_externalscripts.",
    )
    async def test_default_paths(self):
        async with scriptqueue.ScriptQueue(
            index=SalIndex.MAIN_TEL
        ) as queue, salobj.Remote(
            domain=queue.domain, name="ScriptQueue", index=SalIndex.MAIN_TEL
        ) as remote:
            assert os.path.samefile(queue.model.standardpath, self.default_standardpath)
            assert os.path.samefile(queue.model.externalpath, self.default_externalpath)
            rootDir_data = await remote.evt_rootDirectories.next(
                flush=False, timeout=STD_TIMEOUT
            )
            assert os.path.samefile(rootDir_data.standard, self.default_standardpath)
            assert os.path.samefile(rootDir_data.external, self.default_externalpath)

            # some tests rely on these being different, so verify that
            assert self.testdata_standardpath != self.default_standardpath
            assert self.testdata_externalpath != self.default_externalpath

    async def test_explicit_paths(self):
        async with scriptqueue.ScriptQueue(
            index=SalIndex.MAIN_TEL,
            standardpath=self.testdata_standardpath,
            externalpath=self.testdata_externalpath,
        ) as queue, salobj.Remote(
            domain=queue.domain, name="ScriptQueue", index=SalIndex.MAIN_TEL
        ) as remote:
            assert os.path.samefile(
                queue.model.standardpath, self.testdata_standardpath
            )
            assert os.path.samefile(
                queue.model.externalpath, self.testdata_externalpath
            )
            rootDir_data = await remote.evt_rootDirectories.next(
                flush=False, timeout=STD_TIMEOUT
            )
            assert os.path.samefile(rootDir_data.standard, self.testdata_standardpath)
            assert os.path.samefile(rootDir_data.external, self.testdata_externalpath)

    @unittest.skipIf(
        standardscripts is None,
        "Could not import ts_standardscripts.",
    )
    async def test_default_standard_path(self):
        async with scriptqueue.ScriptQueue(
            index=SalIndex.MAIN_TEL, externalpath=self.testdata_externalpath
        ) as queue, salobj.Remote(
            domain=queue.domain, name="ScriptQueue", index=SalIndex.MAIN_TEL
        ) as remote:
            assert os.path.samefile(queue.model.standardpath, self.default_standardpath)
            assert os.path.samefile(
                queue.model.externalpath, self.testdata_externalpath
            )
            rootDir_data = await remote.evt_rootDirectories.next(
                flush=False, timeout=STD_TIMEOUT
            )
            assert os.path.samefile(rootDir_data.standard, self.default_standardpath)
            assert os.path.samefile(rootDir_data.external, self.testdata_externalpath)

    @unittest.skipIf(
        externalscripts is None,
        "Could not import ts_externalscripts.",
    )
    async def test_default_external_path(self):
        async with scriptqueue.ScriptQueue(
            index=SalIndex.MAIN_TEL, standardpath=self.testdata_standardpath
        ) as queue, salobj.Remote(
            domain=queue.domain, name="ScriptQueue", index=SalIndex.MAIN_TEL
        ) as remote:
            assert os.path.samefile(
                queue.model.standardpath, self.testdata_standardpath
            )
            assert os.path.samefile(queue.model.externalpath, self.default_externalpath)
            rootDir_data = await remote.evt_rootDirectories.next(
                flush=False, timeout=STD_TIMEOUT
            )
            assert os.path.samefile(rootDir_data.standard, self.testdata_standardpath)
            assert os.path.samefile(rootDir_data.external, self.default_externalpath)

    def test_invalid_paths(self):
        with pytest.raises(ValueError):
            scriptqueue.ScriptQueue(
                index=SalIndex.MAIN_TEL,
                standardpath=self.badpath,
                externalpath=self.testdata_externalpath,
            )
        with pytest.raises(ValueError):
            scriptqueue.ScriptQueue(
                index=SalIndex.MAIN_TEL,
                standardpath=self.testdata_standardpath,
                externalpath=self.badpath,
            )
        with pytest.raises(ValueError):
            scriptqueue.ScriptQueue(
                index=SalIndex.MAIN_TEL,
                standardpath=self.badpath,
                externalpath=self.badpath,
            )


class ScriptQueueTestCase(salobj.BaseCscTestCase, unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        super().setUp()
        datadir = os.path.abspath(os.path.join(os.path.dirname(__file__), "data"))
        self.standardpath = os.path.join(datadir, "standard")
        self.externalpath = os.path.join(datadir, "external")
        self.events_oldest_timestamp = utils.current_tai()

    def basic_make_csc(self, initial_state, config_dir=None, simulation_mode=0):
        csc = scriptqueue.ScriptQueue(
            index=SalIndex.MAIN_TEL,
            initial_state=initial_state,
            standardpath=self.standardpath,
            externalpath=self.externalpath,
            verbose=True,
        )
        return csc

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
        stop_data.salIndices[0 : stop_data.length] = stop_indices
        stop_data.length = stop_data.length
        stop_data.terminate = False
        return stop_data

    async def assert_next_queue(
        self,
        enabled=True,
        running=False,
        current_sal_index=0,
        sal_indices=(),
        past_sal_indices=(),
        verbose=False,
    ):
        """Get the next queue event and check values.

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
            If this is a `set` then order is ignored when comparing.

        Returns
        -------
        queue_data : ``evt_queue.DataType``
            queue message.

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
        if verbose:
            print(
                f"assert_next_queue(enabled={enabled}, "
                f"running={running}, "
                f"currentSalIndex={current_sal_index}, "
                f"sal_indices={sal_indices}, "
                f"past_sal_indices={past_sal_indices}"
            )
        queue_data = await self.assert_next_sample(
            self.remote.evt_queue,
            enabled=enabled,
            running=running,
        )
        if verbose:
            print(
                "assert_next_queue read: "
                f"enabled={queue_data.enabled}; "
                f"running={queue_data.running}; "
                f"currentSalIndex={queue_data.currentSalIndex}; "
                f"sal_indices={queue_data.salIndices[0 : queue_data.length]}; "
                f"past_sal_indices={queue_data.pastSalIndices[0 : queue_data.pastLength]}"
            )
        if (
            enabled
            and running
            and current_sal_index != 0
            and queue_data.currentSalIndex == 0
            and queue_data.length > 0
        ):
            if verbose:
                print(
                    "assert_next_queue: top script not running yet; await next queue event"
                )
            # Top script not running yet; its group ID is probably being set.
            # Skip this event and check the next.
            queue_data = await self.remote.evt_queue.next(
                flush=False, timeout=STD_TIMEOUT
            )
            if verbose:
                print(
                    "assert_next_queue: next read: "
                    f"enabled={queue_data.enabled}; "
                    f"running={queue_data.running}; "
                    f"currentSalIndex={queue_data.currentSalIndex}; "
                    f"sal_indices={sal_indices}; "
                    f"past_sal_indices={past_sal_indices}"
                )
        assert queue_data.currentSalIndex == current_sal_index
        assert list(queue_data.salIndices[0 : queue_data.length]) == list(sal_indices)
        if isinstance(past_sal_indices, set):
            assert (
                set(queue_data.pastSalIndices[0 : queue_data.pastLength])
                == past_sal_indices
            )
        else:
            assert list(queue_data.pastSalIndices[0 : queue_data.pastLength]) == list(
                past_sal_indices
            )
        return queue_data

    async def assert_next_next_visit(self, sal_index):
        """Assert that the next nextVisit event is for the specified index
        and return the event data.

        Parameters
        ----------
        index : `int`
            SAL index of script.

        Returns
        -------
        data : ``evt_nextVisit.DataType``
            The nextVisit data.
        """
        data = await self.assert_next_sample(
            self.remote.evt_nextVisit, scriptSalIndex=sal_index
        )
        assert data.groupId != ""
        return data

    async def assert_next_next_visit_canceled(self, sal_index):
        """Assert that the next nextVisitCanceled event
        is for the specified index.

        Parameters
        ----------
        index : `int`
            SAL index of script.
        """
        data = await self.assert_next_sample(
            self.remote.evt_nextVisitCanceled,
            scriptSalIndex=sal_index,
        )
        assert data.groupId != ""

    @patch("lsst.ts.utils.ImageNameServiceClient.get_next_obs_id")
    @patch.dict(os.environ, {"IMAGE_SERVER_URL": "mytemp"})
    async def test_add_block(self, mock_get):
        """Test adding scripts that are part of a block."""

        mock_get.side_effect = [
            (0, ["BL1_O_20240228_000001"]),
            (0, ["BL1_O_20240228_000002"]),
        ]
        is_standard = False
        path = "script1"
        config = "wait_time: 1"  # give showScript time to run
        make_add_kwargs = MakeAddKwargs(
            isStandard=is_standard, path=path, config=config, descr="test_add_block"
        )
        async with self.make_csc(initial_state=salobj.State.ENABLED), salobj.Remote(
            domain=self.csc.domain, name="Script"
        ) as script_remote:

            await self.assert_next_queue(enabled=False, running=True)
            await self.assert_next_queue(enabled=True, running=True)

            # Pause the queue so we know what to expect of queue state.
            await self.remote.cmd_pause.start(timeout=STD_TIMEOUT)
            await self.assert_next_queue(running=False)

            # Add script I0; queue is empty, so location is irrelevant.
            add_kwargs = make_add_kwargs()
            add_kwargs["block"] = "BLOCK-1"
            add_kwargs["startBlock"] = True
            add_kwargs["blockSize"] = 3

            ackcmd = await self.remote.cmd_add.set_start(
                **add_kwargs, timeout=STD_TIMEOUT
            )
            assert int(ackcmd.result) == I0
            await self.assert_next_queue(sal_indices=[I0])

            add_kwargs["startBlock"] = False
            add_kwargs.pop("blockSize")

            ackcmd = await self.remote.cmd_add.set_start(
                **add_kwargs, timeout=STD_TIMEOUT
            )
            assert int(ackcmd.result) == I0 + 1
            await self.assert_next_queue(sal_indices=[I0, I0 + 1])

            ackcmd = await self.remote.cmd_add.set_start(
                **add_kwargs, timeout=STD_TIMEOUT
            )
            assert int(ackcmd.result) == I0 + 2
            await self.assert_next_queue(sal_indices=[I0, I0 + 1, I0 + 2])

            # Adding another script to the same block should fail.
            with pytest.raises(
                salobj.AckError,
                match="Block already filled with all the expected number of scripts. Declared capacity is 3.",
            ):
                await self.remote.cmd_add.set_start(**add_kwargs, timeout=STD_TIMEOUT)

            # Start another block execution of the same block.
            add_kwargs = make_add_kwargs()
            add_kwargs["block"] = "BLOCK-1"
            add_kwargs["startBlock"] = True
            add_kwargs["blockSize"] = 3

            ackcmd = await self.remote.cmd_add.set_start(
                **add_kwargs, timeout=STD_TIMEOUT
            )
            # We have to skip one index because the failed attempt to add a
            # script above consumes one index.
            assert int(ackcmd.result) == I0 + 4
            await self.assert_next_queue(sal_indices=[I0, I0 + 1, I0 + 2, I0 + 4])

            add_kwargs["startBlock"] = False
            add_kwargs.pop("blockSize")

            ackcmd = await self.remote.cmd_add.set_start(
                **add_kwargs, timeout=STD_TIMEOUT
            )
            assert int(ackcmd.result) == I0 + 5
            await self.assert_next_queue(
                sal_indices=[I0, I0 + 1, I0 + 2, I0 + 4, I0 + 5]
            )

            ackcmd = await self.remote.cmd_add.set_start(
                **add_kwargs, timeout=STD_TIMEOUT
            )
            assert int(ackcmd.result) == I0 + 6
            await self.assert_next_queue(
                sal_indices=[I0, I0 + 1, I0 + 2, I0 + 4, I0 + 5, I0 + 6]
            )

            # Resume queue and wait for all scripts to finish.
            await self.remote.cmd_resume.start(timeout=STD_TIMEOUT)

            queue = await self.assert_next_sample(self.remote.evt_queue)
            while queue.length > 0:
                queue = await self.assert_next_sample(self.remote.evt_queue)

            block_ids = set()
            state = await script_remote.evt_state.next(flush=False, timeout=STD_TIMEOUT)
            while (state := script_remote.evt_state.get_oldest()) is not None:
                if state.blockId:
                    block_ids.add(state.blockId)
            assert "BL1_O_20240228_000001" in block_ids
            assert "BL1_O_20240228_000002" in block_ids

    async def test_add_remove(self):
        """Test add, remove and showScript."""
        is_standard = False
        path = "script1"
        config = "wait_time: 1"  # give showScript time to run
        make_add_kwargs = MakeAddKwargs(
            isStandard=is_standard, path=path, config=config, descr="test_add_remove"
        )
        async with self.make_csc(initial_state=salobj.State.STANDBY):
            await self.assert_next_queue(enabled=False, running=True)
            await self.remote.cmd_start.start(timeout=STD_TIMEOUT)

            # Check that add fails when the queue is not enabled.
            with pytest.raises(salobj.AckError):
                add_kwargs = make_add_kwargs()
                await self.remote.cmd_add.set_start(**add_kwargs, timeout=STD_TIMEOUT)

            await self.remote.cmd_enable.start(timeout=STD_TIMEOUT)
            await self.assert_next_queue(enabled=True, running=True)

            # Pause the queue so we know what to expect of queue state.
            await self.remote.cmd_pause.start(timeout=STD_TIMEOUT)
            await self.assert_next_queue(running=False)

            # Add script I0; queue is empty, so location is irrelevant.
            add_kwargs = make_add_kwargs()
            ackcmd = await self.remote.cmd_add.set_start(
                **add_kwargs, timeout=STD_TIMEOUT
            )
            seq_num_0 = ackcmd.private_seqNum
            assert int(ackcmd.result) == I0
            await self.assert_next_queue(sal_indices=[I0])

            # Run showScript for a script that has not been configured.
            self.remote.evt_script.flush()
            ackcmd = await self.remote.cmd_showScript.set_start(
                scriptSalIndex=I0, timeout=STD_TIMEOUT
            )
            script_data = await self.remote.evt_script.next(
                flush=False, timeout=STD_TIMEOUT
            )
            assert script_data.cmdId == seq_num_0
            assert script_data.scriptSalIndex == I0
            assert script_data.isStandard == is_standard
            assert script_data.path == path
            assert script_data.processState == ScriptProcessState.LOADING
            assert script_data.timestampProcessStart > 0
            assert script_data.timestampProcessEnd == 0

            # Run showScript for a script that does not exist.
            with salobj.assertRaisesAckError():
                await self.remote.cmd_showScript.set_start(
                    scriptSalIndex=I0 - 1, timeout=STD_TIMEOUT
                )

            # Add script I0+1 last: test add last.
            add_kwargs = make_add_kwargs(location=Location.LAST)
            ackcmd = await self.remote.cmd_add.set_start(
                **add_kwargs, timeout=STD_TIMEOUT
            )
            seq_num1 = ackcmd.private_seqNum
            assert int(ackcmd.result) == I0 + 1
            await self.assert_next_queue(sal_indices=[I0, I0 + 1])

            # Add script I0+2 first: test add first.
            add_kwargs = make_add_kwargs(location=Location.FIRST)
            ackcmd = await self.remote.cmd_add.set_start(
                **add_kwargs, timeout=STD_TIMEOUT
            )
            assert int(ackcmd.result) == I0 + 2
            await self.assert_next_queue(sal_indices=[I0 + 2, I0, I0 + 1])

            # Add script I0+3 after I0+1: test add after last.
            add_kwargs = make_add_kwargs(
                location=Location.AFTER, locationSalIndex=I0 + 1
            )
            ackcmd = await self.remote.cmd_add.set_start(
                **add_kwargs, timeout=STD_TIMEOUT
            )
            seq_num3 = ackcmd.private_seqNum
            await self.assert_next_queue(sal_indices=[I0 + 2, I0, I0 + 1, I0 + 3])

            # Add script I0+4 after I0+2: test add after not-last.
            add_kwargs = make_add_kwargs(
                location=Location.AFTER, locationSalIndex=I0 + 2
            )
            await self.remote.cmd_add.set_start(**add_kwargs, timeout=STD_TIMEOUT)
            await self.assert_next_queue(
                sal_indices=[I0 + 2, I0 + 4, I0, I0 + 1, I0 + 3]
            )

            # Add script I0+5 before I0+2: test add before first.
            add_kwargs = make_add_kwargs(
                location=Location.BEFORE, locationSalIndex=I0 + 2
            )
            await self.remote.cmd_add.set_start(**add_kwargs, timeout=STD_TIMEOUT)
            await self.assert_next_queue(
                sal_indices=[I0 + 5, I0 + 2, I0 + 4, I0, I0 + 1, I0 + 3]
            )

            # Add script I0+6 before I0: test add before not first.
            add_kwargs = make_add_kwargs(location=Location.BEFORE, locationSalIndex=I0)
            await self.remote.cmd_add.set_start(**add_kwargs, timeout=STD_TIMEOUT)
            await self.assert_next_queue(
                sal_indices=[I0 + 5, I0 + 2, I0 + 4, I0 + 6, I0, I0 + 1, I0 + 3]
            )

            # Try some failed adds...
            # Incorrect path.
            add_kwargs = make_add_kwargs(path="bogus_script_name")
            with pytest.raises(salobj.AckError):
                await self.remote.cmd_add.set_start(**add_kwargs, timeout=STD_TIMEOUT)

            # Incorrect location.
            add_kwargs = make_add_kwargs(location=25)
            with pytest.raises(salobj.AckError):
                await self.remote.cmd_add.set_start(**add_kwargs, timeout=STD_TIMEOUT)

            # Incorrect locationSalIndex.
            add_kwargs = make_add_kwargs(location=Location.AFTER, locationSalIndex=4321)
            with pytest.raises(salobj.AckError):
                await self.remote.cmd_add.set_start(**add_kwargs, timeout=STD_TIMEOUT)

            # Make sure the incorrect add commands did not alter the queue.
            await self.remote.cmd_showQueue.start(timeout=STD_TIMEOUT)
            await self.assert_next_queue(
                sal_indices=[I0 + 5, I0 + 2, I0 + 4, I0 + 6, I0, I0 + 1, I0 + 3]
            )

            # Stop a few scripts, including one non-existent script.
            stop_data = self.make_stop_data(
                [I0 + 6, I0 + 5, I0 + 4, I0, 5432], terminate=False
            )
            await self.remote.cmd_stopScripts.start(stop_data, timeout=STD_TIMEOUT)
            queue_data = await self.assert_next_queue(
                sal_indices=[I0 + 2, I0 + 1, I0 + 3],
                past_sal_indices={I0 + 6, I0 + 5, I0 + 4, I0},
            )
            stopped_scripts = list(queue_data.pastSalIndices[0 : queue_data.pastLength])

            # Make sure all scripts are configured, then start the queue
            # and let it run.
            await self.wait_configured(I0 + 1, I0 + 2, I0 + 3)

            # Get script state for a script that has been configured
            # but is not running.
            self.remote.evt_script.flush()
            await self.remote.cmd_showScript.set_start(
                scriptSalIndex=I0 + 3, timeout=STD_TIMEOUT
            )
            while True:
                script_data = await self.remote.evt_script.next(
                    flush=False, timeout=STD_TIMEOUT
                )
                if (
                    script_data.scriptSalIndex == I0 + 3
                    and script_data.processState == ScriptProcessState.CONFIGURED
                ):
                    break
            assert script_data.scriptSalIndex == I0 + 3
            assert script_data.cmdId == seq_num3
            assert script_data.isStandard == is_standard
            assert script_data.path == path
            assert script_data.processState == ScriptProcessState.CONFIGURED
            assert script_data.timestampProcessStart > 0
            assert script_data.timestampProcessEnd == 0

            await self.remote.cmd_resume.start(timeout=STD_TIMEOUT)
            await self.assert_next_next_visit(sal_index=I0 + 2)
            await self.assert_next_queue(
                running=True,
                current_sal_index=I0 + 2,
                sal_indices=[I0 + 1, I0 + 3],
                past_sal_indices=stopped_scripts,
            )

            await self.assert_next_next_visit(sal_index=I0 + 1)
            await self.assert_next_queue(
                running=True,
                current_sal_index=I0 + 1,
                sal_indices=[I0 + 3],
                past_sal_indices=[I0 + 2] + stopped_scripts,
            )

            await self.assert_next_next_visit(sal_index=I0 + 3)
            await self.assert_next_queue(
                running=True,
                current_sal_index=I0 + 3,
                sal_indices=[],
                past_sal_indices=[I0 + 1, I0 + 2] + stopped_scripts,
            )
            await self.assert_next_queue(
                running=True,
                current_sal_index=0,
                sal_indices=[],
                past_sal_indices=[I0 + 3, I0 + 1, I0 + 2] + stopped_scripts,
            )

            # Test that nextVisitCanceled was not output.
            assert not (self.remote.evt_nextVisitCanceled.has_data)

            # Get script state for a script that has been run.
            self.remote.evt_script.flush()
            await self.remote.cmd_showScript.set_start(
                scriptSalIndex=I0 + 1, timeout=STD_TIMEOUT
            )
            while True:
                script_data = await self.remote.evt_script.next(
                    flush=False, timeout=STD_TIMEOUT
                )
                if script_data.scriptSalIndex == I0 + 1:
                    break
            assert script_data.scriptSalIndex == I0 + 1
            assert script_data.cmdId == seq_num1
            assert script_data.isStandard == is_standard
            assert script_data.path == path
            assert script_data.processState == ScriptProcessState.DONE
            assert script_data.timestampProcessStart > 0
            assert script_data.timestampProcessEnd > 0
            process_duration = (
                script_data.timestampProcessEnd - script_data.timestampProcessStart
            )
            assert process_duration > 0.9  # wait time is 1

            # Try to get script state for a non-existent script.
            with pytest.raises(salobj.AckError):
                await self.remote.cmd_showScript.set_start(
                    scriptSalIndex=3579, timeout=STD_TIMEOUT
                )

    async def check_add_log_level(self, log_level):
        """Test script log level when adding a script to the script queue."""
        async with self.make_csc(initial_state=salobj.State.ENABLED), salobj.Remote(
            domain=self.csc.domain, name="Script", index=I0
        ) as script_remote:
            await self.assert_next_queue(enabled=False, running=True)
            await self.assert_next_queue(enabled=True, running=True)

            # Pause the queue so we know what to expect of queue state.
            await self.remote.cmd_pause.start(timeout=STD_TIMEOUT)
            await self.assert_next_queue(running=False)

            await self.remote.cmd_add.set_start(
                logLevel=log_level,
                isStandard=False,
                path="script1",
                config="",
                location=Location.LAST,
                descr="test_add_log_level",
                timeout=STD_TIMEOUT,
            )
            await self.assert_next_queue(running=False, sal_indices=[I0])

            # Wait for the script queue to launch and configure the script.
            topic = script_remote.evt_state

            script_state = await self.get_next_sample(topic)

            assert script_state.state == ScriptState.UNCONFIGURED
            script_state = await script_remote.evt_state.next(
                flush=False, timeout=STD_TIMEOUT
            )
            assert script_state.state == ScriptState.CONFIGURED

            # Check initial log level.
            data = await self.get_next_sample(topic=script_remote.evt_logLevel)
            assert data.level == logging.INFO

            # If log_level != 0 check final log level,
            # else check that no second log level was output.
            if log_level != 0:
                data = await script_remote.evt_logLevel.next(
                    flush=False, timeout=STD_TIMEOUT
                )
                assert data.level == log_level
            else:
                with pytest.raises(asyncio.TimeoutError):
                    await script_remote.evt_logLevel.next(flush=False, timeout=0.01)

            # Wait for the scrip to be enabled, then run the queue.
            await self.wait_configured(I0)
            await self.remote.cmd_resume.start(timeout=STD_TIMEOUT)
            await self.assert_next_queue(
                enabled=True, running=True, current_sal_index=I0
            )
            await self.assert_next_queue(
                enabled=True, running=True, past_sal_indices=[I0]
            )

    async def get_next_sample(self, topic):
        sample = await topic.next(flush=False, timeout=STD_TIMEOUT)
        while sample.private_sndStamp <= self.events_oldest_timestamp:
            print(f"Discarding old {sample=}.")
            sample = await topic.next(flush=False, timeout=STD_TIMEOUT)

        return sample

    async def check_bin_script_initial_state(self, cmdline_args):
        for initial_state, index in (
            (None, SalIndex.MAIN_TEL),
            (salobj.State.STANDBY, SalIndex.AUX_TEL),
            (salobj.State.DISABLED, SalIndex.MAIN_TEL),
            (salobj.State.ENABLED, SalIndex.AUX_TEL),
        ):
            salobj.set_random_lsst_dds_partition_prefix()
            with self.subTest(initial_state=initial_state, index=index):
                await self.check_bin_script(
                    name="ScriptQueue",
                    index=int(index),
                    exe_name="run_script_queue",
                    initial_state=initial_state,
                    cmdline_args=cmdline_args,
                )

    async def test_add_nonzero_log_level(self):
        """Test addding a script with a non-zero log level."""
        # pick a level that does not match the default
        # to make it easier to see that the level has changed
        log_level = logging.INFO - 1
        await self.check_add_log_level(log_level=log_level)

    async def test_add_zero_log_level(self):
        """Test addding a script with log level 0, meaning don't change it."""
        await self.check_add_log_level(log_level=0)

    async def test_add_and_pause(self):
        """Test adding a script with a pause checkpoint."""
        async with self.make_csc(initial_state=salobj.State.DISABLED), salobj.Remote(
            domain=self.csc.domain, name="Script", index=I0
        ) as script_remote:
            await self.assert_next_queue(enabled=False, running=True)
            await self.remote.cmd_enable.start(timeout=STD_TIMEOUT)
            await self.assert_next_queue(enabled=True, running=True)

            # Queue and run a script that pauses at the "start" checkpoint.
            await self.remote.cmd_add.set_start(
                pauseCheckpoint="start",
                isStandard=False,
                path="script1",
                config="",
                location=Location.LAST,
                descr="test_add",
                timeout=STD_TIMEOUT,
            )
            await self.assert_next_queue(running=True, sal_indices=[I0])
            await self.assert_next_queue(running=True, current_sal_index=I0)
            timeout = STD_TIMEOUT

            # Validate the expected script states through PAUSED.
            # The first timeout is longer becauase scripts are slow to load.
            for expected_script_state in (
                ScriptState.UNCONFIGURED,
                ScriptState.CONFIGURED,
                # Group ID set
                ScriptState.CONFIGURED,
                ScriptState.RUNNING,
                ScriptState.PAUSED,
            ):
                script_state = await script_remote.evt_state.next(
                    flush=False, timeout=timeout
                )
                assert script_state.state == expected_script_state
                timeout = STD_TIMEOUT

            # Resume the script and wait for the queue to report it done.
            await script_remote.cmd_resume.start(timeout=STD_TIMEOUT)
            await self.assert_next_queue(running=True, past_sal_indices=[I0])

    async def test_add_and_stop(self):
        """Test adding a script with a stop checkpoint."""
        async with self.make_csc(initial_state=salobj.State.DISABLED), salobj.Remote(
            domain=self.csc.domain, name="Script", index=I0
        ) as script_remote:
            await self.assert_next_queue(enabled=False, running=True)
            await self.remote.cmd_enable.start(timeout=STD_TIMEOUT)
            await self.assert_next_queue(enabled=True, running=True)

            # Queue and run a script that stops at the "start" checkpoint.
            await self.remote.cmd_add.set_start(
                stopCheckpoint="start",
                isStandard=False,
                path="script1",
                config="",
                location=Location.LAST,
                descr="test_add",
                timeout=STD_TIMEOUT,
            )
            await self.assert_next_queue(running=True, sal_indices=[I0])
            await self.assert_next_queue(running=True, current_sal_index=I0)

            # Validate the expected script states through STOPPED.
            # The first timeout is longer because scripts are slow to load.
            for expected_script_state in (
                ScriptState.UNCONFIGURED,
                ScriptState.CONFIGURED,
                # Group ID set
                ScriptState.CONFIGURED,
                ScriptState.RUNNING,
                ScriptState.STOPPING,
                ScriptState.STOPPED,
            ):
                script_state = await self.get_next_sample(script_remote.evt_state)
                assert ScriptState(script_state.state) == expected_script_state

            await self.assert_next_queue(running=True, past_sal_indices=[I0])

    async def test_bin_script_state_with_test_scripts(self):
        """Test the --state argument of run_script_queue

        Note that other bin script tests are in a separate class below,
        but this test relies on salobj.BaseCscTestCase.
        """

        await self.check_bin_script_initial_state(
            cmdline_args=(
                "--standard",
                self.standardpath,
                "--external",
                self.externalpath,
            )
        )

    @unittest.skipIf(
        standardscripts is None or externalscripts is None,
        "Could not import ts_standardscripts and/or ts_externalscripts.",
    )
    async def test_bin_script_state(self):
        """Test the --state argument of run_script_queue

        Note that other bin script tests are in a separate class below,
        but this test relies on salobj.BaseCscTestCase.

        This test is skipped if either ts_standardscripts/ts_externalscripts
        fails to import.
        """
        await self.check_bin_script_initial_state(cmdline_args=())

    async def test_process_state(self):
        """Test the processState value of the queue event."""
        async with self.make_csc(initial_state=salobj.State.DISABLED):
            await self.assert_next_sample(
                topic=self.remote.evt_softwareVersions,
                cscVersion=scriptqueue.__version__,
                subsystemVersions="",
            )

            make_add_kwargs = MakeAddKwargs(descr="test_process_state")

            await self.assert_next_queue(enabled=False, running=True)

            await self.remote.cmd_enable.start(timeout=STD_TIMEOUT)
            await self.assert_next_queue(enabled=True, running=True)

            # Pause the queue so we know what to expect of queue state.
            await self.remote.cmd_pause.start(timeout=STD_TIMEOUT)
            await self.assert_next_queue(running=False)

            # Add script I0 that will fail, and so pause the queue.
            add_kwargs = make_add_kwargs(config="fail_run: True")
            ackcmd = await self.remote.cmd_add.set_start(
                **add_kwargs, timeout=STD_TIMEOUT
            )
            seq_num0 = ackcmd.private_seqNum
            await self.assert_next_queue(sal_indices=[I0])

            # Add script I0+1 that we will terminate.
            add_kwargs = make_add_kwargs(config="")
            ackcmd = await self.remote.cmd_add.set_start(
                **add_kwargs, timeout=STD_TIMEOUT
            )
            seq_num1 = ackcmd.private_seqNum
            await self.assert_next_queue(sal_indices=[I0, I0 + 1])

            # Add script I0+2 that we will allow to run normally.
            add_kwargs = make_add_kwargs(config="")
            ackcmd = await self.remote.cmd_add.set_start(
                **add_kwargs, timeout=STD_TIMEOUT
            )
            seq_num2 = ackcmd.private_seqNum
            await self.assert_next_queue(sal_indices=[I0, I0 + 1, I0 + 2])

            # Wait for all scripts to be configured, so future script output
            # is due to the scripts being run or terminated.
            await self.wait_configured(I0, I0 + 1, I0 + 2)

            # Run the queue and let it pause on failure.
            await self.remote.cmd_resume.start(timeout=STD_TIMEOUT)
            await self.assert_next_queue(
                running=True,
                current_sal_index=I0,
                sal_indices=[I0 + 1, I0 + 2],
                past_sal_indices=[],
            )
            await self.assert_next_queue(
                running=False,
                current_sal_index=I0,
                sal_indices=[I0 + 1, I0 + 2],
                past_sal_indices=[],
            )

            script_data0 = self.remote.evt_script.get()
            assert script_data0.cmdId == seq_num0
            assert script_data0.scriptSalIndex == I0
            assert script_data0.processState == ScriptProcessState.DONE
            assert script_data0.scriptState == ScriptState.FAILED

            # Terminate the next script.
            stop_data = self.make_stop_data([I0 + 1], terminate=True)
            await self.remote.cmd_stopScripts.start(stop_data, timeout=STD_TIMEOUT)
            await self.assert_next_queue(
                running=False,
                current_sal_index=I0,
                sal_indices=[I0 + 2],
                past_sal_indices=[I0 + 1],
            )

            script_data1 = self.remote.evt_script.get()
            assert script_data1.cmdId == seq_num1
            assert script_data1.scriptSalIndex == I0 + 1
            assert script_data1.processState == ScriptProcessState.TERMINATED
            assert script_data1.scriptState == ScriptState.CONFIGURED

            # Resume the queue and let I0+2 run.
            await self.remote.cmd_resume.start(timeout=STD_TIMEOUT)
            await self.assert_next_queue(
                running=True,
                current_sal_index=I0 + 2,
                sal_indices=[],
                past_sal_indices=[I0, I0 + 1],
            )
            await self.assert_next_queue(
                running=True,
                current_sal_index=0,
                sal_indices=[],
                past_sal_indices=[I0 + 2, I0, I0 + 1],
            )

            script_data2 = self.remote.evt_script.get()
            assert script_data2.cmdId == seq_num2
            assert script_data2.scriptSalIndex == I0 + 2
            assert script_data2.processState == ScriptProcessState.DONE
            assert script_data2.scriptState == ScriptState.DONE

    async def test_unloadable_script(self):
        """Test adding a script that fails while loading."""
        async with self.make_csc(initial_state=salobj.State.DISABLED):
            await self.assert_next_queue(enabled=False, running=True)

            await self.remote.cmd_enable.start(timeout=STD_TIMEOUT)
            await self.assert_next_queue(enabled=True, running=True)

            await self.remote.cmd_add.set_start(
                isStandard=True,
                path="unloadable",
                config="",
                location=Location.LAST,
                locationSalIndex=0,
                descr="test_unloadable_script",
                timeout=STD_TIMEOUT,
            )

            await self.assert_next_queue(enabled=True, running=True, sal_indices=[I0])

            script_data0 = await self.get_next_sample(self.remote.evt_script)

            assert script_data0.processState == ScriptProcessState.LOADING
            script_data0 = await self.remote.evt_script.next(
                flush=False, timeout=STD_TIMEOUT
            )
            assert script_data0.processState == ScriptProcessState.LOADFAILED

            await self.assert_next_queue(
                enabled=True, running=True, past_sal_indices=[I0]
            )

    async def test_move(self):
        """Test move, pause and showQueue"""
        async with self.make_csc(initial_state=salobj.State.DISABLED):
            await self.assert_next_queue(enabled=False, running=True)

            # Pause the queue so we know what to expect of queue state.
            # Also check that pause works while not enabled.
            await self.remote.cmd_pause.start(timeout=STD_TIMEOUT)
            await self.assert_next_queue(enabled=False, running=False)

            await self.remote.cmd_enable.start(timeout=STD_TIMEOUT)
            await self.assert_next_queue(enabled=True, running=False)

            # Queue scripts I0, I0+1 and I0+2.
            sal_indices = [I0, I0 + 1, I0 + 2]
            for i, index in enumerate(sal_indices):
                await self.remote.cmd_add.set_start(
                    isStandard=True,
                    path=os.path.join("subdir", "script3"),
                    config="wait_time: 0.1",
                    location=Location.LAST,
                    descr=f"test_move {i}",
                    timeout=STD_TIMEOUT,
                )
                await self.assert_next_queue(sal_indices=sal_indices[0 : i + 1])

            # Try to move a script that does not exist
            with salobj.assertRaisesAckError():
                await self.remote.cmd_move.set_start(
                    scriptSalIndex=I0 - 1, location=Location.FIRST, timeout=STD_TIMEOUT
                )

            # Move I0+2 first.
            await self.remote.cmd_move.set_start(
                scriptSalIndex=I0 + 2, location=Location.FIRST, timeout=STD_TIMEOUT
            )
            await self.assert_next_queue(sal_indices=[I0 + 2, I0, I0 + 1])

            # Move I0+2 first again; this should be a no-op
            # but it should still output the queue event.
            await self.remote.cmd_move.set_start(
                scriptSalIndex=I0 + 2, location=Location.FIRST, timeout=STD_TIMEOUT
            )
            await self.assert_next_queue(sal_indices=[I0 + 2, I0, I0 + 1])

            # Move I0 last.
            await self.remote.cmd_move.set_start(
                scriptSalIndex=I0, location=Location.LAST, timeout=STD_TIMEOUT
            )
            await self.assert_next_queue(sal_indices=[I0 + 2, I0 + 1, I0])

            # Move I0 last again; this should be a no-op,
            # but it should still output the queue event.
            await self.remote.cmd_move.set_start(
                scriptSalIndex=I0, location=Location.LAST, timeout=STD_TIMEOUT
            )
            await self.assert_next_queue(sal_indices=[I0 + 2, I0 + 1, I0])

            # Move I0 before I0+2: before first.
            await self.remote.cmd_move.set_start(
                scriptSalIndex=I0,
                location=Location.BEFORE,
                locationSalIndex=I0 + 2,
                timeout=STD_TIMEOUT,
            )
            await self.assert_next_queue(sal_indices=[I0, I0 + 2, I0 + 1])

            # Move I0+1 before I0+2: before not-first.
            await self.remote.cmd_move.set_start(
                scriptSalIndex=I0 + 1,
                location=Location.BEFORE,
                locationSalIndex=I0 + 2,
                timeout=STD_TIMEOUT,
            )
            await self.assert_next_queue(sal_indices=[I0, I0 + 1, I0 + 2])

            # Move I0 after I0+2: after last.
            await self.remote.cmd_move.set_start(
                scriptSalIndex=I0,
                location=Location.AFTER,
                locationSalIndex=I0 + 2,
                timeout=STD_TIMEOUT,
            )
            await self.assert_next_queue(sal_indices=[I0 + 1, I0 + 2, I0])

            # Move I0+1 after I0+2: after not-last.
            await self.remote.cmd_move.set_start(
                scriptSalIndex=I0 + 1,
                location=Location.AFTER,
                locationSalIndex=I0 + 2,
                timeout=STD_TIMEOUT,
            )
            await self.assert_next_queue(sal_indices=[I0 + 2, I0 + 1, I0])

            # Move I0 after itself: this should be a no-op,
            # but it should still output the queue event.
            await self.remote.cmd_move.set_start(
                scriptSalIndex=I0,
                location=Location.AFTER,
                locationSalIndex=I0,
                timeout=STD_TIMEOUT,
            )
            await self.assert_next_queue(sal_indices=[I0 + 2, I0 + 1, I0])

            # Move I0+1 before itself: this should be a no-op
            # but it should still output the queue event.
            await self.remote.cmd_move.set_start(
                scriptSalIndex=I0 + 1,
                location=Location.BEFORE,
                locationSalIndex=I0 + 1,
                timeout=STD_TIMEOUT,
            )
            await self.assert_next_queue(sal_indices=[I0 + 2, I0 + 1, I0])

            # Try some incorrect moves.
            with pytest.raises(salobj.AckError):
                # no such script
                await self.remote.cmd_move.set_start(
                    scriptSalIndex=1234, location=Location.LAST, timeout=STD_TIMEOUT
                )

            with pytest.raises(salobj.AckError):
                # No such location.
                await self.remote.cmd_move.set_start(
                    scriptSalIndex=I0 + 1, location=21, timeout=STD_TIMEOUT
                )

            with pytest.raises(salobj.AckError):
                # No such locationSalIndex.
                await self.remote.cmd_move.set_start(
                    scriptSalIndex=I0 + 1,
                    location=Location.BEFORE,
                    locationSalIndex=1234,
                    timeout=STD_TIMEOUT,
                )

            # Try incorrect index and the same "before" locationSalIndex.
            with pytest.raises(salobj.AckError):
                # No such scriptSalIndex; no such locationSalIndex.
                await self.remote.cmd_move.set_start(
                    scriptSalIndex=1234,
                    location=Location.BEFORE,
                    locationSalIndex=1234,
                    timeout=STD_TIMEOUT,
                )

            # Try incorrect index and the same "after" locationSalIndex.
            with pytest.raises(salobj.AckError):
                # No such scriptSalIndex; no such locationSalIndex.
                await self.remote.cmd_move.set_start(
                    scriptSalIndex=1234,
                    location=Location.AFTER,
                    locationSalIndex=1234,
                    timeout=STD_TIMEOUT,
                )

            # Make sure those commands did not alter the queue.
            await self.remote.cmd_showQueue.start(timeout=STD_TIMEOUT)
            await self.assert_next_queue(sal_indices=[I0 + 2, I0 + 1, I0])

            await asyncio.wait_for(self.csc.model.terminate_all(), timeout=STD_TIMEOUT)
            for i in range(len(sal_indices)):
                queue_data = await self.remote.evt_queue.next(
                    flush=False, timeout=STD_TIMEOUT
                )
            assert queue_data.length == 0

    async def test_requeue(self):
        """Test requeue, move and terminate"""
        async with self.make_csc(initial_state=salobj.State.DISABLED):
            await self.assert_next_queue(enabled=False, running=True)

            await self.remote.cmd_enable.start(timeout=STD_TIMEOUT)
            await self.assert_next_queue(enabled=True, running=True)

            # Pause the queue so we know what to expect of queue state.
            await self.remote.cmd_pause.start(timeout=STD_TIMEOUT)
            await self.assert_next_queue(running=False)

            # Queue scripts I0, I0+1 and I0+2.
            sal_indices = [I0, I0 + 1, I0 + 2]
            for i, index in enumerate(sal_indices):
                await self.remote.cmd_add.set_start(
                    isStandard=True,
                    path="script2",
                    config="wait_time: 0.1",
                    location=Location.LAST,
                    descr=f"test_requeue {i}",
                    timeout=STD_TIMEOUT,
                )
                await self.assert_next_queue(sal_indices=sal_indices[0 : i + 1])

            # Disable the queue and make sure requeue, move and resume fail
            # (I added some jobs before disabling so we have scripts
            # to try to requeue and move).
            await self.remote.cmd_disable.start(timeout=STD_TIMEOUT)
            await self.assert_next_queue(
                enabled=False, running=False, sal_indices=sal_indices
            )

            with pytest.raises(salobj.AckError):
                await self.remote.cmd_requeue.set_start(
                    scriptSalIndex=I0, location=Location.LAST, timeout=STD_TIMEOUT
                )

            with pytest.raises(salobj.AckError):
                await self.remote.cmd_move.set_start(
                    scriptSalIndex=I0 + 2, location=Location.FIRST, timeout=STD_TIMEOUT
                )

            with pytest.raises(salobj.AckError):
                await self.remote.cmd_resume.start(timeout=STD_TIMEOUT)

            # Re-enable the queue and proceed with the rest of the test.
            await self.remote.cmd_enable.start(timeout=STD_TIMEOUT)
            await self.assert_next_queue(
                enabled=True, running=False, sal_indices=sal_indices
            )

            # Requeue a script that does not exist
            with salobj.assertRaisesAckError():
                await self.remote.cmd_requeue.set_start(
                    scriptSalIndex=I0 - 1, location=Location.LAST, timeout=STD_TIMEOUT
                )

            # Requeue I0 to last, creating I0+3.
            await self.remote.cmd_requeue.set_start(
                scriptSalIndex=I0, location=Location.LAST, timeout=STD_TIMEOUT
            )
            await self.assert_next_queue(sal_indices=[I0, I0 + 1, I0 + 2, I0 + 3])

            # Requeue I0 to first, creating I0+4.
            await self.remote.cmd_requeue.set_start(
                scriptSalIndex=I0, location=Location.FIRST, timeout=STD_TIMEOUT
            )
            await self.assert_next_queue(
                sal_indices=[I0 + 4, I0, I0 + 1, I0 + 2, I0 + 3]
            )

            # Requeue I0+2 to before I0+4 (which is first), creating I0+5.
            await self.remote.cmd_requeue.set_start(
                scriptSalIndex=I0 + 2,
                location=Location.BEFORE,
                locationSalIndex=I0 + 4,
                timeout=STD_TIMEOUT,
            )
            await self.assert_next_queue(
                sal_indices=[I0 + 5, I0 + 4, I0, I0 + 1, I0 + 2, I0 + 3]
            )

            # Requeue I0+3 to before itself (which is not first), creating I0+6
            await self.remote.cmd_requeue.set_start(
                scriptSalIndex=I0 + 3,
                location=Location.BEFORE,
                locationSalIndex=I0 + 3,
                timeout=STD_TIMEOUT,
            )
            await self.assert_next_queue(
                sal_indices=[I0 + 5, I0 + 4, I0, I0 + 1, I0 + 2, I0 + 6, I0 + 3]
            )

            # Requeue I0+3 to after itself (which is last), creating I0+7.
            await self.remote.cmd_requeue.set_start(
                scriptSalIndex=I0 + 3,
                location=Location.AFTER,
                locationSalIndex=I0 + 3,
                timeout=STD_TIMEOUT,
            )
            await self.assert_next_queue(
                sal_indices=[I0 + 5, I0 + 4, I0, I0 + 1, I0 + 2, I0 + 6, I0 + 3, I0 + 7]
            )

            # Requeue I0+5 to last, creating I0+8.
            await self.remote.cmd_requeue.set_start(
                scriptSalIndex=I0 + 5, location=Location.LAST, timeout=STD_TIMEOUT
            )
            await self.assert_next_queue(
                sal_indices=[
                    I0 + 5,
                    I0 + 4,
                    I0 + 0,
                    I0 + 1,
                    I0 + 2,
                    I0 + 6,
                    I0 + 3,
                    I0 + 7,
                    I0 + 8,
                ]
            )

            # Stop all scripts except I0+1 and I0+2.
            stop_data = self.make_stop_data(
                [I0 + 5, I0 + 4, I0, I0 + 6, I0 + 3, I0 + 7, I0 + 8], terminate=False
            )
            await self.remote.cmd_stopScripts.start(stop_data, timeout=STD_TIMEOUT)
            queue_data = await self.assert_next_queue(
                sal_indices=[I0 + 1, I0 + 2],
                past_sal_indices={I0 + 5, I0 + 4, I0, I0 + 6, I0 + 3, I0 + 7, I0 + 8},
            )
            stopped_scripts = list(queue_data.pastSalIndices[0 : queue_data.pastLength])

            await self.wait_configured(I0 + 1, I0 + 2)

            # Run the queue and let it finish.
            await self.remote.cmd_resume.start(timeout=STD_TIMEOUT)
            await self.assert_next_queue(
                running=True,
                current_sal_index=I0 + 1,
                sal_indices=[I0 + 2],
                past_sal_indices=stopped_scripts,
            )
            await self.assert_next_queue(
                running=True,
                current_sal_index=I0 + 2,
                sal_indices=[],
                past_sal_indices=[I0 + 1] + stopped_scripts,
            )
            await self.assert_next_queue(
                running=True,
                current_sal_index=0,
                sal_indices=[],
                past_sal_indices=[I0 + 2, I0 + 1] + stopped_scripts,
            )

            # Pause while we requeue from history.
            await self.remote.cmd_pause.start(timeout=STD_TIMEOUT)
            await self.assert_next_queue(
                running=False,
                current_sal_index=0,
                sal_indices=[],
                past_sal_indices=[I0 + 2, I0 + 1] + stopped_scripts,
            )

            # Requeue a script from the history queue, creating I0+9.
            await self.remote.cmd_requeue.set_start(
                scriptSalIndex=I0 + 1, location=Location.FIRST, timeout=STD_TIMEOUT
            )
            await self.assert_next_queue(
                running=False,
                current_sal_index=0,
                sal_indices=[I0 + 9],
                past_sal_indices=[I0 + 2, I0 + 1] + stopped_scripts,
            )

            # Wait for script I0+9 to be configured, then
            # run the queue and let the script finish.
            await self.wait_configured(I0 + 9)
            await self.remote.cmd_resume.start(timeout=STD_TIMEOUT)
            await self.assert_next_queue(
                running=True,
                current_sal_index=I0 + 9,
                sal_indices=[],
                past_sal_indices=[I0 + 2, I0 + 1] + stopped_scripts,
            )
            await self.assert_next_queue(
                running=True,
                current_sal_index=0,
                sal_indices=[],
                past_sal_indices=[I0 + 9, I0 + 2, I0 + 1] + stopped_scripts,
            )

    async def test_next_visit_canceled(self):
        """Test the nextVisitCanceled event."""
        async with self.make_csc(initial_state=salobj.State.DISABLED):
            make_add_kwargs = MakeAddKwargs(descr="test_next_visit_canceled")
            await self.assert_next_queue(enabled=False, running=True)

            await self.remote.cmd_enable.start(timeout=STD_TIMEOUT)
            await self.assert_next_queue(enabled=True, running=True)

            # Pause the queue so we know what to expect of queue state.
            await self.remote.cmd_pause.start(timeout=STD_TIMEOUT)
            await self.assert_next_queue(running=False)

            # Queue 4 scripts: enough to test different means of
            # canceling the top script.
            # Make the scripts take long enough to run that the first one
            # keeps running while we set and clear group IDs for the
            # remaining scripts.
            sal_indices = []
            for i in range(4):
                sal_indices.append(I0 + i)
                add_kwargs = make_add_kwargs(config="wait_time: 10")
                await self.remote.cmd_add.set_start(**add_kwargs, timeout=STD_TIMEOUT)
                await self.assert_next_queue(sal_indices=sal_indices)

            await self.wait_configured(*sal_indices)
            await self.remote.cmd_resume.start(timeout=STD_TIMEOUT)
            await self.assert_next_queue(running=True, sal_indices=sal_indices)
            await self.assert_next_next_visit(sal_index=I0)
            await self.assert_next_queue(
                running=True, current_sal_index=I0, sal_indices=[I0 + 1, I0 + 2, I0 + 3]
            )
            await self.assert_next_next_visit(sal_index=I0 + 1)

            # Remove I0+1; the value of terminate doesn't matter
            # because the script is not running.
            print(f"remove script {I0+1} from the queue")
            stop_data = self.make_stop_data([I0 + 1], terminate=False)
            await self.remote.cmd_stopScripts.start(stop_data, timeout=STD_TIMEOUT)
            await self.assert_next_queue(
                running=True,
                current_sal_index=I0,
                sal_indices=[I0 + 2, I0 + 3],
                past_sal_indices=[I0 + 1],
            )
            await self.assert_next_next_visit_canceled(sal_index=I0 + 1)
            await self.assert_next_next_visit(sal_index=I0 + 2)

            # Move I0+2 to the end.
            print(f"move script {I0+2} to the end of the queue")
            await self.remote.cmd_move.set_start(
                scriptSalIndex=I0 + 2, location=Location.LAST, timeout=STD_TIMEOUT
            )
            await self.assert_next_queue(
                running=True,
                current_sal_index=I0,
                sal_indices=[I0 + 3, I0 + 2],
                past_sal_indices=[I0 + 1],
            )
            await self.assert_next_next_visit_canceled(sal_index=I0 + 2)
            await self.assert_next_next_visit(sal_index=I0 + 3)
            stop_data = self.make_stop_data([I0, I0 + 2, I0 + 3], terminate=False)
            await self.remote.cmd_stopScripts.start(stop_data, timeout=STD_TIMEOUT)
            await self.assert_next_queue(
                running=True,
                current_sal_index=0,
                sal_indices=[],
                past_sal_indices={I0, I0 + 2, I0 + 3, I0 + 1},
            )

    async def test_show_available_scripts(self):
        """Test the showAvailableScripts command."""
        async with self.make_csc(initial_state=salobj.State.DISABLED):
            # Make sure showAvailableScripts fails when not enabled.
            with pytest.raises(salobj.AckError):
                await self.remote.cmd_showAvailableScripts.start(timeout=STD_TIMEOUT)

            await self.remote.cmd_enable.start(timeout=STD_TIMEOUT)

            # The queue should output available scripts once at startup.
            available_scripts0 = await self.get_next_sample(
                self.remote.evt_availableScripts
            )

            with pytest.raises(asyncio.TimeoutError):
                await self.remote.evt_availableScripts.next(flush=False, timeout=0.1)

            # Ask for available scripts.
            await self.remote.cmd_showAvailableScripts.start(timeout=STD_TIMEOUT)

            available_scripts1 = await self.remote.evt_availableScripts.next(
                flush=False, timeout=STD_TIMEOUT
            )

            expected_std_set = set(
                [
                    "script1",
                    "script2",
                    "unloadable",
                    "subdir/script3",
                    "subdir/subsubdir/script4",
                ]
            )
            expected_ext_set = set(
                ["script1", "script5", "subdir/script3", "subdir/script6"]
            )
            for available_scripts in (available_scripts0, available_scripts1):
                standard_set = set(available_scripts.standard.split(":"))
                external_set = set(available_scripts.external.split(":"))
                assert standard_set == expected_std_set
                assert external_set == expected_ext_set

            # Disable the queue and again make sure showAvailableScripts fails.
            await self.remote.cmd_disable.start(timeout=STD_TIMEOUT)

            with pytest.raises(salobj.AckError):
                await self.remote.cmd_showAvailableScripts.start(timeout=STD_TIMEOUT)

    async def test_show_schema(self):
        """Test the showSchema command."""
        async with self.make_csc(initial_state=salobj.State.DISABLED):
            is_standard = False
            path = "script1"
            await self.assert_next_queue(enabled=False, running=True)
            self.remote.cmd_showSchema.set(isStandard=is_standard, path=path)

            # Make sure showSchema fails when not enabled.
            with pytest.raises(salobj.AckError):
                await self.remote.cmd_showSchema.start(timeout=STD_TIMEOUT)

            await self.remote.cmd_enable.start(timeout=STD_TIMEOUT)
            await self.assert_next_queue(enabled=True, running=True)

            await self.remote.cmd_showSchema.start(timeout=STD_TIMEOUT)
            data = await self.remote.evt_configSchema.next(
                flush=False, timeout=STD_TIMEOUT
            )
            assert data.isStandard == is_standard
            assert data.path == path
            schema = yaml.safe_load(data.configSchema)
            assert schema == salobj.TestScript.get_schema()

    async def test_show_queue(self):
        """Test the showQueue command."""
        async with self.make_csc(initial_state=salobj.State.DISABLED):
            await self.assert_next_queue(enabled=False, running=True)

            # Make sure showQueue fails when not enabled.
            with pytest.raises(salobj.AckError):
                await self.remote.cmd_showQueue.start(timeout=STD_TIMEOUT)

            await self.remote.cmd_enable.start(timeout=STD_TIMEOUT)
            await self.assert_next_queue(enabled=True, running=True)

            # Make sure we have no more queue events.
            with pytest.raises(asyncio.TimeoutError):
                await self.remote.evt_queue.next(flush=False, timeout=0.1)

            await self.remote.cmd_showQueue.start(timeout=STD_TIMEOUT)
            await self.assert_next_queue(enabled=True, running=True)

            # Make sure disabling the queue outputs the queue event,
            # with runnable False, and disables the showQueue command.
            await self.remote.cmd_disable.start(timeout=STD_TIMEOUT)
            await self.assert_next_queue(enabled=False, running=True)
            with pytest.raises(asyncio.TimeoutError):
                await self.remote.evt_queue.next(flush=False, timeout=0.1)
            with pytest.raises(salobj.AckError):
                await self.remote.cmd_showQueue.start(timeout=STD_TIMEOUT)

    async def wait_configured(self, *sal_indices):
        """Wait for the specified scripts to be configured.

        Call this before running the queue if you want the queue data
        to be predictable; otherwise the queue may start up with
        no script running.
        """
        print(f"wait_configured({sal_indices}")
        for sal_index in sal_indices:
            print(f"waiting for script {sal_index} to be loaded")
            script_info = self.csc.model.get_script_info(
                sal_index, search_history=False
            )
            await asyncio.wait_for(script_info.start_task, timeout=STD_TIMEOUT)
            print(f"waiting for script {sal_index} to be configured")
            await asyncio.wait_for(script_info.config_task, STD_TIMEOUT)


class CmdLineTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        salobj.set_random_lsst_dds_partition_prefix()
        self.index = 1
        try:
            self.default_standardpath = scriptqueue.get_default_scripts_dir(
                is_standard=True
            )
        except ImportError:
            self.default_standardpath = None

        try:
            self.default_externalpath = scriptqueue.get_default_scripts_dir(
                is_standard=False
            )
        except ImportError:
            self.default_externalpath = None

        self.datadir = os.path.abspath(os.path.join(os.path.dirname(__file__), "data"))
        self.testdata_standardpath = os.path.join(self.datadir, "standard")
        self.testdata_externalpath = os.path.join(self.datadir, "external")
        self.badpath = os.path.join(self.datadir, "not_a_directory")
        self.events_oldest_timestamp = utils.current_tai()

    async def get_next_sample(self, topic):
        sample = await topic.next(flush=False, timeout=STD_TIMEOUT)
        while sample.private_sndStamp <= self.events_oldest_timestamp:
            print(f"Discarding old {sample=}.")
            sample = await topic.next(flush=False, timeout=STD_TIMEOUT)

        return sample

    async def test_run_with_standard_and_external(self):
        exe_name = "run_script_queue"
        exe_path = shutil.which(exe_name)
        if exe_path is None:
            self.fail(
                f"Could not find bin script {exe_name}; did you setup and scons this package?"
            )

        async with salobj.Domain() as domain, salobj.Remote(
            domain=domain, name="ScriptQueue", index=self.index
        ) as remote:
            process = await asyncio.create_subprocess_exec(
                exe_name,
                str(self.index),
                "--standard",
                self.testdata_standardpath,
                "--external",
                self.testdata_externalpath,
                "--verbose",
            )
            try:
                summaryState_data = await self.get_next_sample(remote.evt_summaryState)

                assert summaryState_data.summaryState == salobj.State.STANDBY

                rootDir_data = await self.get_next_sample(remote.evt_rootDirectories)

                assert os.path.samefile(
                    rootDir_data.standard, self.testdata_standardpath
                )
                assert os.path.samefile(
                    rootDir_data.external, self.testdata_externalpath
                )

                ackcmd = await remote.cmd_exitControl.start(timeout=STD_TIMEOUT)
                assert ackcmd.ack == salobj.SalRetCode.CMD_COMPLETE
                summaryState_data = await remote.evt_summaryState.next(
                    flush=False, timeout=STD_TIMEOUT
                )
                assert summaryState_data.summaryState == salobj.State.OFFLINE

                await asyncio.wait_for(process.wait(), timeout=STD_TIMEOUT)
            except Exception:
                if process.returncode is None:
                    process.terminate()
                raise

    @unittest.skipIf(
        standardscripts is None or externalscripts is None,
        "Could not import ts_standardscripts and/or ts_externalscripts.",
    )
    async def test_run_default_standard_external(self):
        exe_name = "run_script_queue"
        exe_path = shutil.which(exe_name)
        if exe_path is None:
            self.fail(
                f"Could not find bin script {exe_name}; did you setup and scons this package?"
            )

        async with salobj.Domain() as domain, salobj.Remote(
            domain=domain, name="ScriptQueue", index=self.index
        ) as remote:
            process = await asyncio.create_subprocess_exec(
                exe_name, str(self.index), "--verbose"
            )
            try:
                summaryState_data = await self.get_next_sample(remote.evt_summaryState)
                assert summaryState_data.summaryState == salobj.State.STANDBY

                rootDir_data = await self.get_next_sample(remote.evt_rootDirectories)
                assert os.path.samefile(
                    rootDir_data.standard, self.default_standardpath
                )
                assert os.path.samefile(
                    rootDir_data.external, self.default_externalpath
                )

                ackcmd = await remote.cmd_exitControl.start(timeout=STD_TIMEOUT)
                assert ackcmd.ack == salobj.SalRetCode.CMD_COMPLETE
                summaryState_data = await remote.evt_summaryState.next(
                    flush=False, timeout=STD_TIMEOUT
                )
                assert summaryState_data.summaryState == salobj.State.OFFLINE

                await asyncio.wait_for(process.wait(), timeout=STD_TIMEOUT)
            except Exception:
                if process.returncode is None:
                    process.terminate()
                raise
