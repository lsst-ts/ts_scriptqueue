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
import time
import unittest

from lsst.ts import salobj
from lsst.ts.scriptqueue.ui import RequestModel
from lsst.ts.scriptqueue import ScriptState, ScriptProcessState

import SALPY_ScriptQueue


index_gen = salobj.index_generator()


class Harness:

    def __init__(self):
        self.index = next(index_gen)
        salobj.test_utils.set_random_lsst_dds_domain()

        self.external_script = "unit:unit1:unit2"
        self.standard_script = "test:test1:test2"
        self.enabled = True
        self.running = True

        self.queue = salobj.Controller(SALPY_ScriptQueue, self.index)

        self.queue.cmd_showQueue.callback = self.show_queue_callback
        self.queue.cmd_showScript.callback = self.show_script_callback
        self.queue.cmd_start.callback = self.start_callback
        self.queue.cmd_enable.callback = self.enable_callback
        self.queue.cmd_disable.callback = self.disable_callback
        self.queue.cmd_standby.callback = self.standby_callback
        self.queue.cmd_exitControl.callback = self.exit_control_callback
        self.queue.cmd_showAvailableScripts.callback = self.show_available_scripts_callback
        self.queue.cmd_pause.callback = self.pause_callback
        self.queue.cmd_resume.callback = self.resume_callback
        self.queue.cmd_add.callback = self.add_callback

        self.queue.evt_summaryState.set_put(summaryState=salobj.State.ENABLED)

        self.scripts = {}

        self.setup_script_info()

        self.ui_model = RequestModel(self.index)

        self.add_index = [99, 100, 101]

    def show_queue_callback(self, id_data):
        """Callback for the queue command showQueue."""

        queue = SALPY_ScriptQueue.ScriptQueue_logevent_queueC()

        queue.enabled = self.enabled
        queue.running = self.running

        queue.currentSalIndex = 100

        queue.length = 1
        queue.pastLength = 1

        queue.salIndices[0] = 101
        queue.pastSalIndices[0] = 99

        self.queue.evt_queue.put(queue)

    def show_script_callback(self, id_data):
        """Callback for the queue command showScript."""
        self.queue.evt_script.put(self.scripts[id_data.data.salIndex])

    def show_available_scripts_callback(self, id_data):
        """Callback for the queue showAvailableScripts command."""
        self.queue.evt_availableScripts.set(external=self.external_script,
                                            standard=self.standard_script)
        self.queue.evt_availableScripts.put()

    def start_callback(self, id_data):
        """Callback for the queue start command."""
        self.queue.evt_summaryState.set_put(summaryState=salobj.State.DISABLED)
        self.enabled = False
        self.show_queue_callback(None)

    def enable_callback(self, id_data):
        """Callback for the queue enable command."""
        self.queue.evt_summaryState.set_put(summaryState=salobj.State.ENABLED)
        self.enabled = True
        self.show_queue_callback(None)

    def disable_callback(self, id_data):
        """Callback for the queue disable command."""
        self.queue.evt_summaryState.set_put(summaryState=salobj.State.DISABLED)
        self.enabled = False
        self.show_queue_callback(None)

    def standby_callback(self, id_data):
        """Callback for the queue standby command."""
        self.queue.evt_summaryState.set_put(summaryState=salobj.State.STANDBY)
        self.enabled = False
        self.show_queue_callback(None)

    def exit_control_callback(self, id_data):
        """Callback for the queue exitControl command."""
        self.queue.evt_summaryState.set_put(summaryState=salobj.State.OFFLINE)
        self.enabled = False
        self.show_queue_callback(None)

    def pause_callback(self, id_data):
        """Callback to queue pause command"""
        self.running = False
        self.show_queue_callback(None)

    def resume_callback(self, id_data):
        """Callback to queue resume command"""
        self.running = True
        self.show_queue_callback(None)

    def add_callback(self, id_data):
        """Callback to the queue add command."""

        new_script = SALPY_ScriptQueue.ScriptQueue_logevent_scriptC()
        new_script.path = id_data.data.path
        new_script.timestampProcessStart = time.time()
        new_script.timestampRunStart = 0.
        new_script.timestampProcessEnd = 0.
        new_script.timestampConfigureStart = 0.
        new_script.timestampConfigureEnd = 0.
        new_script.processState = ScriptProcessState.LOADING
        new_script.scriptState = ScriptState.UNCONFIGURED

        if id_data.data.isStandard and id_data.data.path in self.standard_script:
            new_index = max(self.add_index) + 1
            self.add_index.append(new_index)

            new_script.salIndex = new_index
            new_script.isStandard = True

        elif not id_data.data.isStandard and id_data.data.path in self.external_script:

            new_index = max(self.add_index) + 1
            self.add_index.append(new_index)

            new_script.salIndex = new_index
            new_script.isStandard = True
        else:
            raise RuntimeError(f"Cannot add script {id_data.data.path}.")

        self.scripts[new_index] = new_script

        return self.queue.salinfo.makeAck(self.queue.salinfo.lib.SAL__CMD_COMPLETE, result=f"{new_index}")

    def setup_script_info(self):
        """Method to output script info."""

        current_script = SALPY_ScriptQueue.ScriptQueue_logevent_scriptC()
        current_script.salIndex = 100
        current_script.isStandard = True
        current_script.path = 'test'
        current_script.timestampProcessStart = time.time()-20.
        current_script.timestampRunStart = time.time()-5.
        current_script.timestampProcessEnd = 0.
        current_script.timestampConfigureStart = time.time()-15.
        current_script.timestampConfigureEnd = time.time()-14.
        current_script.processState = ScriptProcessState.RUNNING
        current_script.scriptState = ScriptState.RUNNING

        past_script = SALPY_ScriptQueue.ScriptQueue_logevent_scriptC()
        past_script.salIndex = 99
        past_script.isStandard = True
        past_script.path = 'test'
        past_script.timestampProcessStart = time.time()-40.
        past_script.timestampRunStart = time.time()-25.
        past_script.timestampProcessEnd = time.time()-6.
        past_script.timestampConfigureStart = time.time()-35.
        past_script.timestampConfigureEnd = time.time()-34.
        past_script.processState = ScriptProcessState.DONE
        past_script.scriptState = ScriptState.DONE

        next_script = SALPY_ScriptQueue.ScriptQueue_logevent_scriptC()
        next_script.salIndex = 101
        next_script.isStandard = True
        next_script.path = 'test'
        next_script.timestampProcessStart = time.time()-19.
        next_script.timestampRunStart = 0.
        next_script.timestampProcessEnd = 0.
        next_script.timestampConfigureStart = time.time()-13.
        next_script.timestampConfigureEnd = time.time()-12.
        next_script.processState = ScriptProcessState.CONFIGURED
        next_script.scriptState = ScriptState.CONFIGURED

        self.scripts = {99: past_script,
                        100: current_script,
                        101: next_script}

    async def output_heartbeats(self):
        """Emulate heartbeat from the queue."""
        while True:
            self.queue.evt_heartbeat.put()
            await asyncio.sleep(1.)


class TestRequestModel(unittest.TestCase):

    def test_request_model(self):

        harness = Harness()

        harness.queue.evt_summaryState.set_put(summaryState=salobj.State.STANDBY)

        self.assertTrue(harness.ui_model.summary_state,
                        salobj.State.STANDBY)

        harness.ui_model.enable_queue()

        self.assertTrue(harness.ui_model.summary_state,
                        salobj.State.ENABLED)

        available_scripts = harness.ui_model.get_scripts()

        for external in available_scripts['external']:
            self.assertIn(external, harness.external_script)
        for standard in available_scripts['standard']:
            self.assertIn(standard, harness.standard_script)

        harness.ui_model.pause_queue()

        self.assertEqual(harness.ui_model.state(), "Stopped")

        harness.ui_model.resume_queue()

        self.assertEqual(harness.ui_model.state(), "Running")

        result = harness.ui_model.add(path="test", is_standard=True, config="")

        self.assertIn(result, harness.add_index)

        harness.ui_model.quit_queue()

        self.assertTrue(harness.ui_model.summary_state,
                        salobj.State.OFFLINE)


if __name__ == "__main__":
    unittest.main()
