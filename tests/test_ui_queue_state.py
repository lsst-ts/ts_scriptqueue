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

import time
import unittest

import SALPY_ScriptQueue
from lsst.ts.scriptqueue.ui import QueueState
from lsst.ts.scriptqueue import ScriptState, ScriptProcessState


class TestQueueState(unittest.TestCase):

    def test_queue_state(self):

        queue = SALPY_ScriptQueue.ScriptQueue_logevent_queueC()

        queue.enabled = True
        queue.running = True

        queue.currentSalIndex = 100

        queue.length = 1
        queue.pastLength = 1

        queue.salIndices[0] = 101
        queue.pastSalIndices[0] = 99

        queue_state = QueueState()

        queue_state.update(queue)

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

        queue_state.update_script_info(past_script)
        queue_state.update_script_info(current_script)
        queue_state.update_script_info(next_script)

        self.assertEqual(queue_state.enabled, queue.enabled)
        self.assertEqual(queue_state.running, queue.running)
        self.assertEqual(queue_state.state, "Running")
        self.assertEqual(queue_state._current_script_index, queue.currentSalIndex)
        self.assertEqual(len(queue_state._past_script_indices), queue.pastLength)
        self.assertEqual(len(queue_state._queue_script_indices), queue.length)

        for index in (99, 100, 101):
            with self.subTest(index=index):
                self.assertTrue(index in queue_state.scripts)


if __name__ == "__main__":
    unittest.main()
