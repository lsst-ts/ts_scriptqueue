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

import os
import subprocess
import time
import unittest

from lsst.ts import salobj
from lsst.ts.scriptqueue.ui import RequestModel


class TestRequestModel(unittest.TestCase):

    def test_request_model(self):
        salobj.set_random_lsst_dds_domain()
        index = 1
        datadir = os.path.abspath(os.path.join(os.path.dirname(__file__), "data"))
        standardpath = os.path.join(datadir, "standard")
        externalpath = os.path.join(datadir, "external")

        print("start subprocess")
        with subprocess.Popen(["run_script_queue.py", str(index),
                               "--standard", standardpath,
                               "--external", externalpath]) as proc:
            try:
                ui_model = RequestModel(index)

                print("enable queue")
                ui_model.enable_queue()

                self.assertEqual(ui_model.summary_state, salobj.State.ENABLED)

                print("get available scripts")
                available_scripts = ui_model.get_scripts()

                print("check available scripts")
                expected_std_set = set(["script1", "script2", "unloadable",
                                        "subdir/script3", "subdir/subsubdir/script4"])
                expected_ext_set = set(["script1", "script5", "subdir/script3", "subdir/script6"])
                self.assertEqual(set(available_scripts["standard"]), expected_std_set)
                self.assertEqual(set(available_scripts["external"]), expected_ext_set)

                print("pause queue")
                ui_model.pause_queue()
                self.assertEqual(ui_model.state(), "Stopped")

                print("resume queue")
                ui_model.resume_queue()
                self.assertEqual(ui_model.state(), "Running")

                print("start a script")
                result = ui_model.add(path="script1", is_standard=True, config="")
                self.assertEqual(result, 100000)

                print("quit queue")
                ui_model.quit_queue()
                t0 = time.time()
                while ui_model.summary_state != salobj.State.OFFLINE:
                    time.sleep(0.1)
                    if time.time() - t0 > 2:
                        self.fail("Timed out waiting for ui_model to go OFFLINE")
            except Exception:
                proc.terminate()
                raise


if __name__ == "__main__":
    unittest.main()
