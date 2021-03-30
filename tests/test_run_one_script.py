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
import pathlib
import shutil
import time
import unittest

import yaml

from lsst.ts.idl.enums.Script import ScriptState
from lsst.ts import salobj
from lsst.ts.scriptqueue import ui

# Long enough to perform any reasonable operation
# including starting a CSC or loading a script (seconds)
STD_TIMEOUT = 60

DATA_DIR = pathlib.Path(__file__).resolve().parent / "data"


class ParseRunOneScriptTestCase(unittest.IsolatedAsyncioTestCase):
    def test_basics(self):
        script = DATA_DIR / "standard" / "subdir" / "script3"
        cmd = ui.parse_run_one_script_cmd(args=[str(script)])
        self.assertTrue(script.samefile(cmd.script))
        self.assertEqual(cmd.config, "")

    def test_config_arg(self):
        script = DATA_DIR / "external" / "script1"
        config_path = DATA_DIR / "config1.yaml"
        with open(config_path, "r") as f:
            expected_config = f.read()
        cmd = ui.parse_run_one_script_cmd(
            args=[str(script), "--config", config_path.as_posix()]
        )
        self.assertTrue(script.samefile(cmd.script))
        self.assertEqual(cmd.config, expected_config)

    def test_parameters_arg(self):
        script = DATA_DIR / "external" / "script1"
        config_dict = dict(abool=True, anint=47, afloat=0.2, astr="string_value")
        config_arg_list = [f"{key}={value}" for key, value in config_dict.items()]
        cmd = ui.parse_run_one_script_cmd(
            args=[str(script), "--parameters"] + config_arg_list
        )
        self.assertTrue(script.samefile(cmd.script))
        config_dict_from_parser = yaml.safe_load(cmd.config)
        self.assertEqual(config_dict_from_parser, config_dict)

    def test_loglevel(self):
        script = DATA_DIR / "external" / "script1"
        cmd = ui.parse_run_one_script_cmd(args=[str(script)])
        self.assertIsNone(cmd.loglevel)

        loglevel = 15
        cmd = ui.parse_run_one_script_cmd(
            args=[str(script), "--loglevel", str(loglevel)]
        )
        self.assertTrue(cmd.loglevel, loglevel)

        loglevel = 21
        cmd = ui.parse_run_one_script_cmd(args=[str(script), "-l", str(loglevel)])
        self.assertTrue(cmd.loglevel, loglevel)

    def test_invalid_arguments(self):
        script = DATA_DIR / "external" / "script1"
        config_path = DATA_DIR / "config1.yaml"

        with self.assertRaises(SystemExit):
            # need type and path
            ui.parse_run_one_script_cmd(args=[])
        with self.assertRaises(SystemExit):
            # there is only one allowed positional arguments
            ui.parse_run_one_script_cmd(args=[str(script), "extra_argument"])
        with self.assertRaises(SystemExit):
            # invalid option
            ui.parse_run_one_script_cmd(args=[str(script), "--invalid"])
        with self.assertRaises(SystemExit):
            # nonexistent config file
            ui.parse_run_one_script_cmd(
                args=[str(script), "--config", "nonexistent_config_file.yaml"]
            )
        with self.assertRaises(SystemExit):
            # --index must have a value if specified
            ui.parse_run_one_script_cmd(args=[str(script), "--index"])
        with self.assertRaises(SystemExit):
            # --index must be > 0
            ui.parse_run_one_script_cmd(args=[str(script), "--index", "0"])
        with self.assertRaises(SystemExit):
            # --index must be <= salobj.MAX_SAL_INDEX
            too_large_index = salobj.MAX_SAL_INDEX + 1
            ui.parse_run_one_script_cmd(
                args=[str(script), "--index", str(too_large_index)]
            )
        with self.assertRaises(SystemExit):
            # --index must be an integer
            ui.parse_run_one_script_cmd(args=[str(script), "--index", "not_an_integer"])
        with self.assertRaises(SystemExit):
            # --parameters requires data
            ui.parse_run_one_script_cmd(args=[str(script), "--parameters"])
        with self.assertRaises(SystemExit):
            # invalid data for --parameters; no =
            ui.parse_run_one_script_cmd(
                args=[str(script), "--parameters", "invalid_parameter"]
            )
        with self.assertRaises(SystemExit):
            # invalid data for --parameters; space
            ui.parse_run_one_script_cmd(
                args=[str(script), "--parameters", "wait_time", "=", "0.1"]
            )
        with self.assertRaises(SystemExit):
            # cannot specify both --config and --parameters
            ui.parse_run_one_script_cmd(
                args=[
                    str(script),
                    "--config",
                    config_path.as_posix(),
                    "--parameters",
                    "wait_time=0.1",
                ]
            )


class RunOneScriptTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        salobj.set_random_lsst_dds_partition_prefix()

    async def test_run_one_script(self):
        script = DATA_DIR / "standard" / "subdir" / "script3"
        config_path = DATA_DIR / "config1.yaml"
        with open(config_path, "r") as f:
            config = f.read()
        await ui.run_one_script(index=1, script=script, config=config, loglevel=10)

    async def test_run_command_line(self):
        exe_name = "run_one_script.py"
        exe_path = shutil.which(exe_name)
        if exe_path is None:
            self.fail(
                f"Could not find bin script {exe_name}; did you setup and scons this package?"
            )

        index = 135
        script = DATA_DIR / "standard" / "subdir" / "script3"
        config_path = DATA_DIR / "config1.yaml"
        async with salobj.Domain() as domain, salobj.Remote(
            domain=domain, name="Script", index=index
        ) as remote:
            process = await asyncio.create_subprocess_exec(
                exe_name,
                str(script),
                "--config",
                str(config_path),
                "--index",
                str(index),
                "--loglevel",
                "10",
            )
            try:
                t0 = time.time()
                await asyncio.wait_for(process.wait(), timeout=STD_TIMEOUT)
                dt = time.time() - t0
                print(f"It took {dt:0.2f} seconds to run the script")
            except Exception:
                if process.returncode is None:
                    process.terminate()
                raise
            final_state = remote.evt_state.get()
            self.assertEqual(final_state.state, ScriptState.DONE)


if __name__ == "__main__":
    unittest.main()
