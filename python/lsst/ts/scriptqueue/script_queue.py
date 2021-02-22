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

__all__ = ["ScriptQueue"]

import asyncio
import os
import subprocess

import numpy as np

from lsst.ts import salobj
from . import __version__
from . import utils
from .script_info import ScriptInfo
from .queue_model import QueueModel

SCRIPT_INDEX_MULT = 100000
"""Minimum Script SAL index is ScriptQueue SAL index * SCRIPT_INDEX_MULT
and the maximum is SCRIPT_INDEX_MULT-1 more.
"""
_MAX_SCRIPTQUEUE_INDEX = salobj.MAX_SAL_INDEX // SCRIPT_INDEX_MULT - 1


class ScriptQueue(salobj.BaseCsc):
    """CSC to load and configure scripts, so they can be run.

    Parameters
    ----------
    index : `int`
        ScriptQueue SAL component index:

        * 1 for the Main telescope.
        * 2 for AuxTel.
        * Any allowed value (see ``Raises``) for unit tests.
    initial_state : `lsst.ts.salobj.State` or `int`, optional
        The initial state of the CSC.
    standardpath : `str`, `bytes` or `os.PathLike` (optional)
        Path to standard SAL scripts. If None then use
        ``lsst.ts.standardscripts.get_scripts_dir()``.
    externalpath : `str`, `bytes` or `os.PathLike` (optional)
        Path to external SAL scripts. If None then use
        ``lsst.ts.externalscripts.get_scripts_dir()``.
    verbose : `bool`
        If True then print diagnostic messages to stdout.

    Raises
    ------
    ValueError
        If ``index`` < 0 or > MAX_SAL_INDEX//100,000 - 1.
        If ``standardpath`` or ``externalpath`` is not an existing directory.
    """

    valid_simulation_modes = [0]
    version = __version__
    enable_cmdline_state = True

    def __init__(
        self,
        index,
        initial_state=salobj.State.STANDBY,
        standardpath=None,
        externalpath=None,
        verbose=False,
    ):
        if index < 0 or index > _MAX_SCRIPTQUEUE_INDEX:
            raise ValueError(
                f"index {index} must be >= 0 and <= {_MAX_SCRIPTQUEUE_INDEX}"
            )
        standardpath = self._get_scripts_path(standardpath, is_standard=True)
        externalpath = self._get_scripts_path(externalpath, is_standard=False)
        self.verbose = verbose

        min_sal_index = index * SCRIPT_INDEX_MULT
        max_sal_index = min_sal_index + SCRIPT_INDEX_MULT - 1
        if max_sal_index > salobj.MAX_SAL_INDEX:
            raise ValueError(f"index {index} too large and a bug let this slip through")

        super().__init__(name="ScriptQueue", index=index, initial_state=initial_state)

        self.model = QueueModel(
            domain=self.domain,
            log=self.log,
            standardpath=standardpath,
            externalpath=externalpath,
            next_visit_callback=self.put_next_visit,
            next_visit_canceled_callback=self.put_next_visit_canceled,
            queue_callback=self.put_queue,
            script_callback=self.put_script,
            min_sal_index=min_sal_index,
            max_sal_index=max_sal_index,
            verbose=verbose,
        )

    def _get_scripts_path(self, patharg, is_standard):
        """Get the scripts path from the ``standardpath`` or ``externalpath``
        constructor argument.

        Parameters
        ----------
        patharg : `str` or `None`
            ``standardpath`` or ``externalpath`` constructor argument.
        If None then use ``lsst.ts.standardscripts.get_scripts_dir()`` if
            ``is_standard``, else ``lsst.ts.externalscripts.get_scripts_dir()``
        is_standard : `bool`
            True if ``patharg`` is the ``standardpath`` constructor argument,
            False if ``patharg`` is the ``externalpath`` constructor argument.

        Returns
        -------
        dir_path : `pathlib.Path`
            Path to the standard or external scripts directory.

        Raises
        ------
        ValueError
            If ``patharg`` is not `None` and does not point to a directory.
        """
        if patharg is None:
            dir_path = utils.get_default_scripts_dir(is_standard)
        else:
            dir_path = patharg
        if not os.path.isdir(dir_path):
            category = "standard" if is_standard else "external"
            raise ValueError(f"{category} scripts path {dir_path} is not a directory")
        return dir_path

    async def start(self):
        """Finish creating the script queue."""
        await self.model.start_task
        self.evt_rootDirectories.set_put(
            standard=self.model.standardpath,
            external=self.model.externalpath,
            force_output=True,
        )
        self.put_queue()
        await super().start()

    async def close_tasks(self):
        """Shut down the queue, terminate all scripts and free resources."""
        await self.model.close()
        await super().close_tasks()

    def do_showAvailableScripts(self, data=None):
        """Output a list of available scripts.

        Parameters
        ----------
        data : ``cmd_showAvailableScripts.DataType``
            Command data. Ignored.
        """
        self.assert_enabled("showAvailableScripts")
        scripts = self.model.find_available_scripts()
        self.evt_availableScripts.set_put(
            standard=":".join(scripts.standard),
            external=":".join(scripts.external),
            force_output=True,
        )

    async def do_showSchema(self, data):
        """Output the config schema for a script.

        Parameters
        ----------
        data : ``cmd_showSchema.DataType``
            Command data specifying the script.
        """
        self.assert_enabled("showSchema")
        fullpath = self.model.make_full_path(data.isStandard, data.path)
        initialpath = os.environ["PATH"]
        scriptdir, scriptname = os.path.split(fullpath)
        os.environ["PATH"] = scriptdir + ":" + initialpath
        process = await asyncio.create_subprocess_exec(
            scriptname, "0", "--schema", stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=20)
            self.evt_configSchema.set_put(
                isStandard=data.isStandard,
                path=data.path,
                configSchema=stdout,
                force_output=True,
            )
        except Exception:
            if process.returncode is None:
                process.terminate()
                self.log.warning(
                    "showSchema killed a process that was not properly terminated"
                )
            raise
        finally:
            os.environ["PATH"] = initialpath

    def do_showQueue(self, data):
        """Output the queue event.

        Parameters
        ----------
        data : ``cmd_showQueue.DataType`` (optional)
            Command data. Ignored.
        """
        self.assert_enabled("showQueue")
        self.put_queue()

    def do_showScript(self, data):
        """Output the script event for one script.

        Parameters
        ----------
        data : ``cmd_showScript.DataType`` (optional)
            Command data. Ignored.
        """
        self.assert_enabled("showScript")
        try:
            script_info = self.model.get_script_info(data.salIndex, search_history=True)
        except ValueError:
            raise salobj.ExpectedError(f"Unknown script {data.salIndex}")
        self.put_script(script_info, force_output=True)

    def do_pause(self, data):
        """Pause the queue. A no-op if already paused.

        Unlike most commands, this can be issued in any state.

        Parameters
        ----------
        data : ``cmd_pause.DataType`` (optional)
            Command data. Ignored.
        """
        self.model.running = False

    def do_resume(self, data):
        """Run the queue. A no-op if already running.

        Parameters
        ----------
        data : ``cmd_resume.DataType`` (optional)
            Command data. Ignored.
        """
        self.assert_enabled("resume")
        self.model.running = True

    async def do_add(self, data):
        """Add a script to the queue.

        Start and configure a script SAL component, but don't run it.

        On success the ``result`` field of the final acknowledgement
        contains ``str(index)`` where ``index`` is the SAL index
        of the added Script.
        """
        self.assert_enabled("add")
        script_info = ScriptInfo(
            log=self.log,
            remote=self.model.remote,
            index=self.model.next_sal_index,
            seq_num=data.private_seqNum,
            is_standard=data.isStandard,
            path=data.path,
            config=data.config,
            log_level=data.logLevel,
            pause_checkpoint=data.pauseCheckpoint,
            stop_checkpoint=data.stopCheckpoint,
            descr=data.descr,
            verbose=self.verbose,
        )
        await self.model.add(
            script_info=script_info,
            location=data.location,
            location_sal_index=data.locationSalIndex,
        )

        return self.salinfo.make_ackcmd(
            private_seqNum=data.private_seqNum,
            ack=salobj.SalRetCode.CMD_COMPLETE,
            result=str(script_info.index),
        )

    def do_move(self, data):
        """Move a script within the queue.
        """
        self.assert_enabled("move")
        try:
            self.model.move(
                sal_index=data.salIndex,
                location=data.location,
                location_sal_index=data.locationSalIndex,
            )
        except ValueError as e:
            raise salobj.ExpectedError(str(e))

    async def do_requeue(self, data):
        """Put a script back on the queue with the same configuration.
        """
        self.assert_enabled("requeue")
        try:
            await self.model.requeue(
                sal_index=data.salIndex,
                seq_num=data.private_seqNum,
                location=data.location,
                location_sal_index=data.locationSalIndex,
            )
        except ValueError as e:
            raise salobj.ExpectedError(str(e))

    async def do_stopScripts(self, data):
        """Stop one or more queued scripts and/or the current script.

        If you stop the current script, it is moved to the history.
        If you stop queued scripts they are not not moved to the history.
        """
        self.assert_enabled("stopScripts")
        if data.length <= 0:
            raise salobj.ExpectedError(f"length={data.length} must be positive")
        timeout = 5 + 0.2 * data.length
        await asyncio.wait_for(
            self.model.stop_scripts(
                sal_indices=data.salIndices[0 : data.length], terminate=data.terminate
            ),
            timeout,
        )

    def report_summary_state(self):
        super().report_summary_state()
        enabled = self.summary_state == salobj.State.ENABLED
        self.model.enabled = enabled
        if enabled:
            self.do_showAvailableScripts()

    def put_next_visit(self, script_info):
        """Output the ``nextVisit`` event.
        """
        if self.verbose:
            print(
                f"put_next_visit: index={script_info.index}, "
                f"group_id={script_info.group_id}"
            )
        if script_info.metadata is None:
            raise RuntimeError("script_info has no metadata")
        if not script_info.group_id:
            raise RuntimeError("script_info has no group_id")
        metadata_dict = {
            key: value
            for key, value in script_info.metadata.get_vars().items()
            if not key.startswith("private_")
        }
        del metadata_dict["ScriptID"]
        self.evt_nextVisit.set_put(
            salIndex=script_info.index,
            groupId=script_info.group_id,
            **metadata_dict,
            force_output=True,
        )

    def put_next_visit_canceled(self, script_info):
        """Output the ``nextVisitCanceled`` event.
        """
        if self.verbose:
            print(
                f"put_next_visit_canceled: index={script_info.index}, "
                f"group_id={script_info.group_id}"
            )
        if not script_info.group_id:
            raise RuntimeError("script_info has no group_id")
        self.evt_nextVisitCanceled.set_put(
            salIndex=script_info.index, groupId=script_info.group_id, force_output=True
        )

    def put_queue(self):
        """Output the queued scripts as a ``queue`` event.

        The data is put even if the queue has not changed. That way commands
        which alter the queue can rely on the event being published,
        even if the command has no effect (e.g. moving a script before itself).
        """
        raw_sal_indices = self.model.queue_indices
        output_sal_indices = np.zeros_like(self.evt_queue.data.salIndices)
        indlen = min(len(raw_sal_indices), len(output_sal_indices))
        output_sal_indices[0:indlen] = raw_sal_indices[0:indlen]

        output_past_sal_indices = np.zeros_like(self.evt_queue.data.pastSalIndices)
        raw_past_sal_indices = self.model.history_indices
        pastlen = min(len(raw_past_sal_indices), len(output_past_sal_indices))
        output_past_sal_indices[0:pastlen] = raw_past_sal_indices[0:pastlen]

        if self.verbose:
            print(
                f"put_queue: enabled={self.model.enabled}, running={self.model.running}, "
                f"currentSalIndex={self.model.current_index}, "
                f"salIndices={output_sal_indices[0:indlen]}, "
                f"pastSalIndices={output_past_sal_indices[0:pastlen]}"
            )
        self.evt_queue.set_put(
            enabled=self.model.enabled,
            running=self.model.running,
            currentSalIndex=self.model.current_index,
            length=indlen,
            salIndices=output_sal_indices,
            pastLength=pastlen,
            pastSalIndices=output_past_sal_indices,
            force_output=True,
        )

    def put_script(self, script_info, force_output=False):
        """Output information about a script as a ``script`` event.

        Designed to be used as a QueueModel script_callback.

        Parameters
        ----------
        script_info : `ScriptInfo`
            Information about the script.
        force_output : `bool` (optional)
            If True the output even if not changed.
        """
        if script_info is None:
            return

        if self.verbose:
            print(
                f"put_script: index={script_info.index}, "
                f"process_state={script_info.process_state}, "
                f"script_state={script_info.script_state}"
            )
        self.evt_script.set_put(
            cmdId=script_info.seq_num,
            salIndex=script_info.index,
            path=script_info.path,
            isStandard=script_info.is_standard,
            timestampProcessStart=script_info.timestamp_process_start,
            timestampConfigureStart=script_info.timestamp_configure_start,
            timestampConfigureEnd=script_info.timestamp_configure_end,
            timestampRunStart=script_info.timestamp_run_start,
            timestampProcessEnd=script_info.timestamp_process_end,
            processState=script_info.process_state,
            scriptState=script_info.script_state,
            force_output=force_output,
        )

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument(
            "--standard",
            help="Directory containing standard scripts; "
            "defaults to ts_standardscripts/scripts",
        )
        parser.add_argument(
            "--external",
            help="Directory containing external scripts; "
            "defaults to ts_externalscripts/scripts",
        )
        parser.add_argument(
            "--verbose",
            action="store_true",
            help="Print diagnostic information to stdout",
        )

    @classmethod
    def add_kwargs_from_args(cls, args, kwargs):
        kwargs["standardpath"] = args.standard
        kwargs["externalpath"] = args.external
        kwargs["verbose"] = args.verbose
