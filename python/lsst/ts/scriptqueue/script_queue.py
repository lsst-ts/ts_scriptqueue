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
from . import utils
from .queue_model import QueueModel, ScriptInfo

SCRIPT_INDEX_MULT = 100000
"""Minimum Script SAL index is ScriptQueue SAL index * SCRIPT_INDEX_MULT
and the maximum is SCRIPT_INDEX_MULT-1 more.
"""
_MAX_SCRIPTQUEUE_INDEX = salobj.MAX_SAL_INDEX//SCRIPT_INDEX_MULT - 1


class ScriptQueue(salobj.BaseCsc):
    """CSC to load and configure scripts, so they can be run.

    Parameters
    ----------
    index : `int`
        ScriptQueue SAL component index:

        * 1 for the Main telescope.
        * 2 for AuxTel.
        * Any allowed value (see ``Raises``) for unit tests.
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

    Notes
    -----
    .. _script_queue_basic_usage:

    Basic usage:

    * Send the ``add`` command to the ``ScriptQueue`` to add a script
      to the queue. The added script is loaded in a subprocess
      as a new ``Script`` SAL component with a unique SAL index:

      * The script's SAL index is used to uniquely identify the script
        for commands such as ``move`` and ``stopScript``.
      * The index is returned as the ``result`` field of
        the final acknowledgement of the ``add`` command.
      * The first script loaded has index ``min_sal_index``,
        the next has index ``min_sal_index+1``,
        then  ``min_sal_index+2``, ... ``max_sal_index``,
        then wrap around to start over at ``min_sal_index``.
      * The minimum SAL script index is 100,000 * the SAL index
        of `ScriptQueue`: 100,000 for the main telescope
        and 200,000 for the auxiliary telescope.
        The maximum is, naturally, 99,999 more than that.

    * Once a script is added, it reports its state as
      `ScriptState.UNCONFIGURED`. At that point the `ScriptQueue`
      configures it, using the configuration specified in the
      ``add`` command.
    * Configuring a script causes it to output the ``metadata`` event,
      which includes an estimated duration, and changes the script's
      state to `ScriptState.CONFIGURED`. This means it can now be run.
    * When the current script is finished, its information is moved
      to a history list, in order to support requeueing old scripts. Then:

      * If the queue is running, then when the first script in the queue
        has been configured, it is moved to the ``current`` slot and run.
      * If the queue is paused, then the current slot is left empty
        and no new script is run.
    * Once a script has finished running, its information is moved to
      a history list, which is output as part of the ``queue`` event.
      The history list allows ``requeue`` to work with scripts that
      have already run.

    Events:

    * As each script is added or changes state `ScriptQueue` outputs
      a ``script_info`` event which includes the script's SAL index,
      path and state.
    * As the script queue changes state `ScriptQueue` outputs the
      ``queue`` event listing the SAL indices of scripts on the queue,
      the currently running script, and scripts that have been run
      (the history list).
    * When each script is configured, the script (not `ScriptQueue`)
      outputs a ``metadata`` event that includes estimated duration.
    """
    def __init__(self, index, standardpath=None, externalpath=None, verbose=False):
        if index < 0 or index > _MAX_SCRIPTQUEUE_INDEX:
            raise ValueError(f"index {index} must be >= 0 and <= {_MAX_SCRIPTQUEUE_INDEX}")
        standardpath = self._get_scripts_path(standardpath, is_standard=True)
        externalpath = self._get_scripts_path(externalpath, is_standard=False)
        self.verbose = verbose

        min_sal_index = index * SCRIPT_INDEX_MULT
        max_sal_index = min_sal_index + SCRIPT_INDEX_MULT - 1
        if max_sal_index > salobj.MAX_SAL_INDEX:
            raise ValueError(f"index {index} too large and a bug let this slip through")

        super().__init__("ScriptQueue", index)

        self.model = QueueModel(domain=self.domain,
                                log=self.log,
                                standardpath=standardpath,
                                externalpath=externalpath,
                                queue_callback=self.put_queue,
                                script_callback=self.put_script,
                                min_sal_index=min_sal_index,
                                max_sal_index=max_sal_index,
                                verbose=verbose)

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
        await super().start()
        await self.model.start_task
        self.evt_rootDirectories.set_put(standard=self.model.standardpath,
                                         external=self.model.externalpath,
                                         force_output=True)
        self.put_queue()

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
        fullpath = self.make_full_path(data.is_standard, data.path)
        initialpath = os.environ["PATH"]
        scriptdir, scriptname = os.path.split(fullpath)
        os.environ["PATH"] = scriptdir + ":" + initialpath
        # save task so process creation can be cancelled if it hangs
        process = await asyncio.create_subprocess_exec(scriptname, "0", "--schema",
                                                       stdout=subprocess.PIPE,
                                                       stderr=subprocess.PIPE)
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=20)
            self.evt_configSchema.set_put(is_standard=data.is_standard,
                                          path=data.path,
                                          configSchema=stdout,
                                          force_output=True)

            self.process = await self.create_process_task
            await asyncio.wait_for(self.process.wait(), timeout=20)
        except Exception:
            if process.returncode is None:
                process.terminate()
                self.log.warn("showSchema killed a process that was not properly terminated")
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
        script_info = self.model.get_script_info(data.salIndex,
                                                 search_history=True)
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
            descr=data.descr,
            verbose=self.verbose,
        )
        await self.model.add(
            script_info=script_info,
            location=data.location,
            location_sal_index=data.locationSalIndex,
        )
        return self.salinfo.makeAckCmd(private_seqNum=data.private_seqNum,
                                       ack=salobj.SalRetCode.CMD_COMPLETE, result=str(script_info.index))

    def do_move(self, data):
        """Move a script within the queue.
        """
        self.assert_enabled("move")
        self.model.move(sal_index=data.salIndex,
                        location=data.location,
                        location_sal_index=data.locationSalIndex)

    async def do_requeue(self, data):
        """Put a script back on the queue with the same configuration.
        """
        self.assert_enabled("requeue")
        await self.model.requeue(
            sal_index=data.salIndex,
            seq_num=data.private_seqNum,
            location=data.location,
            location_sal_index=data.locationSalIndex,
        )

    async def do_stopScripts(self, data):
        """Stop one or more queued scripts and/or the current script.

        If you stop the current script, it is moved to the history.
        If you stop queued scripts they are not not moved to the history.
        """
        self.assert_enabled("stopScripts")
        if data.length <= 0:
            raise salobj.ExpectedError(f"length={data.length} must be positive")
        timeout = 5 + 0.2*data.length
        await asyncio.wait_for(self.model.stop_scripts(sal_indices=data.salIndices[0:data.length],
                                                       terminate=data.terminate), timeout)

    def report_summary_state(self):
        super().report_summary_state()
        enabled = self.summary_state == salobj.State.ENABLED
        self.model.enabled = enabled
        if enabled:
            self.do_showAvailableScripts()

    def put_queue(self):
        """Output the queued scripts as a ``queue`` event.

        The data is put even if the queue has not changed. That way commands
        which alter the queue can rely on the event being published,
        even if the command has no effect (e.g. moving a script before itself).
        """
        sal_indices = np.zeros_like(self.evt_queue.data.salIndices)
        indlen = min(len(self.model.queue), len(sal_indices))
        sal_indices[0:indlen] = [info.index for info in self.model.queue][0:indlen]

        past_sal_indices = np.zeros_like(self.evt_queue.data.pastSalIndices)
        pastlen = min(len(self.model.history), len(past_sal_indices))
        past_sal_indices[0:pastlen] = [info.index for info in self.model.history][0:pastlen]

        if self.verbose:
            print(f"put_queue: enabled={self.model.enabled}, running={self.model.running}, "
                  f"currentSalIndex={self.model.current_index}, "
                  f"salIndices={sal_indices[0:indlen]}, "
                  f"pastSalIndices={past_sal_indices[0:pastlen]}")
        self.evt_queue.set_put(
            enabled=self.model.enabled,
            running=self.model.running,
            currentSalIndex=self.model.current_index,
            length=indlen,
            salIndices=sal_indices,
            pastLength=pastlen,
            pastSalIndices=past_sal_indices,
            force_output=True)

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
            print(f"put_script: index={script_info.index}, "
                  f"process_state={script_info.process_state}, "
                  f"script_state={script_info.script_state}")
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
