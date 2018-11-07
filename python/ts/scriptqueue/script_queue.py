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
import os.path

import SALPY_ScriptQueue
import salobj
from .queue_model import QueueModel, ScriptInfo

SCRIPT_INDEX_MULT = 10000
"""Minimum Script SAL index is ScriptQueue SAL index * SCRIPT_INDEX_MULT
and the maximum is SCRIPT_INDEX_MULT-1 more.

TODO once a ts_sal release with TSS-3305 fixed is in widespread use,
increase this to 100,000.
"""
_MAX_SCRIPTQUEUE_INDEX = salobj.MAX_SAL_INDEX//SCRIPT_INDEX_MULT - 1
_LOAD_TIMEOUT = 20  # seconds


class ScriptQueue(salobj.BaseCsc):
    """CSC to load and configure scripts, so they can be run.

    Parameters
    ----------
    index : `int`
        ScriptQueue SAL component index:

        * 1 for the Main telescope.
        * 2 for AuxTel.
        * Any allowed value (see ``Raises``) for unit tests.
    standardpath : `str`, `bytes` or `os.PathLike`
        Path to standard SAL scripts.
    externalpath : `str`, `bytes` or `os.PathLike`
        Path to external SAL scripts.

    Raises
    ------
    ValueError
        If ``index`` < 0 or > MAX_SAL_INDEX//100,000 - 1.
        If ``standardpath`` or ``externalpath`` is not an existing dir.

    Notes
    -----
    Basic usage:

    * Send the ``add`` command to the ``ScriptQueue`` to add a script
      to the queue.
    * The added script will be loaded in a subprocess as a new ``Script``
      SAL component with a unique index. The first script loaded
      has index ``min_sal_index``, and each script after has the next
      index until it wraps around after ``max_sal_index``.
    * `ScriptQueue` will output a ``script_info`` event which includes
      the the index of the ``Script`` SAL component and the command ID
      of the add command. If you wish to reliably know which script your
      ``add`` command added then pay attention to the command ID.
    * Once the script is ready (reports a state of UNCONFIGURED),
      the queue will configure it, using the configuration specified in
      the ``add`` command. Configuring the script causes it to output
      metadata and puts the script into the `ScriptState.CONFIGURED` state,
      which means it can be run.
    * Provided the queue is running (not paused) the first script
      in the queue will be moved to the current script slot
      (as reported in the ``queue`` event). The current script be run
      as soon as it is configured.
    * Once a script has finished running its information is moved to
      a history buffer; history is output as part of the ``queue`` event.
      The history buffer allows ``requeue`` to work with scripts that
      have already run. Eventually support for history can probably
      be done better using the Engineering Facilities Database,
      in which case the history buffer will be eliminated.

    The minimum SAL Script index will be 100,000 * ``index``
    and the maximum will 99,999 more than that.
    """
    def __init__(self, index, standardpath, externalpath):
        if index < 0 or index > _MAX_SCRIPTQUEUE_INDEX:
            raise ValueError(f"index {index} must be >= 0 and <= {_MAX_SCRIPTQUEUE_INDEX}")
        if not os.path.isdir(standardpath):
            raise ValueError(f"No such dir standardpath={standardpath}")
        if not os.path.isdir(externalpath):
            raise ValueError(f"No such dir externalpath={externalpath}")

        super().__init__(SALPY_ScriptQueue, index)
        self.cmd_stop.allow_multiple_callbacks = True
        self.cmd_terminate.allow_multiple_callbacks = True
        min_sal_index = index * SCRIPT_INDEX_MULT
        max_sal_index = min_sal_index + SCRIPT_INDEX_MULT - 1
        if max_sal_index > salobj.MAX_SAL_INDEX:
            raise ValueError(f"index {index} too large and a bug let this slip through")
        self.model = QueueModel(standardpath=standardpath,
                                externalpath=externalpath,
                                queue_callback=self.put_queue,
                                script_callback=self.put_script,
                                min_sal_index=min_sal_index,
                                max_sal_index=max_sal_index)
        self.put_queue()

    def do_showAvailableScripts(self, id_data=None):
        """Output a list of available scripts.

        Parameters
        ----------
        id_data : `salobj.CommandIdData` (optional)
            Command ID and data. Ignored.
        """
        self.assert_enabled("showAvailableScripts")
        scripts = self.model.find_available_scripts()
        evtdata = self.evt_availableScripts.DataType()
        evtdata.standard = ":".join(scripts.standard)
        evtdata.external = ":".join(scripts.external)
        self.evt_availableScripts.put(evtdata, 1)

    def do_showQueue(self, id_data):
        """Output the queue event.

        Parameters
        ----------
        id_data : `salobj.CommandIdData` (optional)
            Command ID and data. Ignored.
        """
        self.assert_enabled("showQueue")
        self.put_queue()

    def do_showScript(self, id_data):
        """Output the script event for one script.

        Parameters
        ----------
        id_data : `salobj.CommandIdData` (optional)
            Command ID and data. Ignored.
        """
        self.assert_enabled("showScript")
        script_info = self.model.get_script_info(id_data.data.salIndex,
                                                 search_history=True)
        self.put_script(script_info)

    def do_pause(self, id_data):
        """Pause the queue. A no-op if already paused.

        Unlike most commands, this can be issued in any state.

        Parameters
        ----------
        id_data : `salobj.CommandIdData` (optional)
            Command ID and data. Ignored.
        """
        self.model.running = False

    def do_resume(self, id_data):
        """Run the queue. A no-op if already running.

        Parameters
        ----------
        id_data : `salobj.CommandIdData` (optional)
            Command ID and data. Ignored.
        """
        self.assert_enabled("resume")
        self.model.running = True

    async def do_add(self, id_data):
        """Add a script to the queue.

        Start and configure a script SAL component, but don't run it.
        """
        self.assert_enabled("add")
        script_info = ScriptInfo(
            index=self.model.next_sal_index,
            cmd_id=id_data.cmd_id,
            is_standard=id_data.data.isStandard,
            path=id_data.data.path,
            config=id_data.data.config,
            descr=id_data.data.descr,
        )
        await self.model.add(
            script_info=script_info,
            location=id_data.data.location,
            location_sal_index=id_data.data.locationSalIndex,
        )

    def do_move(self, id_data):
        """Move a script within the queue.
        """
        self.assert_enabled("move")
        self.model.move(sal_index=id_data.data.salIndex,
                        location=id_data.data.location,
                        location_sal_index=id_data.data.locationSalIndex)

    async def do_requeue(self, id_data):
        """Put a script back on the queue with the same configuration.
        """
        self.assert_enabled("requeue")
        await self.model.requeue(
            sal_index=id_data.data.salIndex,
            cmd_id=id_data.cmd_id,
            location=id_data.data.location,
            location_sal_index=id_data.data.locationSalIndex,
        )

    async def do_stop(self, id_data):
        """Stop a queued or running script, giving it a chance to clean up.

        First try sending the script the ``stop`` command.
        Then, if necessary, terminate the script by sending
        SIGTERM to the subprocess.

        If the script was running, it is moved to the history.
        If the script was not yet running, it is removed
        from the queue.
        """
        self.assert_enabled("remove")
        await self.model.stop(id_data.data.salIndex, timeout=20)

    async def do_terminate(self, id_data):
        """Stop a queued or running script without giving it
        a chance to clean up.

        Terminate a script by sending SIGTERM to the subprocess.

        If the script was running, it is moved to the history.
        If the script was not yet running, it is removed
        from the queue.
        """
        self.assert_enabled("remove")
        await asyncio.wait_for(self.model.terminate(id_data.data.salIndex), 5)

    def report_summary_state(self):
        super().report_summary_state()
        enabled = self.summary_state == salobj.State.ENABLED
        self.model.enabled = enabled
        if enabled:
            self.do_showAvailableScripts()

    def put_queue(self):
        """Output the queued scripts as a ``queue`` event.
        """
        evtdata = self.evt_queue.DataType()

        evtdata.enabled = self.model.enabled
        evtdata.running = self.model.running

        evtdata.currentSalIndex = self.model.current_index

        evtdata.length = min(len(self.model.queue), len(evtdata.salIndices))
        evtdata.salIndices[0:evtdata.length] = [info.index for info in self.model.queue][0:evtdata.length]
        evtdata.salIndices[evtdata.length:] = 0

        evtdata.pastLength = len(self.model.history)
        evtdata.pastSalIndices[0:evtdata.pastLength] = \
            [info.index for info in self.model.history][0:evtdata.pastLength]
        evtdata.pastSalIndices[evtdata.pastLength:] = 0

        self.evt_queue.put(evtdata, 1)

    def put_script(self, script_info):
        """Output information about a script as a ``script`` event.

        Designed to be used as a QueueModel script_callback.

        Parameters
        ----------
        script_info : `ScriptInfo`
            Information about the script.
        """
        if script_info is None:
            return

        sallib = self.salinfo.lib
        evtdata = self.evt_script.DataType()
        evtdata.cmdId = script_info.cmd_id
        evtdata.salIndex = script_info.index
        evtdata.path = script_info.path
        evtdata.isStandard = script_info.is_standard
        evtdata.timestamp = script_info.timestamp
        evtdata.duration = script_info.duration
        if script_info.done:
            returncode = script_info.process.returncode
            assert returncode is not None
            if returncode == 0:
                evtdata.processState = sallib.script_Complete
            elif returncode > 0:
                evtdata.processState = sallib.script_Failed
            else:
                evtdata.processState = sallib.script_Terminated
        elif script_info.running:
            evtdata.processState = sallib.script_Running
        elif script_info.configured:
            evtdata.processState = sallib.script_Configured
        else:
            evtdata.processState = sallib.script_Loading
        self.evt_script.put(evtdata, 1)
