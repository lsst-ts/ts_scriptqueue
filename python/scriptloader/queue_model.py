# This file is part of scriptloader.
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

__all__ = ["QueueModel", "ScriptInfo"]

import asyncio
import functools
import os
import pathlib

import SALPY_Script
import salobj
from . import utils

_LOAD_TIMEOUT = 2  # seconds
_STATE_TIMEOUT = 15  # seconds; includes time to make Script SAL component
_CONFIGURE_TIMEOUT = 15  # seconds


class Scripts:
    """Struct to hold relative paths to scripts.

    Parameters
    ----------
    standard : ``iterable`` of `str`
        Relative paths to standard SAL scripts
    external : ``iterable`` of `str`
        Relative paths to external SAL scripts
    """
    def __init__(self, standard, external):
        self.standard = standard
        self.external = external


class ScriptInfo:
    """Information about a loaded script.

    Parameters
    ----------
    cmd_id : `int`
        ID of ``load`` command.
    is_standard : `bool`
        Is this a standard (True) or external (False) script?
    path : `str`, `bytes` or `os.PathLike`
        Path to script, relative to standard or external root dir.
    index : `int`
        Index of script. This must be unique among all Script SAL
        components that are currently running.
    remote : `salobj.Remote`
        Remote which talks to the ``Script`` SAL component.
    process : `asyncio.subprocess.Process`
        Process in which the ``Script`` SAL component is loaded.
    task : `asyncio.Task`
        Task awaiting ``process.wait()``
    """
    def __init__(self, cmd_id, is_standard, path, index, remote, process, task):
        self.cmd_id = int(cmd_id)
        self.is_standard = bool(is_standard)
        self.path = str(path)
        self.index = int(index)
        self.remote = remote
        self.process = process
        self.task = task
        self.timestamp_start = self.remote.salinfo.manager.getCurrentTime()
        """Time at which this ``ScriptInfo`` was constructed."""
        self.timestamp_end = 0
        """Time at which ``task`` ended, or 0 if still running."""
        self.task.add_done_callback(self._set_timestamp_end)

    def _set_timestamp_end(self, returncode=None):
        self.timestamp_end = self.remote.salinfo.manager.getCurrentTime()


class QueueModel:
    """Code to load and configure scripts; implementation for ScriptQueue.

    Parameters
    ----------
    standardpath : `str`, `bytes` or `os.PathLike`
        Path to standard SAL scripts.
    externalpath : `str`, `bytes` or `os.PathLike`
        Path to external SAL scripts.

    Raises
    ------
    ValueError
        If ``standardpath`` or ``externalpath`` does not exist.
    """
    def __init__(self, standardpath, externalpath):
        if not os.path.isdir(standardpath):
            raise ValueError(f"No such dir standardpath={standardpath}")
        if not os.path.isdir(externalpath):
            raise ValueError(f"No such dir externalpath={externalpath}")

        self.standardpath = standardpath
        self.externalpath = externalpath
        # dict of script index: script info
        self.info = {}
        self._index_generator = salobj.index_generator()

    def findscripts(self):
        """Find available scripts.

        Returns
        -------
        scripts : `Scripts`
            Paths to standard and external scripts.
        """
        return Scripts(
            standard=utils.findscripts(self.standardpath),
            external=utils.findscripts(self.externalpath),
        )

    async def load(self, cmd_id, path, is_standard, config, callback=None):
        """Load and optionally configure a script.

        Start a script in a subprocess, set ``self.process`` to the
        resulting ``asyncio.Process``, and set ``self.task`` to an
        ``asyncio.Task`` that waits for the process to finish.
        Wait for the process to start.
        Configure the script.

        Parameters
        ----------
        cmd_id : `int`
            Command ID; recorded in the script info.
        is_standard : `bool`
            Is this a standard (True) or external (False) script?
        path : `str`, `bytes` or `os.PathLike`
            Path to script, relative to standard or external root dir.
        config : `str` (optional)
            Configuration data as a YAML encoded string.
            If None then do not configure the script.
        callback : `callable`
            A function that will be called when the process starts
            and finishes. It receives two argument:

            * The script info (a `ScriptInfo`).
            * The final return code, or None if not done

        Returns
        -------
        remote : `salobj.Remote`
            A remote that talks to the script; note that
            remote.salinfo.index contains the script index.

        Raises
        ------
        RuntimeError
            If the script does not exist or is not executable.
        """
        if callback and not callable(callback):
            raise TypeError(f"callback={callback} not callable")
        fullpath = self.makefullpath(is_standard=is_standard, path=path)
        initialpath = os.environ["PATH"]
        scriptdir, scriptname = os.path.split(fullpath)
        try:
            os.environ["PATH"] = scriptdir + ":" + initialpath
            index = next(self._index_generator)
            remote = salobj.Remote(SALPY_Script, index)
            process = await asyncio.create_subprocess_exec(scriptname, str(index))
            task = asyncio.ensure_future(process.wait())
            script_info = ScriptInfo(cmd_id=cmd_id,
                                     is_standard=is_standard,
                                     path=path,
                                     index=index,
                                     remote=remote,
                                     process=process,
                                     task=task)
            task.add_done_callback(functools.partial(self._delete_script_info, index))
            self.info[index] = script_info
            if callback:
                callback(script_info, None)  # call for initial state
                task.add_done_callback(functools.partial(callback, script_info))

            if config is not None:
                try:
                    await remote.evt_state.next(timeout=_STATE_TIMEOUT)

                    config_data = remote.cmd_configure.DataType()
                    config_data.config = config
                    id_ack = await remote.cmd_configure.start(config_data, timeout=_CONFIGURE_TIMEOUT)
                    if id_ack.ack.ack != remote.salinfo.lib.SAL__CMD_COMPLETE:
                        raise salobj.ExpectedError(f"Configure command failed")
                except Exception:
                    self.terminate(script_info.index)
                    script_info.remote = None
                    raise

            return script_info
        finally:
            os.environ["PATH"] = initialpath

    def _delete_script_info(self, index, returncode):
        if returncode is None:
            return

        async def delete_shortly(index):
            await asyncio.sleep(0.1)
            del self.info[index]

        asyncio.ensure_future(delete_shortly(index))

    def makefullpath(self, is_standard, path):
        """Make a full path from path and is_standard and check that
        it points to a runnable script.

        Parameters
        ----------
        is_standard : `bool`
            Is this a standard (True) or external (False) script?
        path : `str`, `bytes` or `os.PathLike`
            Path to script, relative to standard or external root dir.

        Returns
        -------
        fullpath : `pathlib.Path`
            The full path to the script.

        Raises
        ------
        RuntimeError
            If The full path is not in the appropriate root path
            (``standardpath`` or ``externalpath``, depending on
            ``is_standard``).
        RuntimeError
            If the script does not exist or is not a file,
            is invisible (name starts with ".")
            or private (name starts with "_"),
            or is not executable.
        """
        root = pathlib.Path(self.standardpath if is_standard else self.externalpath)
        fullpath = root.joinpath(path)
        if root not in fullpath.parents:
            raise RuntimeError(f"path {path} is not relative to {root}")
        if not fullpath.is_file():
            raise RuntimeError(f"Cannot find script {fullpath}.")
        if fullpath.name[0] in (".", "_"):
            raise RuntimeError(f"script {path} is invisible or private")
        if not os.access(fullpath, os.X_OK):
            raise RuntimeError(f"Script {fullpath} is not executable.")
        return fullpath

    def terminate(self, index):
        """Terminate the specified script by sending SIGTERM.

        Parameters
        ----------
        index : `int`
            Index of Script SAL component to terminate.

        Raises
        ------
        RuntimeError
            If we have no information about that script, meaning
            it already finished or was never loaded.
        """
        script_info = self.info.get(index)
        if script_info is None:
            raise RuntimeError(f"Unknown script index {index}")
        if script_info.process is None:
            return
        if script_info.process.returncode is not None:
            return
        script_info.process.terminate()

    def terminate_all(self):
        """Terminate all subprocesses and return the number killed."""
        nkilled = 0
        for script_info in self.info.values():
            if script_info.process.returncode is None:
                nkilled += 1
                script_info.process.terminate()
        return nkilled
