# This file is part of scriptrunner.
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

__all__ = ["LoaderModel", "ScriptInfo"]

import asyncio
import functools
import os
import pathlib

from . import utils


_MaxIndex = (2 << 30) - 1


class Scripts:
    """Struct to hold relative paths to scripts.

    Parameters
    ----------
    standard : `iterable` of `str`
        Relative paths to standard scripts
    external : `iterable` of `str`
        Relative paths to external scripts
    """
    def __init__(self, standard, external):
        self.standard = standard
        self.external = external


class ScriptInfo:
    """Information about a loaded script.
    """
    def __init__(self, cmd_id, index, path, is_standard, process, task, timefunc):
        self.cmd_id = int(cmd_id)  # command ID
        self.index = int(index)  # index of Script SAL component
        self.path = str(path)  # relative path to script
        self.is_standard = bool(is_standard)  # is this a standard script?
        self.process = process  # script process
        self.task = task  # task on process.wait()
        self.timefunc = timefunc
        self.timestamp_start = timefunc()
        self.timestamp_end = 0
        self.task.add_done_callback(self._set_timestamp_end)

    def _set_timestamp_end(self, returncode=None):
        self.timestamp_end = self.timefunc()


class LoaderModel:
    """Code to load and configure scripts; implementation for ScriptLoader.

    Parameters
    ----------
    standardpath : `str`, `bytes` or `os.PathLike`
        Path to standard modules.
    externalpath : `str`, `bytes` or `os.PathLike`
        Path to external modules.
    timefunc : `callable`
        A function that returns the current SAL standard time as a float.
        It will be called with no arguments.

    Raises
    ------
    ValueError
        If ``standardpath`` or ``externalpath`` does not exist.
    TypeError
        If ``timefunc`` is not callable.
    """
    _previous_index = 0

    def __init__(self, standardpath, externalpath, timefunc):
        if not os.path.isdir(standardpath):
            raise ValueError(f"No such dir standardpath={standardpath}")
        if not os.path.isdir(externalpath):
            raise ValueError(f"No such dir externalpath={externalpath}")
        if not callable(timefunc):
            raise TypeError(f"timefunc={timefunc} is not callable")

        self.standardpath = standardpath
        self.externalpath = externalpath
        self.timefunc = timefunc
        # dict of script index: script info
        self.info = {}

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

    async def load(self, id_data, path, is_standard, callback=None):
        """Load a script.

        Start a script in a subprocess, set ``self.process`` to the
        resulting ``asyncio.Process``, set ``self.task`` to an
        ``asyncio.Task`` that waits for the process to finish,
        then wait for the process to start.

        Parameters
        ----------
        id_data : `salobj.topics.CommandIdData` (optional)
            Command ID and data. Ignored.
        path : `str`, `bytes` or `os.PathLike`
            Path to script, relative to standard or external root dir.
        is_standard : `bool`
            Is this a standard (True) or external (False) script?
        callback : `callable`
            A function that will be called when the process starts
            and finishes. It receives two argument:

            * The script info (a `ScriptInfo`).
            * The final return code, or None if not done

        Raises
        ------
        RuntimeError
            If the script does not exist or is not executable.
        """
        if callback and not callable(callback):
            raise TypeError(f"callback={callback} not callable")
        fullpath = self.makefullpath(path, is_standard)
        initialpath = os.environ["PATH"]
        scriptdir, scriptname = os.path.split(fullpath)
        try:
            index = self.next_index()
            os.environ["PATH"] = scriptdir + ":" + initialpath
            process = await asyncio.create_subprocess_exec(scriptname, str(index),
                                                           stdout=asyncio.subprocess.PIPE,
                                                           stderr=asyncio.subprocess.PIPE)
            task = asyncio.ensure_future(process.wait())
            script_info = ScriptInfo(cmd_id=id_data.id, index=index,
                                     process=process, task=task,
                                     path=path, is_standard=is_standard,
                                     timefunc=self.timefunc)
            self.info[index] = script_info
            if callback:
                callback(script_info, None)  # call for initial state
                task.add_done_callback(functools.partial(callback, script_info))
            return script_info
        finally:
            os.environ["PATH"] = initialpath

    def makefullpath(self, path, is_standard):
        """Make a full path from path and is_standard and check that
        it points to a runnable script.

        Parameters
        ----------
        path : `str`, `bytes` or `os.PathLike`
            Path to script, relative to standard or external root dir.
        is_standard : `bool`
            Is this a standard (True) or external (False) script?

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

    @classmethod
    def next_index(cls):
        """Get the next index for a Script SAL component.

        Starts at 1 and Wraps around to 1 after the maximum positive index.
        """
        cls._previous_index += 1
        if cls._previous_index > _MaxIndex:
            cls._previous_index = 1

        return cls._previous_index
