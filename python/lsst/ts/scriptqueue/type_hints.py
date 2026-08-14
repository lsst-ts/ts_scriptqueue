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

from collections.abc import Awaitable, Callable, Coroutine
from typing import Any, Protocol, runtime_checkable

from lsst.ts.xml.enums.ScriptQueue import ScriptProcessState


@runtime_checkable
class Indexed(Protocol):
    index: int


class ScriptInfoProtocol(Protocol):
    index: int
    group_id: str
    metadata: str | None
    script_state: int
    seq_num: int
    path: str
    is_standard: bool
    timestamp_process_start: float
    timestamp_configure_start: float
    timestamp_configure_end: float
    timestamp_run_start: float
    timestamp_process_end: float

    def set_block_id(self, block_id: str) -> None: ...

    def set_block_index(self, block_index: int) -> None: ...

    @property
    def process_done(self) -> bool: ...

    @property
    def process_state(self) -> ScriptProcessState: ...

    @property
    def running(self) -> bool: ...


class AsyncScriptInfoBoolCallback(Protocol):
    def __call__(self, script_info: ScriptInfoProtocol, force_output: bool = False) -> Awaitable[None]: ...


AsyncScriptInfoCallback = Callable[[ScriptInfoProtocol], Coroutine[Any, Any, None]]
AsyncNoArgsCallback = Callable[[], Coroutine[Any, Any, None]]
