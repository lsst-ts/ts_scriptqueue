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

__all__ = ["BlockInfo"]

import os
import re
from collections import deque

from lsst.ts.utils import ImageNameServiceClient

BLOCK_REGEX = re.compile(
    r"(?P<block_test_case>BLOCK-T)?(?P<block>BLOCK-)?(?P<id>[0-9]*)"
)


class BlockInfo:
    """Information about a block.

    A block is an ordered collection of Scripts. They
    contain a parent block id and a unique id for each
    block execution.

    Parameters
    ----------
    log : `logging.Logger`
        Parent logger.
    block_id : `str`
        Parent block id.
    block_size : `int`
        How many scripts are part of this block.
    """

    def __init__(self, log, block_id, block_size):
        self.log = log.getChild("BlockInfo")
        self.block_id = block_id
        self.block_size = block_size

        block_match = BLOCK_REGEX.match(block_id)
        if block_match.span()[1] == 0:
            raise ValueError(
                f"{block_id} has the wrong format, should be BLOCK-N or BLOCK-TN."
            )

        self._block_ticket_id = abs(int(block_match.groupdict()["id"]))
        self._block_type = (
            "BlockT"
            if block_match.groupdict()["block_test_case"] is not None
            else "Block"
        )

        self._block_uid = None
        self.scripts_info = deque(maxlen=block_size)

        self.image_server_url = os.environ.get("IMAGE_SERVER_URL")
        if self.image_server_url is None:
            raise ValueError(
                "IMAGE_SERVER_URL environment variable not defined. "
                "Block indexing functionality will not work."
            )

    def get_block_uid(self):
        """Retrieve block uid.

        Returns
        -------
        block_uid : `str`
            Block unique id.
        """
        if not self.has_uid():
            raise RuntimeError(
                "Block uid has not been set yet, call set_block_uid first."
            )

        return self._block_uid

    def has_uid(self):
        """Check if block uid was set.

        Returns
        -------
        `bool`
            True is uid is set, False otherwise.
        """
        return self._block_uid is not None

    async def set_block_uid(self):
        """Retrieve and set the block unique id from the name server."""

        if self._block_uid is not None:
            self.log.debug(f"Block uid already set: {self._block_uid}.")
            return

        image_server_client = ImageNameServiceClient(
            self.image_server_url, self._block_ticket_id, self._block_type
        )
        _, data = await image_server_client.get_next_obs_id(num_images=1)
        self._block_uid = data[0]

    def add(self, script_info):
        """Add Script to the block.

        Parameters
        ----------
        script_info : `ScriptInfo`
            ScriptInfo for the script to add to the block.
        """
        if self._block_uid is None:
            raise RuntimeError("Cannot add script to the block without a block uid.")

        if len(self.scripts_info) >= self.block_size:
            raise RuntimeError(
                "Block already filled with all the expected number of scripts. "
                f"Declared capacity is {self.block_size}."
            )

        script_info.set_block_id(self._block_uid)
        index = len(self.scripts_info)
        self.scripts_info.append(script_info)
        script_info.set_block_index(index)

    def done(self):
        """Check if block is done.

        A block is considered done is all the scripts that
        are part of it have finished.

        Returns
        -------
        `bool`
            True is block is done, False otherwise.
        """
        return all([script_info.process_done() for script_info in self.scripts_info])
