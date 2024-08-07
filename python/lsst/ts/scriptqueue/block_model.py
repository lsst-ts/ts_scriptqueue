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

__all__ = [
    "BlockModel",
]


class BlockModel:
    """Manages block information.

    Blocks are a collection of scripts that must be executed
    together by the ScriptQueue. They must be executed in the
    order in which they are added to the queue and cannot have
    interleaving scripts between them. If one script of the block
    fails the entire block must fail.
    """

    def __init__(self):
        self.blocks = dict()
        self.current_blocks = dict()

    def add_block(self, block_info):
        """Add block info to the list of blocks.

        When a new block is added it will become the "current"
        block of its type. If another block of the same type
        is currently incomplete, it is going to be moved down
        the list.

        Parameters
        ----------
        block_info : `BlockInfo`
            Information about a block.
        """
        if block_info.block_id not in self.blocks:
            self.blocks[block_info.block_id] = {
                block_info.get_block_uid(): block_info,
            }
        else:
            self.blocks[block_info.block_id][block_info.get_block_uid()] = block_info

        self.current_blocks[block_info.block_id] = block_info.get_block_uid()

    def get_current_block(self, block_id):
        """Return the BlockInfo for the current block.

        Parameters
        ----------
        block_id : `str`
            Id of the block, e.g. BLOCK-000.

        Returns
        -------
        block_info : `BlockInfo`
            Info about the current block.
        """

        if block_id not in self.current_blocks:
            raise ValueError(f"{block_id} not in the list of current blocks.")

        block_info = self.blocks[block_id][self.current_blocks[block_id]]

        return block_info

    def remove_done_blocks(self):
        """Remove blocks that have already finished."""

        for block in self.blocks:
            blocks_to_remove = []
            for block_id in self.blocks[block]:
                if block_id == self.current_blocks[block]:
                    continue
                elif self.block[block][block_id].done():
                    blocks_to_remove.append(block_id)
            _ = [self.blocks[block].pop(block_id) for block_id in blocks_to_remove]
