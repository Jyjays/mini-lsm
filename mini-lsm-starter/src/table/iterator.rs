// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// #![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
// #![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use anyhow::{Ok, Result};

use super::SsTable;
use crate::{
    block::BlockIterator,
    iterators::StorageIterator,
    key::{KeyBytes, KeySlice},
};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let block = table.read_block_cached(0)?;
        let blk_iter = BlockIterator::create_and_seek_to_first(block);

        Ok(SsTableIterator {
            table,
            blk_iter,
            blk_idx: 0,
        })
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        let block = self.table.read_block_cached(0)?;
        self.blk_iter = BlockIterator::create_and_seek_to_first(block);
        self.blk_idx = 0;
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        // 1. 使用修正后的 find_block_idx (逻辑应为: meta.last_key < key)
        let mut blk_idx = table.find_block_idx(key);
        let block = table.read_block_cached(blk_idx)?;
        let mut blk_iter = BlockIterator::create_and_seek_to_key(block, key);
        // 2. 检查索引是否越界（即 key 比整个 SST 最大的 key 还要大）
        if blk_idx >= table.block_meta.len() {
            // 如果越界，返回一个无效的迭代器（blk_idx 设为越界值，blk_iter 为空或无效）
            // 注意：这里需要确保你有一个能创建“空”BlockIterator 的方法
            return Ok(SsTableIterator {
                table,
                blk_iter,
                blk_idx,
            });
        }

        // 3. 读取对应的 Block 并 seek
        // let block = table.read_block_cached(blk_idx)?;
        // let mut blk_iter = BlockIterator::create_and_seek_to_key(block, key);

        // 4. 【关键修正】处理间隙逻辑
        // 如果在当前 Block 没找到 >= key 的值，说明 key 落在两个 Block 之间
        // 我们必须跳到下一个 Block 的第一个 key
        if !blk_iter.is_valid() {
            blk_idx += 1;
            if blk_idx < table.block_meta.len() {
                let next_block = table.read_block_cached(blk_idx)?;
                blk_iter = BlockIterator::create_and_seek_to_first(next_block);
            }
        }

        Ok(SsTableIterator {
            table,
            blk_iter,
            blk_idx,
        })
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        self.blk_idx = self.table.find_block_idx(key);
        if self.blk_idx >= self.table.block_meta.len() {
            return Ok(());
        }
        let block = self.table.read_block_cached(self.blk_idx)?;
        self.blk_iter = BlockIterator::create_and_seek_to_key(block, key);

        // 关键：如果在这个 block 里没找到（比如 key 刚好落在两个 block 之间的间隙）
        // 需要跳转到下一个 block 的开头
        if !self.blk_iter.is_valid() {
            self.blk_idx += 1;
            if self.blk_idx < self.table.block_meta.len() {
                let block = self.table.read_block_cached(self.blk_idx)?;
                self.blk_iter = BlockIterator::create_and_seek_to_first(block);
            }
        }
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid() && self.blk_idx < self.table.block_meta.len()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        if !self.is_valid() {
            return Err(anyhow::anyhow!("SsTableIter is not valid!"));
        }
        self.blk_iter.next();
        if !self.blk_iter.is_valid() {
            self.blk_idx += 1;
            if self.blk_idx < self.table.block_meta.len() {
                let block = self.table.read_block_cached(self.blk_idx)?;
                self.blk_iter = BlockIterator::create_and_seek_to_first(block);
            } else {
                // no more block
                return Ok(());
            }
        }
        Ok(())
    }
}
