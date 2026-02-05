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

use crate::key::{Key, KeySlice, KeyVec};

use bytes::Buf;

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        // unimplemented!()
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to_index(0);
    }

    fn seek_to_index_util(block: &Block, index: usize) -> (&[u8], (usize, usize)) {
        let offset = block.offsets[index] as usize;
        let mut data_ptr = &block.data[offset..];

        // Parse key length and content
        let key_len = data_ptr.get_u16() as usize;
        let key_content = &data_ptr[..key_len];
        data_ptr.advance(key_len);

        // Parse value length and compute its range in block.data
        let value_len = data_ptr.get_u16() as usize;
        let value_start = block.data.len() - data_ptr.len();
        let value_end = value_start + value_len;

        (key_content, (value_start, value_end))
    }
    pub fn seek_to_index(&mut self, index: usize) {
        if index >= self.block.offsets.len() {
            self.key.clear();
            return;
        }

        let (key_content, (value_start, value_end)) = Self::seek_to_index_util(&self.block, index);
        self.key.clear();
        self.key.append(key_content);
        self.value_range = (value_start, value_end);
        self.idx = index;
    }
    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.seek_to_index(self.idx + 1);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        // 寻找第一个满足 k >= key 的索引
        let index = self.block.offsets.partition_point(|&offset| {
            let mut data_ptr = &self.block.data[offset as usize..];
            let key_len = data_ptr.get_u16() as usize;
            let k = &data_ptr[..key_len];
            KeySlice::from_slice(k) < key
        });

        self.seek_to_index(index);
    }
}
