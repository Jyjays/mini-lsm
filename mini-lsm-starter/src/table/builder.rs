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
use std::{mem, path::Path};

use anyhow::Result;
use bytes::{BufMut, Bytes};

use super::{BlockMeta, SsTable};
use crate::table::FileObject;
use crate::{
    block::BlockBuilder,
    key::{KeyBytes, KeySlice},
    lsm_storage::BlockCache,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        // unimplemented!()
        SsTableBuilder {
            builder: BlockBuilder::new(block_size),
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        // unimplemented!()
        if self.builder.add(key, value) {
            self.last_key = Vec::from(key.raw_ref());
            // if empty
            if self.first_key.is_empty() {
                self.first_key = self.last_key.clone();
            }
            return;
        }
        // get the old builder, create a new builder
        let old_builder = mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let block = old_builder.build();
        let encoded_block = block.encode();

        let offset = self.data.len();
        self.data.extend_from_slice(&encoded_block);
        // full ,add a new BlockMeta, then add again

        let first_keyb = Bytes::copy_from_slice(&self.first_key);
        let last_keyb = Bytes::copy_from_slice(&self.last_key);
        let meta = BlockMeta::new(
            offset,
            KeyBytes::from_bytes(first_keyb),
            KeyBytes::from_bytes(last_keyb),
        );
        self.meta.push(meta);

        let _ = self.builder.add(key, value);
        self.last_key = Vec::from(key.raw_ref());
        self.first_key = self.last_key.clone();
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        // unimplemented!()
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        #[allow(unused_mut)] mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        if !self.builder.is_empty() {
            let old_builder =
                std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
            let block = old_builder.build();
            let encoded_block = block.encode();

            // 记录这最后一个 Block 的元数据
            self.meta.push(BlockMeta {
                offset: self.data.len(),
                first_key: KeyBytes::from_bytes(Bytes::copy_from_slice(&self.first_key)),
                last_key: KeyBytes::from_bytes(Bytes::copy_from_slice(&self.last_key)),
            });

            self.data.extend_from_slice(&encoded_block);
        }

        let meta_offset = self.data.len();
        BlockMeta::encode_block_meta(&self.meta, &mut self.data); // write meta information to data.
        self.data.put_u32(meta_offset as u32);
        let file = FileObject::create(path.as_ref(), self.data)?;

        let first_key = &self.meta.first().unwrap().first_key;
        let last_key = &self.meta.last().unwrap().last_key;
        Ok(SsTable {
            file,
            block_meta: self.meta.clone(),
            block_meta_offset: meta_offset,
            id,
            block_cache,
            first_key: first_key.clone(),
            last_key: last_key.clone(),
            bloom: None,
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
