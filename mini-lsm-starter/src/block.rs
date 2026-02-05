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

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the course
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        // unimplemented!()
        // let num_of_elements = self.offsets.len();
        let mut data = self.data.clone();

        for &off in &self.offsets {
            data.put_u16(off);
        }
        data.put_u16(self.offsets.len() as u16);
        data.into()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let num_elements_ptr = data.len() - 2;
        let num_elements = (&data[num_elements_ptr..]).get_u16() as usize;

        let offsets_ptr = num_elements_ptr - num_elements * 2;

        // block.data use u8 as data type, can be converted by to_vec() directly.
        let kvdata = data[0..offsets_ptr].to_vec();

        // offsets needs transversal
        let mut offsets = Vec::with_capacity(num_elements);
        let mut offsets_data = &data[offsets_ptr..num_elements_ptr];
        while offsets_data.has_remaining() {
            offsets.push(offsets_data.get_u16());
        }
        Self {
            data: kvdata,
            offsets: offsets,
        }
    }
}
