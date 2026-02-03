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

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self};
use std::collections::BinaryHeap;
use std::collections::binary_heap::PeekMut;

use anyhow::{Result, anyhow};

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

// For BinaryHeap to compare the iter's order
impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        // unimplemented!()
        // Assume the iters are sorted by Version
        let mut heap = BinaryHeap::<HeapWrapper<I>>::new();
        let mut i = 0;
        for iter in iters {
            if !iter.is_valid() {
                continue;
            }
            heap.push(HeapWrapper(i, iter));
            i += 1;
        }
        let current = heap.pop();
        MergeIterator {
            iters: heap,
            current,
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        // unimplemented!()
        match &self.current {
            Some(wrapper) => wrapper.1.key(),
            None => KeySlice::from_slice(&[]),
        }
    }

    fn value(&self) -> &[u8] {
        match &self.current {
            Some(wrapper) => wrapper.1.value(),
            None => &[],
        }
    }

    fn is_valid(&self) -> bool {
        match &self.current {
            Some(wrapper) => wrapper.1.is_valid(),
            None => false,
        }
    }

    fn next(&mut self) -> Result<()> {
        if !self.is_valid() {
            return Err(anyhow::anyhow!("iterator has errored"));
        }
        // 1. 获取当前正在使用的迭代器（current 必然是有效值，除非已经迭代结束）
        let current_wrapper = self.current.as_mut().unwrap();

        // 2. 去重逻辑：
        while let Some(mut top) = self.iters.peek_mut() {
            if top.1.key() == current_wrapper.1.key() {
                if let Err(e) = top.1.next() {
                    PeekMut::pop(top);
                    self.current = None;
                    return Err(e);
                }
                if !top.1.is_valid() {
                    PeekMut::pop(top);
                }
                // 注意：PeekMut 在这里会自动根据 top.1.key() 重新平衡堆
            } else {
                break;
            }
        }

        // 3. 推进当前的迭代器
        current_wrapper.1.next()?;

        // 4. 将当前的迭代器重新放回堆（如果依然有效）
        // 这里需要把 current 拿出来
        let current_inner = self.current.take().unwrap();
        if current_inner.1.is_valid() {
            self.iters.push(current_inner);
        }

        // 5. 从堆中弹出新的最小值作为 current
        self.current = self.iters.pop();

        Ok(())
    }
}
