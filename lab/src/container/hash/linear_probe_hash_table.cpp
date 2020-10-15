//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// linear_probe_hash_table.cpp
//
// Identification: src/container/hash/linear_probe_hash_table.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "storage/table/tmp_tuple.h"
#include "storage/index/hash_comparator.h"
#include "container/hash/linear_probe_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::LinearProbeHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                      const KeyComparator &comparator, size_t num_buckets,
                                      HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  Page *page = buffer_pool_manager_->NewPage(&header_page_id_);
  page->WLatch();
  auto *hash_table_header = reinterpret_cast<HashTableHeaderPage *>(page->GetData());
  hash_table_header->SetPageId(header_page_id_);
  num_buckets = BLOCK_ARRAY_SIZE * ((num_buckets + BLOCK_ARRAY_SIZE - 1) / BLOCK_ARRAY_SIZE);
  hash_table_header->SetSize(num_buckets);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(header_page_id_, true);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();

  Page *header_page = buffer_pool_manager_->FetchPage(header_page_id_);
  header_page->RLatch();
  auto *header = reinterpret_cast<HashTableHeaderPage *>(header_page->GetData());
  size_t num_buckets = header->GetSize();
  size_t num_blocks = num_buckets  / BLOCK_ARRAY_SIZE;
  size_t total_idx, block_idx, bucket_idx;
  GetIndex(key, num_buckets, &total_idx, &block_idx, &bucket_idx);
  if (block_idx >= header->NumBlocks()) {
    header_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(header_page_id_, false);
    table_latch_.RUnlock();
    return false;
  }
  page_id_t block_page_id = header->GetBlockPageId(block_idx);
  Page *block_page = buffer_pool_manager_->FetchPage(block_page_id);
  block_page->RLatch();
  auto *block = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(block_page->GetData());
  while (block->IsOccupied(bucket_idx)) {
      if (block->IsReadable(bucket_idx) && comparator_(block->KeyAt(bucket_idx), key) == 0) {   // exist key
          result->push_back(block->ValueAt(bucket_idx));
      }
      bucket_idx++;
      if (block_idx * BLOCK_ARRAY_SIZE + bucket_idx == total_idx)
        break;
      if (bucket_idx == BLOCK_ARRAY_SIZE) {
        bucket_idx = 0;
        block_page->RUnlatch();
        buffer_pool_manager_->UnpinPage(block_page_id, false);
        block_idx = (block_idx + 1) % num_blocks;
        if (block_idx >= header->NumBlocks()) {
          header_page->RUnlatch();
          buffer_pool_manager_->UnpinPage(header_page_id_, false);
          table_latch_.RUnlock();
          return !result->empty();
        }
        block_page_id = header->GetBlockPageId(block_idx);
        block_page = buffer_pool_manager_->FetchPage(block_page_id);
        block_page->RLatch();
        block = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(block_page->GetData());
      }
  }
  block_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(block_page_id, false);
  header_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(header_page_id_, false);
  table_latch_.RUnlock();
  return !result->empty();
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
begin:
  table_latch_.RLock();

  Page *header_page = buffer_pool_manager_->FetchPage(header_page_id_);
  header_page->RLatch();

  auto *header = reinterpret_cast<HashTableHeaderPage *>(header_page->GetData());
  size_t num_buckets = header->GetSize();
  size_t num_blocks = num_buckets  / BLOCK_ARRAY_SIZE;
  size_t total_idx, block_idx, bucket_idx;
  GetIndex(key, num_buckets, &total_idx, &block_idx, &bucket_idx);
  bool header_modify = false;
  if (block_idx >= header->NumBlocks()) {
    page_id_t page_id;
    header_page->RUnlatch();
    header_page->WLatch();
    if (block_idx >= header->NumBlocks()) {
      for (auto i = header->NumBlocks(); i <= block_idx; ++i) {
        if (buffer_pool_manager_->NewPage(&page_id) == nullptr) {
          header_page->WUnlatch();
          buffer_pool_manager_->UnpinPage(header_page_id_, header_modify);
          table_latch_.RUnlock();
          return false;
        }
        header_modify = true;
        header->AddBlockPageId(page_id);
        buffer_pool_manager_->UnpinPage(page_id, false);
      }
      header_page->WUnlatch();
      header_page->RLatch();
    }
  }
  page_id_t block_page_id = header->GetBlockPageId(block_idx);
  Page *block_page = buffer_pool_manager_->FetchPage(block_page_id);
  block_page->WLatch();
  auto *block = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(block_page->GetData());
  while (true) {
    if (!block->IsOccupied(bucket_idx)) {
      block->Insert(bucket_idx, key, value);
      block_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(block_page_id, true);
      header_page->RUnlatch();
      buffer_pool_manager_->UnpinPage(header_page_id_, header_modify);
      table_latch_.RUnlock();
      return true;
    }
    if (block->IsReadable(bucket_idx) && comparator_(block->KeyAt(bucket_idx), key) == 0 &&
        block->ValueAt(bucket_idx) == value) {  // exist (key,value) pair
      block_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(block_page_id, false);
      header_page->RUnlatch();
      buffer_pool_manager_->UnpinPage(header_page_id_, header_modify);
      table_latch_.RUnlock();
      return false;
    }
    bucket_idx++;
    if (block_idx * BLOCK_ARRAY_SIZE + bucket_idx == total_idx) {
      block_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(block_page_id, false);
      header_page->RUnlatch();
      buffer_pool_manager_->UnpinPage(header_page_id_, header_modify);
      table_latch_.RUnlock();
      Resize(num_buckets);
      goto begin;
    }
    if (bucket_idx == BLOCK_ARRAY_SIZE) {
      bucket_idx = 0;
      block_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(block_page_id, false);
      block_idx = (block_idx + 1) % num_blocks;
      if (block_idx >= header->NumBlocks()) {
        header_page->RUnlatch();
        header_page->WLatch();
        if (block_idx >= header->NumBlocks()) {
          if (buffer_pool_manager_->NewPage(&block_page_id) == nullptr) {
            header_page->WUnlatch();
            buffer_pool_manager_->UnpinPage(header_page_id_, header_modify);
            table_latch_.RUnlock();
            return false;
          }
          header_modify = true;
          header->AddBlockPageId(block_page_id);
          header_page->WUnlatch();
          header_page->RLatch();
        }
      }
      block_page_id = header->GetBlockPageId(block_idx);
      block_page = buffer_pool_manager_->FetchPage(block_page_id);
      block_page->WLatch();
      block = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(block_page->GetData());
    }
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  Page *header_page = buffer_pool_manager_->FetchPage(header_page_id_);
  header_page->RLatch();

  auto *header = reinterpret_cast<HashTableHeaderPage *>(header_page->GetData());
  size_t num_buckets = header->GetSize();
  size_t num_blocks = num_buckets  / BLOCK_ARRAY_SIZE;
  size_t total_idx, block_idx, bucket_idx;
  GetIndex(key, num_buckets, &total_idx, &block_idx, &bucket_idx);
  if (block_idx >= header->NumBlocks()) {
    header_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(header_page_id_, false);
    table_latch_.RUnlock();
    return false;
  }
  bool remove = false;
  page_id_t block_page_id = header->GetBlockPageId(block_idx);
  Page *block_page = buffer_pool_manager_->FetchPage(block_page_id);
  block_page->WLatch();
  auto *block = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(block_page->GetData());
  while (block->IsOccupied(bucket_idx)) {
    if (block->IsReadable(bucket_idx) && comparator_(block->KeyAt(bucket_idx), key) == 0
        && block->ValueAt(bucket_idx) == value) {
      block->Remove(bucket_idx);
      remove = true;
      break;
    }
    bucket_idx++;
    if (block_idx * BLOCK_ARRAY_SIZE + bucket_idx == total_idx) {
      block_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(block_page_id, false);
      block_idx = (block_idx + 1) % num_blocks;
      if (block_idx >= header->NumBlocks()) {
        header_page->RUnlatch();
        buffer_pool_manager_->UnpinPage(header_page_id_, false);
        table_latch_.RUnlock();
        return remove;
      }
      block_page_id = header->GetBlockPageId(block_idx);
      block_page = buffer_pool_manager_->FetchPage(block_page_id);
      block_page->WLatch();
      block = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(block_page->GetData());
    }
  }
  block_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(block_page_id, true);
  header_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(header_page_id_, false);
  table_latch_.RUnlock();
  return remove;
}

/*****************************************************************************
 * RESIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) {
  table_latch_.WLock();
  size_t new_size = initial_size << 1;
  size_t new_num_blocks = new_size / BLOCK_ARRAY_SIZE;
  page_id_t old_header_page_id = header_page_id_;
  Page *header_page = buffer_pool_manager_->NewPage(&header_page_id_);
  header_page->WLatch();
  auto *header = reinterpret_cast<HashTableHeaderPage *>(header_page->GetData());
  header->SetPageId(header_page_id_);
  header->SetSize(new_size);
  for (size_t i = 0; i < new_num_blocks; ++i) {
    page_id_t block_page_id;
    buffer_pool_manager_->NewPage(&block_page_id);
    header->AddBlockPageId(block_page_id);
    buffer_pool_manager_->UnpinPage(block_page_id, false);
  }
  header_page->WUnlatch();
  header_page->RLatch();
  Page *old_header_page = buffer_pool_manager_->FetchPage(old_header_page_id);
  old_header_page->RLatch();
  auto *old_header = reinterpret_cast<HashTableHeaderPage *>(old_header_page->GetData());
  size_t num_buckets = header->GetSize();
  size_t num_blocks = num_buckets / BLOCK_ARRAY_SIZE;
  size_t total_idx, block_idx, bucket_idx;
  for (size_t i = 0; i < old_header->NumBlocks(); ++i) {
    page_id_t old_block_page_id = old_header->GetBlockPageId(i);
    Page *old_block_page = buffer_pool_manager_->FetchPage(old_block_page_id);
    old_block_page->RLatch();
    auto *old_block = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(old_block_page->GetData());
    for (size_t j = 0; j < BLOCK_ARRAY_SIZE; ++j) {
      if (old_block->IsReadable(j)) {
        KeyType key = old_block->KeyAt(j);
        ValueType value = old_block->ValueAt(j);
        GetIndex(key, num_buckets, &total_idx, &block_idx, &bucket_idx);
        page_id_t new_block_page_id = header->GetBlockPageId(block_idx);
        Page *new_block_page = buffer_pool_manager_->FetchPage(new_block_page_id);
        new_block_page->WLatch();
        auto *new_block = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(new_block_page->GetData());
        while(true){
          if (!new_block->IsOccupied(bucket_idx)) {
            new_block->Insert(bucket_idx, key, value);
            new_block_page->WUnlatch();
            buffer_pool_manager_->UnpinPage(new_block_page_id, true);
            break;
          }
          ++bucket_idx;
          if(bucket_idx == BLOCK_ARRAY_SIZE){
            bucket_idx = 0;
            new_block_page->WUnlatch();
            buffer_pool_manager_->UnpinPage(new_block_page_id, false);
            block_idx = (block_idx + 1) % num_blocks;
            new_block_page_id = header->GetBlockPageId(block_idx);
            new_block_page = buffer_pool_manager_->FetchPage(new_block_page_id);
            new_block_page->WLatch();
            new_block = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(new_block_page->GetData());
          }
        }
      }
    }
    old_block_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(old_block_page_id, false);
    buffer_pool_manager_->DeletePage(old_block_page_id);
  }
  header_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(header_page_id_, true);
  old_header_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(old_header_page_id, false);
  buffer_pool_manager_->DeletePage(old_header_page_id);
  table_latch_.WUnlock();
}

/*****************************************************************************
 * GETSIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_TYPE::GetSize() {
  table_latch_.RLock();
  auto *header_page =  buffer_pool_manager_->FetchPage(header_page_id_);
  header_page->RLatch();
  auto *header = reinterpret_cast<HashTableHeaderPage *>(header_page->GetData());
  size_t num_buckets = header->GetSize();
  header_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(header_page_id_, false);
  table_latch_.RUnlock();
  return num_buckets;
}

/*****************************************************************************
 * GETINDEX
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::GetIndex(const KeyType &key, const size_t &num_buckets, size_t *total_idx,
                               size_t *block_idx, size_t *bucket_idx) {
  uint64_t hash_value = hash_fn_.GetHash(key);
  *total_idx = hash_value % num_buckets;
  *block_idx = *total_idx / BLOCK_ARRAY_SIZE;
  *bucket_idx = *total_idx % BLOCK_ARRAY_SIZE;
}

template class LinearProbeHashTable<int, int, IntComparator>;
template class LinearProbeHashTable<hash_t, TmpTuple, HashComparator>;
template class LinearProbeHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class LinearProbeHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class LinearProbeHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class LinearProbeHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class LinearProbeHashTable<GenericKey<64>, RID, GenericComparator<64>>;


}  // namespace bustub
