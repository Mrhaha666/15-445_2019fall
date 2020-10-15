//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_block_page.cpp
//
// Identification: src/storage/page/hash_table_block_page.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/util/hash_util.h"
#include "storage/table/tmp_tuple.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/hash_table_block_page.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BLOCK_TYPE::KeyAt(slot_offset_t bucket_ind) const {
  return array_[bucket_ind].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BLOCK_TYPE::ValueAt(slot_offset_t bucket_ind) const {
  return array_[bucket_ind].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::Insert(slot_offset_t bucket_ind, const KeyType &key, const ValueType &value) {
  if (IsOccupied(bucket_ind)) {
    return false;
  } else {
    auto ind = bucket_ind >> 3;
    auto offset = bucket_ind - (ind << 3);
    array_[bucket_ind] = std::make_pair(key, value);
    readable_[ind] |= (1 << offset);
    occupied_[ind] |= (1 << offset);
    return true;
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BLOCK_TYPE::Remove(slot_offset_t bucket_ind) {
  if (IsReadable(bucket_ind)) {
    auto ind = bucket_ind >> 3;
    auto offset = bucket_ind - (ind << 3);
    readable_[ind] &= ~(1 << offset);
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::IsOccupied(slot_offset_t bucket_ind) const {
  auto ind = bucket_ind >> 3;
  auto offset = bucket_ind - (ind << 3);
  return occupied_[ind] & ( 1 << offset);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::IsReadable(slot_offset_t bucket_ind) const {
  auto ind = bucket_ind >> 3;
  auto offset = bucket_ind - (ind << 3);
  return readable_[ind] & ( 1 << offset);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBlockPage<int, int, IntComparator>;
template class HashTableBlockPage<hash_t, TmpTuple, HashComparator>;

template class HashTableBlockPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBlockPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBlockPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBlockPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBlockPage<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
