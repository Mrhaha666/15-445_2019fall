#pragma once

#include "storage/page/page.h"
#include "storage/table/tmp_tuple.h"
#include "storage/table/tuple.h"

namespace bustub {

// To pass the test cases for this class, you must follow the existing TmpTuplePage format and implement the
// existing functions exactly as they are! It may be helpful to look at TablePage.
// Remember that this task is optional, you get full credit if you finish the next task.

/**
 * TmpTuplePage format:
 *
 * Sizes are in bytes.
 * | PageId (4) | LSN (4) | FreeSpace (4) | (free space) | TupleSize2 | TupleData2 | TupleSize1 | TupleData1 |
 *
 * We choose this format because DeserializeExpression expects to read Size followed by Data.
 */
class TmpTuplePage : public Page {
 public:
  void Init(page_id_t page_id, uint32_t page_size) {
    memcpy(GetData(), &page_id, sizeof(page_id_t));
    memcpy(GetData() + OFFSET_FREE_SPACE, &page_size, sizeof(uint32_t));
  }

  page_id_t GetTablePageId() { return *reinterpret_cast<page_id_t *>(GetData()); }

  bool Insert(const Tuple &tuple, TmpTuple *out) {
    BUSTUB_ASSERT(tuple.GetLength() > 0, "Cannot have empty tuples.");
    if (GetFreeSpaceRemaining() < tuple.GetLength() + SIZE_TUPLE) {
      return false;
    }
    uint32_t tuple_len = tuple.GetLength();
    SetFreeSpaceRemaining(GetFreeSpaceRemaining() - tuple_len);
    memcpy(GetData() + GetFreeSpaceOffset(), tuple.GetData(), tuple_len);
    size_t offset = GetFreeSpaceOffset();
    SetFreeSpaceRemaining(GetFreeSpaceRemaining() - SIZE_TUPLE);
    memcpy(GetData() + GetFreeSpaceOffset(), &tuple_len, SIZE_TUPLE);
    *out = TmpTuple(GetTablePageId(), offset);
    return true;
  }

  void GetTuple(size_t offset, Tuple *tuple){
    memcpy(&tuple->size_, GetData() + offset - SIZE_TUPLE, SIZE_TUPLE) ;
    if (tuple->allocated_) {
      delete[] tuple->data_;
    }
    tuple->data_ = new char[tuple->size_];
    memcpy(tuple->data_, GetData() + offset, tuple->size_);
    tuple->allocated_ = true;
  }

  uint32_t GetFreeSpaceRemaining() {
    return *reinterpret_cast<uint32_t *>(GetData() + OFFSET_FREE_SPACE);
  }

  uint32_t GetFreeSpaceOffset() {
    return GetFreeSpaceRemaining() + SIZE_TABLE_PAGE_HEADER;
  }

  void SetFreeSpaceRemaining(uint32_t free_space_remaining) {
    memcpy(GetData() + OFFSET_FREE_SPACE, &free_space_remaining, sizeof(uint32_t));
  }

 private:
  static_assert(sizeof(page_id_t) == 4);
  static constexpr size_t SIZE_TUPLE = 4;
  static constexpr size_t OFFSET_FREE_SPACE = 8;
  static constexpr size_t SIZE_TABLE_PAGE_HEADER = 12;
};

}  // namespace bustub
