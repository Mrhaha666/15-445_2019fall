//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "storage/page/table_page.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new ClockReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.push_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  latch_.lock();
  auto iter = page_table_.find(page_id);
  frame_id_t frame_id;
  if (iter != page_table_.end()) {
    frame_id = iter->second;
    replacer_->Pin(frame_id);
    pages_[frame_id].pin_count_++;
    latch_.unlock();
    return &pages_[frame_id];
  }
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    goto update;
  }
  if (replacer_->Victim(&frame_id)) {
    if (pages_[frame_id].is_dirty_) {
      FlushPage(pages_[frame_id].page_id_);
    }
    page_table_.erase(pages_[frame_id].page_id_);
  } else {
    latch_.unlock();
    return nullptr;
  }
update:
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 1;
  disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
  page_table_[page_id] = frame_id;
  latch_.unlock();
  return &pages_[frame_id];
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  latch_.lock();
  frame_id_t frame_id;
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end() || pages_[frame_id = iter->second].pin_count_ <= 0) {
    latch_.unlock();
    return false;
  }
  pages_[frame_id].is_dirty_ = pages_[frame_id].is_dirty_ || is_dirty;
  pages_[frame_id].pin_count_--;
  if (pages_[frame_id].pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  }
  latch_.unlock();
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  frame_id_t frame_id;
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }
  frame_id = iter->second;


  auto *table_page = reinterpret_cast<TablePage *>(pages_[frame_id].data_);
  lsn_t page_lsn = table_page->GetLSN();
  while (enable_logging && page_lsn > log_manager_->GetPersistentLSN()){
    log_manager_->GetCv().notify_one();
    std::this_thread::yield();
  }

  disk_manager_->WritePage(page_id, pages_[frame_id].data_);
  pages_[frame_id].is_dirty_ = false;
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  latch_.lock();
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    goto update;
  }
  if (replacer_->Victim(&frame_id)) {
    if (pages_[frame_id].is_dirty_) {
      FlushPage(pages_[frame_id].page_id_);
    }
    page_table_.erase(pages_[frame_id].page_id_);
  } else {
    latch_.unlock();
    return nullptr;
  }
update:
  *page_id = disk_manager_->AllocatePage();
  pages_[frame_id].page_id_ = *page_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].ResetMemory();
  page_table_[*page_id] = frame_id;
  latch_.unlock();
  return &pages_[frame_id];
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  latch_.lock();
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    latch_.unlock();
    return true;
  }
  frame_id_t frame_id = iter->second;
  if (pages_[frame_id].pin_count_ > 0) {
    latch_.unlock();
    return false;
  }
  replacer_->Pin(frame_id);
  page_table_.erase(iter);
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].ResetMemory();
  free_list_.push_back(frame_id);
  disk_manager_->DeallocatePage(page_id);
  latch_.unlock();
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  latch_.lock();
  for (auto &iter : page_table_) {
    if (pages_[iter.second].is_dirty_) {
      FlushPage(iter.first);
    }
  }
  latch_.unlock();
}

}  // namespace bustub
