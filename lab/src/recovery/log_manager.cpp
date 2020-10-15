//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// log_manager.cpp
//
// Identification: src/recovery/log_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "recovery/log_manager.h"

namespace bustub {
/*
 * set enable_logging = true
 * Start a separate thread to execute flush to disk operation periodically
 * The flush can be triggered when timeout or the log buffer is full or buffer
 * pool manager wants to force flush (it only happens when the flushed page has
 * a larger LSN than persistent LSN)
 *
 * This thread runs forever until system shutdown/StopFlushThread
 */
void LogManager::RunFlushThread() {
  if (!enable_logging) {
    enable_logging = true;
    flush_thread_ = new std::thread(std::mem_fn(&LogManager::FlushLog), this);
  }
}

/*
 * Stop and join the flush thread, set enable_logging = false
 */
void LogManager::StopFlushThread() {
  if (enable_logging) {
    enable_logging = false;
    flush_thread_->join();
    delete flush_thread_;
  }
}

/*
 * append a log record into log buffer
 * you MUST set the log record's lsn within this method
 * @return: lsn that is assigned to this log record
 *
 *
 * example below
 * // First, serialize the must have fields(20 bytes in total)
 * log_record.lsn_ = next_lsn_++;
 * memcpy(log_buffer_ + offset_, &log_record, 20);
 * int pos = offset_ + 20;
 *
 * if (log_record.log_record_type_ == LogRecordType::INSERT) {
 *    memcpy(log_buffer_ + pos, &log_record.insert_rid_, sizeof(RID));
 *    pos += sizeof(RID);
 *    // we have provided serialize function for tuple class
 *    log_record.insert_tuple_.SerializeTo(log_buffer_ + pos);
 *  }
 *
 */
lsn_t LogManager::AppendLogRecord(LogRecord *log_record) {
  latch_.lock();
  while (log_offset_ + log_record->GetSize() > LOG_BUFFER_SIZE){
    latch_.unlock();
    cv_.notify_one();
    std::this_thread::yield();
    latch_.lock();
  }
  log_record->lsn_ = next_lsn_++;
  memcpy(log_buffer_ + log_offset_, log_record, 20);
  int pos = log_offset_ + 20;
  switch(log_record->log_record_type_){
    case LogRecordType::INVALID:
      break;
    case LogRecordType::INSERT:
      memcpy(log_buffer_ + pos, &log_record->insert_rid_, sizeof(RID));
      pos += sizeof(RID);
      log_record->insert_tuple_.SerializeTo(log_buffer_ + pos);
      pos += sizeof(int32_t) + log_record->insert_tuple_.GetLength();
      break;
    case LogRecordType::MARKDELETE:
    case LogRecordType::ROLLBACKDELETE:
    case LogRecordType::APPLYDELETE:
      memcpy(log_buffer_ + pos, &log_record->delete_rid_,sizeof(RID));
      pos += sizeof(RID);
      log_record->delete_tuple_.SerializeTo(log_buffer_ + pos);
      pos += sizeof(int32_t) + log_record->delete_tuple_.GetLength();
      break;
    case LogRecordType::UPDATE:
      memcpy(log_buffer_ + pos, &log_record->update_rid_, sizeof(RID));
      pos += sizeof(RID);
      log_record->old_tuple_.SerializeTo(log_buffer_ + pos);
      pos += sizeof(int32_t) + log_record->old_tuple_.GetLength();
      log_record->new_tuple_.SerializeTo(log_buffer_ + pos);
      pos += sizeof(int32_t) + log_record->new_tuple_.GetLength();
      break;
    case LogRecordType::NEWPAGE:
      memcpy(log_buffer_ + pos, &log_record->prev_page_id_, 2 * sizeof(page_id_t));
      pos += 2 * sizeof(page_id_t);
      break;
    case LogRecordType::BEGIN:
      break;
    case LogRecordType::COMMIT:
      break;
    case LogRecordType::ABORT:
      break;
  }
  log_offset_ = pos;
  latch_.unlock();
  return log_record->lsn_;
}

void LogManager::FlushLog() {
  while (enable_logging) {
    lsn_t persistent_lsn;
      {
        std::unique_lock<std::mutex> latch(latch_);
        cv_.wait_for(latch, log_timeout);
        persistent_lsn = next_lsn_ - 1;
        std::swap(log_buffer_, flush_buffer_);
        std::swap(log_offset_, flush_offset_);
      }
    disk_manager_->WriteLog(flush_buffer_, flush_offset_);
    flush_offset_ = 0;
    SetPersistentLSN(persistent_lsn);
  }
}

}  // namespace bustub
