//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// log_recovery.cpp
//
// Identification: src/recovery/log_recovery.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "recovery/log_recovery.h"
#include <set>
#include "storage/page/table_page.h"

namespace bustub {
/*
 * deserialize a log record from log buffer
 * @return: true means deserialize succeed, otherwise can't deserialize cause
 * incomplete log record
 */
bool LogRecovery::DeserializeLogRecord(const char *data, LogRecord *log_record) {
  if(data + LogRecord::HEADER_SIZE > log_buffer_ + LOG_BUFFER_SIZE){
    return false;
  }
  memcpy(static_cast<void *>(log_record), data, LogRecord::HEADER_SIZE);
  int32_t log_size = log_record->GetSize();
  if(log_size <= 0 || data + log_size > log_buffer_ + LOG_BUFFER_SIZE){
    return false;
  }
  data += LogRecord::HEADER_SIZE;
  switch (log_record->log_record_type_) {
    case LogRecordType::INVALID:
      return false;
    case LogRecordType::INSERT:
      memcpy(&log_record->insert_rid_, data, sizeof(RID));
      // log_record->insert_rid_ = *reinterpret_cast<const RID *>(data);
      data += sizeof(RID);
      log_record->insert_tuple_.DeserializeFrom(data);
      break;
    case LogRecordType::MARKDELETE:
    case LogRecordType::APPLYDELETE:
    case LogRecordType::ROLLBACKDELETE:
      memcpy(&log_record->delete_rid_, data, sizeof(RID));
      // log_record->delete_rid_ = *reinterpret_cast<const RID *>(data);
      data += sizeof(RID);
      log_record->insert_tuple_.DeserializeFrom(data);
      break;
    case LogRecordType::UPDATE:
      memcpy(&log_record->update_rid_, data, sizeof(RID));
      // log_record->update_rid_ = *reinterpret_cast<const RID *>(data);
      log_record->old_tuple_.DeserializeFrom(data + sizeof(RID));
      data += sizeof(RID) + sizeof(uint32_t) + log_record->old_tuple_.GetLength();
      log_record->new_tuple_.DeserializeFrom(data);
      break;
    case LogRecordType::BEGIN:
    case LogRecordType::COMMIT:
    case LogRecordType::ABORT:
      break;
    case LogRecordType::NEWPAGE:
      memcpy(&log_record->prev_page_id_, data, 2 * sizeof(page_id_t));
      // log_record->prev_page_id_ = *reinterpret_cast<const page_id_t *>(data);
      // data += sizeof(page_id_t);
      // log_record->page_id_ = *reinterpret_cast<const page_id_t *>(data);
      break;
  }
  return true;
}

/*
 *redo phase on TABLE PAGE level(table/table_page.h)
 *read log file from the beginning to end (you must prefetch log records into
 *log buffer to reduce unnecessary I/O operations), remember to compare page's
 *LSN with log_record's sequence number, and also build active_txn_ table &
 *lsn_mapping_ table
 */
void LogRecovery::Redo() {
  assert(enable_logging == false);
  LogRecord log_record;
  int out_offset = 0;
  while (disk_manager_->ReadLog(log_buffer_, LOG_BUFFER_SIZE, out_offset)) {
    int inter_offset = 0;
    while (DeserializeLogRecord(log_buffer_ + inter_offset, &log_record)) {
      active_txn_[log_record.txn_id_] = log_record.lsn_;
      lsn_mapping_[log_record.lsn_] = out_offset + inter_offset;
      switch (log_record.log_record_type_) {
        case LogRecordType::INVALID:
          break;
        case LogRecordType::INSERT:
        {
          RID rid = log_record.insert_rid_;
          Page *page = buffer_pool_manager_->FetchPage(rid.GetPageId());
          auto *table_page = reinterpret_cast<TablePage *>(page->GetData());
          bool need_redo = table_page ->GetLSN() < log_record.lsn_;
          if (need_redo) {
            table_page->InsertTuple(log_record.insert_tuple_, &rid, nullptr, nullptr, nullptr);
            table_page->SetLSN(log_record.lsn_);
          }
          buffer_pool_manager_->UnpinPage(page->GetPageId(), need_redo);
        }
          break;
        case LogRecordType::MARKDELETE:
        case LogRecordType::APPLYDELETE:
        case LogRecordType::ROLLBACKDELETE:
        {
          RID rid = log_record.delete_rid_;
          Page *page = buffer_pool_manager_->FetchPage(rid.GetPageId());
          auto *table_page = reinterpret_cast<TablePage *>(page->GetData());
          bool need_redo = table_page ->GetLSN() < log_record.lsn_;
          if (need_redo) {
            if(log_record.log_record_type_ == LogRecordType::MARKDELETE) {
              table_page->MarkDelete(rid, nullptr, nullptr, nullptr);
            }else if (log_record.log_record_type_ == LogRecordType::APPLYDELETE) {
              table_page->ApplyDelete(rid, nullptr, nullptr);
            }else if(log_record.log_record_type_ == LogRecordType::ROLLBACKDELETE) {
              table_page->RollbackDelete(rid, nullptr, nullptr);
            }
            table_page->SetLSN(log_record.lsn_);
          }
          buffer_pool_manager_->UnpinPage(page->GetPageId(), need_redo);
        }
          break;
        case LogRecordType::UPDATE:
        {
          RID rid = log_record.update_rid_;
          Page *page = buffer_pool_manager_->FetchPage(rid.GetPageId());
          auto *table_page = reinterpret_cast<TablePage *>(page->GetData());
          bool need_redo = table_page ->GetLSN() < log_record.lsn_;
          if (need_redo) {
            table_page->UpdateTuple(log_record.new_tuple_, &log_record.old_tuple_, rid, nullptr, nullptr, nullptr);
            table_page->SetLSN(log_record.lsn_);
          }
          buffer_pool_manager_->UnpinPage(page->GetPageId(), need_redo);
        }
          break;
        case LogRecordType::BEGIN:
          break;
        case LogRecordType::COMMIT:
        case LogRecordType::ABORT:
          active_txn_.erase(log_record.txn_id_);
          break;
        case LogRecordType::NEWPAGE: {
          Page *page = buffer_pool_manager_->FetchPage(log_record.page_id_);
          auto *table_page = reinterpret_cast<TablePage *>(page->GetData());
          bool need_redo1 = table_page->GetLSN() < log_record.lsn_;
          if (need_redo1) {
            table_page->Init(log_record.page_id_, PAGE_SIZE, log_record.prev_page_id_, nullptr, nullptr);
            table_page->SetLSN(log_record.lsn_);
            if(log_record.prev_page_id_ != INVALID_PAGE_ID) {
              Page *prev_page = buffer_pool_manager_->FetchPage(log_record.prev_page_id_);
              auto *pre_table_page = reinterpret_cast<TablePage *>(prev_page->GetData());
              bool need_redo2 = pre_table_page->GetNextPageId() < log_record.lsn_;
              if(need_redo2) {
                pre_table_page->SetNextPageId(log_record.page_id_);
                pre_table_page->SetLSN(log_record.lsn_);
              }
              buffer_pool_manager_->UnpinPage(log_record.prev_page_id_, need_redo2);
            }
          }
          buffer_pool_manager_->UnpinPage(log_record.page_id_, need_redo1);
        }
          break;
      }
      inter_offset += log_record.GetSize();
    }
    out_offset += inter_offset;
  }

}

/*
 *undo phase on TABLE PAGE level(table/table_page.h)
 *iterate through active txn map and undo each operation
 */
void LogRecovery::Undo() {
  std::set<lsn_t, std::greater<>> undo_set;
  for(const auto &txn : active_txn_) {
    undo_set.insert(txn.second);
  }
  int buffer_off = lsn_mapping_[*undo_set.begin()];
  disk_manager_->ReadLog(log_buffer_, LOG_BUFFER_SIZE, buffer_off);
  while (!undo_set.empty()) {
    lsn_t lsn = *undo_set.begin();
    undo_set.erase(lsn);
    int log_offset = lsn_mapping_[lsn];
    int inter_buff_off;
    if(log_offset + LogRecord::HEADER_SIZE >= buffer_off + LOG_BUFFER_SIZE || log_offset < buffer_off) {
      disk_manager_->ReadLog(log_buffer_, LOG_BUFFER_SIZE, log_offset);
      buffer_off = log_offset;
      inter_buff_off = 0;
    } else {
      inter_buff_off = log_offset - buffer_off;
    }
    LogRecord log_record;
    while ( !DeserializeLogRecord(log_buffer_ + inter_buff_off, &log_record)) {
      disk_manager_->ReadLog(log_buffer_, LOG_BUFFER_SIZE, log_offset);
      buffer_off = log_offset;
      inter_buff_off = 0;
    }
    switch (log_record.GetLogRecordType()) {
      case LogRecordType::INVALID:
        break;
      case LogRecordType::INSERT:
      {
        RID rid = log_record.insert_rid_;
        Page *page = buffer_pool_manager_->FetchPage(rid.GetPageId());
        auto *table_page = reinterpret_cast<TablePage *>(page->GetData());
        table_page->ApplyDelete(rid, nullptr, nullptr);
        buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
      }
        break;
      case LogRecordType::MARKDELETE:
      case LogRecordType::APPLYDELETE:
      case LogRecordType::ROLLBACKDELETE:
      {
        RID rid = log_record.delete_rid_;
        Page *page = buffer_pool_manager_->FetchPage(rid.GetPageId());
        auto *table_page = reinterpret_cast<TablePage *>(page->GetData());
        if( log_record.log_record_type_ == LogRecordType::MARKDELETE) {
          table_page->RollbackDelete(rid, nullptr, nullptr);
        } else if (log_record.log_record_type_ == LogRecordType::APPLYDELETE) {
          table_page->InsertTuple(log_record.delete_tuple_, &rid, nullptr, nullptr, nullptr);
        } else if (log_record.log_record_type_ == LogRecordType::ROLLBACKDELETE) {
          table_page->MarkDelete(rid, nullptr, nullptr, nullptr);
        }
        buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
      }
        break;
      case LogRecordType::UPDATE:
      {
        RID rid = log_record.update_rid_;
        Page *page = buffer_pool_manager_->FetchPage(rid.GetPageId());
        auto *table_page = reinterpret_cast<TablePage *>(page->GetData());
        table_page->UpdateTuple(log_record.old_tuple_, &log_record.new_tuple_, rid, nullptr, nullptr, nullptr);
        buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
      }
        break;
      case LogRecordType::BEGIN:
        break;
      case LogRecordType::COMMIT:
        break;
      case LogRecordType::ABORT:
        break;
      case LogRecordType::NEWPAGE:
        break;
    }
    if (log_record.prev_lsn_ != INVALID_LSN) {
      undo_set.insert(log_record.prev_lsn_);
    }
  }

}

}  // namespace bustub
