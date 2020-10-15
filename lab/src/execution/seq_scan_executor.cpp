//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  SimpleCatalog *catalog = exec_ctx_->GetCatalog();
  table_oid_t table_oid = plan_->GetTableOid();
  table_metadata_ = catalog->GetTable(table_oid);
  iter_ = table_metadata_->table_->Begin(exec_ctx_->GetTransaction());
}

bool SeqScanExecutor::Next(Tuple *tuple) {
  auto predicate = plan_->GetPredicate();
  while (iter_ != table_metadata_->table_->End() && (predicate != nullptr) &&
         !predicate->Evaluate(iter_.operator->(), GetOutputSchema()).GetAs<bool>()) {
    ++iter_;
  }
  if (iter_ != table_metadata_->table_->End()) {
    *tuple = *iter_.operator->();
    ++iter_;
    return true;
  }
  return false;
}

}  // namespace bustub
