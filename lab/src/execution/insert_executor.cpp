//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

const Schema *InsertExecutor::GetOutputSchema() { return plan_->OutputSchema(); }

void InsertExecutor::Init() {
  SimpleCatalog *catalog = exec_ctx_->GetCatalog();
  table_oid_t table_oid = plan_->TableOid();
  table_metadata_ = catalog->GetTable(table_oid);
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple) {
  RID rid;
  if (child_executor_ == nullptr) {
    auto row_values = plan_->RawValues();
    for (size_t i = 0; i < row_values.size(); ++i) {
      Tuple tuple1(plan_->RawValuesAt(i), &table_metadata_->schema_);
      if (!table_metadata_->table_->InsertTuple(tuple1, &rid, exec_ctx_->GetTransaction())) {
        return false;
      }
    }
  } else {
    Tuple tuple2;
    child_executor_->Init();
    while (child_executor_->Next(&tuple2)) {
      if (!table_metadata_->table_->InsertTuple(tuple2, &rid, exec_ctx_->GetTransaction())) {
        return false;
      }
    }
  }
  return true;
}

}  // namespace bustub
