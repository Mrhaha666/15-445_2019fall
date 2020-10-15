//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/hash_join_executor.h"

#include <memory>
#include <vector>

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left, std::unique_ptr<AbstractExecutor> &&right)
    : AbstractExecutor(exec_ctx), plan_(plan),
      jht_("hash_table", exec_ctx_->GetBufferPoolManager(), jht_comp_, jht_num_buckets_, jht_hash_fn_),
      left_(std::move(left)), right_(std::move(right)) {}

/** @return the JHT in use. Do not modify this function, otherwise you will get a zero. */
// Uncomment me! const HT *GetJHT() const { return &jht_; }

void HashJoinExecutor::Init() {
  left_->Init();
  right_->Init();
  Tuple tuple;
  TmpTuplePage *tmp_tuple_page = GetNewTmpTuplePage();
  TmpTuple tmp_tuple{};
  while (left_->Next(&tuple)) {
    if (! tmp_tuple_page->Insert(tuple, &tmp_tuple)) {
      tmp_tuple_page = GetNewTmpTuplePage();
      tmp_tuple_page->Insert(tuple, &tmp_tuple);
    }
    hash_t hash_value = HashValues(&tuple, plan_->GetLeftPlan()->OutputSchema(), plan_->GetLeftKeys());
    jht_.Insert(exec_ctx_->GetTransaction(), hash_value, tmp_tuple);
  }
}


bool HashJoinExecutor::Next(Tuple *tuple) {
  if (!stage_output_tuples_.empty()) {
    *tuple = stage_output_tuples_.back();
    stage_output_tuples_.pop_back();
    return true;
  }
  Tuple right_tuple;
  const AbstractExpression *predicate = plan_->Predicate();
  const auto *left_plan = plan_->GetLeftPlan();
  const auto *right_plan = plan_->GetRightPlan();
  const auto *output_schema = plan_->OutputSchema();
  const auto *left_schema = left_plan->OutputSchema();
  const auto *right_schema = right_plan->OutputSchema();
  std::vector<TmpTuple> left_tmp_tuples(0);
  while (right_->Next(&right_tuple)) {
    hash_t hash_value = HashValues(&right_tuple, right_plan->OutputSchema(), plan_->GetRightKeys());
    jht_.GetValue(exec_ctx_->GetTransaction(), hash_value, &left_tmp_tuples);
    std::vector<Tuple> left_tuples(left_tmp_tuples.size());
    TmpTupleToTuple(left_tmp_tuples, &left_tuples);
    std::vector<Value> values(output_schema->GetColumnCount());
    for (auto & left_tuple : left_tuples) {
      if (predicate->EvaluateJoin(&left_tuple, left_plan->OutputSchema(),
                                  &right_tuple, right_plan->OutputSchema()).GetAs<bool>()) {
        const auto& output_columns = output_schema->GetColumns();
        for (size_t k = 0; k < values.size(); ++k) {
          values.at(k) = output_columns.at(k).GetExpr()
                             ->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema);
        }
        stage_output_tuples_.emplace_back(values, output_schema);
      }
    }
  }
  if (!stage_output_tuples_.empty()) {
    *tuple = stage_output_tuples_.back();
    stage_output_tuples_.pop_back();
    return true;
  }
  BufferPoolManager *bpm = exec_ctx_->GetBufferPoolManager();
  for(auto & elem : tmp_tuple_pages_){
    bpm->DeletePage(elem);
  }
  return false;
}
}  // namespace bustub
