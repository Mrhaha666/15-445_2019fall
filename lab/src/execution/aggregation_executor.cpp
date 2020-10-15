//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child)),
      aht_(SimpleAggregationHashTable(plan_->GetAggregates(), plan_->GetAggregateTypes())),
      aht_iterator_(aht_.Begin()) {}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

const Schema *AggregationExecutor::GetOutputSchema() { return plan_->OutputSchema(); }

void AggregationExecutor::Init() {
  child_->Init();
  Tuple tuple;
  while (child_->Next(&tuple)) {
    aht_.InsertCombine(MakeKey(&tuple), MakeVal(&tuple));
  }
  aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple) {
  const auto *output_schema = plan_->OutputSchema();
  const auto& output_column = output_schema->GetColumns();
  std::vector<Value> values(output_schema->GetColumnCount());
  while (aht_iterator_ != aht_.End()) {
    auto group_bys = aht_iterator_.Key().group_bys_;
    auto aggregates = aht_iterator_.Val().aggregates_;
    if ((plan_->GetHaving() == nullptr) || plan_->GetHaving()->EvaluateAggregate(group_bys, aggregates).GetAs<bool>()) {
      for (size_t i = 0; i < values.size(); ++i) {
        values.at(i) = output_column.at(i).GetExpr()->EvaluateAggregate(group_bys, aggregates);
      }
      *tuple = Tuple(values, output_schema);
      ++aht_iterator_;
      return true;
    }
    ++aht_iterator_;
  }
  return false;
}

}  // namespace bustub
