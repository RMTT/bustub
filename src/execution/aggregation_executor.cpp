//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"
#include "type/value.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()),
      child_empty_(false) {}

void AggregationExecutor::Init() {
  Tuple tuple;
  RID rid;

  child_->Init();
  aht_.Clear();
  while (child_->Next(&tuple, &rid)) {
    aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }

  aht_iterator_ = aht_.Begin();

  if (aht_iterator_ == aht_.End()) {
    child_empty_ = true;
  }
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ == aht_.End() && !child_empty_) {
    return false;
  }
  std::vector<Value> values;
  if (child_empty_) {
    if (!plan_->group_bys_.empty()) {
      return false;
    }
    std::vector<Value> defaults = aht_.GenerateInitialAggregateValue().aggregates_;

    values.insert(values.end(), defaults.begin(), defaults.end());
    ;
    child_empty_ = false;
  } else {
    values.insert(values.end(), aht_iterator_.Key().group_bys_.begin(), aht_iterator_.Key().group_bys_.end());
    values.insert(values.end(), aht_iterator_.Val().aggregates_.begin(), aht_iterator_.Val().aggregates_.end());
    ++aht_iterator_;
  }

  *tuple = Tuple(values, &plan_->OutputSchema());
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
