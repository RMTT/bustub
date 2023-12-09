#include "execution/executors/sort_executor.h"
#include <algorithm>
#include "binder/bound_order_by.h"
#include "execution/plans/sort_plan.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  Tuple tuple;
  RID rid;
  child_executor_->Init();
  tuples_.clear();

  while (child_executor_->Next(&tuple, &rid)) {
    tuples_.emplace_back(tuple);
  }

  std::sort(tuples_.begin(), tuples_.end(), Cmp(*plan_));
  iter_ = tuples_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == tuples_.end()) {
    return false;
  }

  *tuple = *iter_;
  *rid = iter_->GetRid();
  iter_++;
  return true;
}

SortExecutor::Cmp::Cmp(const SortPlanNode &plan) : plan_(plan), schema_(plan_.OutputSchema()) {}
auto SortExecutor::Cmp::operator()(Tuple &a, Tuple &b) -> bool {
  for (auto &order_by : plan_.order_bys_) {
    auto &type = order_by.first;
    auto ref = order_by.second;

    Value x = ref->Evaluate(&a, schema_);
    Value y = ref->Evaluate(&b, schema_);
    if (x.CompareEquals(y) == CmpBool::CmpTrue) {
      continue;
    }
    if (type == OrderByType::ASC || type == OrderByType::DEFAULT) {
      return x.CompareLessThanEquals(y) == CmpBool::CmpTrue;
    }
    if (type == OrderByType::DESC) {
      return x.CompareGreaterThanEquals(y) == CmpBool::CmpTrue;
    }
  }
  return false;
}
}  // namespace bustub
