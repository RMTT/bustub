#include "execution/executors/topn_executor.h"
#include <algorithm>
#include <queue>
#include <vector>
#include "execution/plans/topn_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  Tuple tuple;
  RID rid;
  Cmp cmp(*plan_);
  child_executor_->Init();

  tuples_.clear();
  // fake topn
  while (child_executor_->Next(&tuple, &rid)) {
    tuples_.emplace_back(tuple);
  }

  std::sort(tuples_.begin(), tuples_.end(), cmp);
  count_ = 0;
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (tuples_.empty() || count_ >= plan_->GetN() || count_ >= tuples_.size()) {
    return false;
  }

  if (count_ < plan_->GetN()) {
    *tuple = tuples_[count_++];
    *rid = tuple->GetRid();
    return true;
  }
  return false;
}

Cmp::Cmp(const TopNPlanNode &plan) : plan_(plan), schema_(plan_.OutputSchema()) {}
auto Cmp::operator()(Tuple &a, Tuple &b) -> bool {
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
