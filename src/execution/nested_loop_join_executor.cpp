//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <algorithm>
#include <cstdint>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/type.h"
#include "type/type_id.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)),
      in_right_(false) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  in_right_ = false;
  have_ = false;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple right_tuple;
  RID right_rid;
  std::vector<Value> values;
  Schema out_schema = plan_->OutputSchema();
  Schema left_schema = left_executor_->GetOutputSchema();
  Schema right_schema = right_executor_->GetOutputSchema();
  while (true) {
    if (!in_right_) {
      if (!left_executor_->Next(&left_tuple_, &left_rid_)) {
        return false;
      }
      in_right_ = true;
    }

    if (!right_executor_->Next(&right_tuple, &right_rid)) {
      in_right_ = false;
      right_executor_->Init();

      if (!have_) {
        if (plan_->GetJoinType() == JoinType::LEFT) {
          for (uint32_t i = 0; i < left_schema.GetColumnCount(); ++i) {
            values.emplace_back(left_tuple_.GetValue(&left_schema, i));
          }
          for (uint32_t i = 0; i < right_schema.GetColumnCount(); ++i) {
            TypeId type = right_schema.GetColumn(i).GetType();
            values.emplace_back(ValueFactory::GetNullValueByType(type));
          }
          *tuple = Tuple(values, &out_schema);
          return true;
        }
      }
      have_ = false;
      continue;
    }
    Value res = plan_->Predicate().EvaluateJoin(&left_tuple_, left_schema, &right_tuple, right_schema);
    CmpBool r = res.CompareEquals(ValueFactory::GetBooleanValue(true));
    if (r != CmpBool::CmpTrue) {
      continue;
    }

    for (uint32_t i = 0; i < left_schema.GetColumnCount(); ++i) {
      values.emplace_back(left_tuple_.GetValue(&left_schema, i));
    }
    for (uint32_t i = 0; i < right_schema.GetColumnCount(); ++i) {
      values.emplace_back(right_tuple.GetValue(&right_schema, i));
    }
    have_ = true;

    *tuple = Tuple(values, &out_schema);
    break;
  }

  return true;
}
}  // namespace bustub
