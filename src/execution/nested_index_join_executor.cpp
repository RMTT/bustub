//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include <cstdint>
#include <vector>
#include "catalog/catalog.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      index_(exec_ctx->GetCatalog()->GetIndex(plan_->GetIndexOid())) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() { child_executor_->Init(); }

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple left_tuple;
  RID left_rid;
  Tuple right_tuple;
  Schema left_schema = child_executor_->GetOutputSchema();
  Schema right_schema = plan_->InnerTableSchema();
  Schema out_schema = plan_->OutputSchema();
  std::vector<RID> scan_results;
  std::vector<Value> values;

  while (true) {
    if (!child_executor_->Next(&left_tuple, &left_rid)) {
      return false;
    }
    values.clear();

    for (uint32_t i = 0; i < left_schema.GetColumnCount(); ++i) {
      values.emplace_back(left_tuple.GetValue(&left_schema, i));
    }

    Value key = plan_->KeyPredicate()->Evaluate(&left_tuple, child_executor_->GetOutputSchema());
    std::vector<Value> key_values{key};
    Tuple key_tuple(key_values, &index_->key_schema_);
    index_->index_->ScanKey(key_tuple, &scan_results, exec_ctx_->GetTransaction());
    if (scan_results.empty()) {
      if (plan_->GetJoinType() != JoinType::LEFT) {
        continue;
      }
      for (uint32_t i = 0; i < right_schema.GetColumnCount(); ++i) {
        TypeId type = right_schema.GetColumn(i).GetType();
        values.emplace_back(ValueFactory::GetNullValueByType(type));
      }
    } else {
      TableInfo *table = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());
      table->table_->GetTuple(scan_results[0], &right_tuple, exec_ctx_->GetTransaction());
      for (uint32_t i = 0; i < right_schema.GetColumnCount(); ++i) {
        values.emplace_back(right_tuple.GetValue(&right_schema, i));
      }
    }
    break;
  }

  *tuple = Tuple(values, &out_schema);

  return true;
}

}  // namespace bustub
