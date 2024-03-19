//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <vector>

#include "catalog/column.h"
#include "concurrency/transaction.h"
#include "execution/executors/insert_executor.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), inserted_(false) {}

void InsertExecutor::Init() {
  auto table_info = this->exec_ctx_->GetCatalog()->GetTable(this->plan_->table_oid_);
  this->indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  this->table_info_ = table_info;
  if (!exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE,
                                              table_info->oid_)) {
    throw std::exception();
  }
  child_executor_->Init();
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple value_tuple;
  RID id;
  Schema table_schema = table_info_->schema_;
  int count = 0;

  if (inserted_) {
    return false;
  }

  Schema value_schema = this->child_executor_->GetOutputSchema();
  while (this->child_executor_->Next(&value_tuple, &id)) {
    RID tmp_id;
    if (this->table_info_->table_->InsertTuple(value_tuple, &tmp_id, this->exec_ctx_->GetTransaction())) {
      if (!exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE,
                                                plan_->table_oid_, tmp_id)) {
        throw std::exception();
      }
      auto write_set = exec_ctx_->GetTransaction()->GetWriteSet();
      write_set->emplace_back(tmp_id, WType::INSERT, value_tuple, table_info_->table_.release());
      count++;
      for (auto index : this->indexes_) {
        Schema key_schema = index->key_schema_;
        std::vector<Value> values;

        for (auto &key_column : key_schema.GetColumns()) {
          auto col_idx = table_schema.GetColIdx(key_column.GetName());
          values.emplace_back(value_tuple.GetValue(&value_schema, col_idx));
        }
        Tuple key_tuple = Tuple(values, &key_schema);
        index->index_->InsertEntry(key_tuple, tmp_id, this->exec_ctx_->GetTransaction());
      }
    } else {
      return false;
    }
  }

  Schema output_schema = this->plan_->OutputSchema();
  std::vector<Value> values;
  values.emplace_back(INTEGER, count);
  *tuple = Tuple(values, &output_schema);
  inserted_ = true;
  return true;
}
}  // namespace bustub
