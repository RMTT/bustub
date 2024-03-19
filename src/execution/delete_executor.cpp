//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), deleted_(false) {}

void DeleteExecutor::Init() {
  auto table_info = this->exec_ctx_->GetCatalog()->GetTable(this->plan_->table_oid_);
  this->indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  this->table_info_ = table_info;
  if (!exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE,
                                              table_info->oid_)) {
    throw std::exception();
  }
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  int count = 0;
  Tuple delete_tuple;
  RID id;
  Schema table_schema = table_info_->schema_;

  if (deleted_) {
    return false;
  }

  child_executor_->Init();
  Schema delete_schema = child_executor_->GetOutputSchema();
  while (this->child_executor_->Next(&delete_tuple, &id)) {
    if (!exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE,
                                              plan_->table_oid_, id)) {
      throw std::exception();
    }
    auto write_set = exec_ctx_->GetTransaction()->GetWriteSet();
    write_set->emplace_back(id, WType::INSERT, delete_tuple, table_info_->table_.release());
    if (this->table_info_->table_->MarkDelete(id, this->exec_ctx_->GetTransaction())) {
      count++;
      for (auto index : this->indexes_) {
        Schema key_schema = index->key_schema_;
        std::vector<Value> values;

        for (auto &key_column : key_schema.GetColumns()) {
          auto col_idx = table_schema.GetColIdx(key_column.GetName());
          values.emplace_back(delete_tuple.GetValue(&delete_schema, col_idx));
        }
        Tuple key_tuple = Tuple(values, &key_schema);
        index->index_->DeleteEntry(key_tuple, id, this->exec_ctx_->GetTransaction());
      }
    } else {
      return false;
    }
  }

  Schema output_schema = this->plan_->OutputSchema();
  std::vector<Value> values;
  values.emplace_back(Value(INTEGER, count));
  *tuple = Tuple(values, &output_schema);
  deleted_ = true;
  return true;
}
}  // namespace bustub
