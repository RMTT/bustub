//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <cstdio>
#include <exception>
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "pg_definitions.hpp"
#include "storage/table/table_heap.h"
#include "storage/table/table_iterator.h"
#include "storage/table/tuple.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), iter_(nullptr, RID(), nullptr), heap_(nullptr) {
  this->plan_ = plan;
}

void SeqScanExecutor::Init() {
  auto table_info = this->exec_ctx_->GetCatalog()->GetTable(this->plan_->GetTableOid());
  Assert(table_info != Catalog::NULL_TABLE_INFO);

  if (!(exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED)) {
    if (!exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_SHARED,
                                                table_info->oid_)) {
      throw std::exception();
    }
  }
  this->heap_ = table_info->table_.get();
  this->iter_ = this->heap_->Begin(this->exec_ctx_->GetTransaction());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (this->iter_ == this->heap_->End()) {
    return false;
  }

  Schema output_schema = this->plan_->OutputSchema();

  std::vector<Value> values;
  *rid = this->iter_->GetRid();

  if (!exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED,
                                            plan_->table_oid_, *rid)) {
    throw std::exception();
  }
  for (unsigned int i = 0; i < output_schema.GetColumnCount(); ++i) {
    values.push_back(this->iter_->GetValue(&output_schema, i));
  }

  *tuple = Tuple(values, &output_schema);

  ++this->iter_;

  return true;
}
}  // namespace bustub
