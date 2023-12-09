//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include "storage/index/b_plus_tree_index.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      tree_(dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(
          exec_ctx_->GetCatalog()->GetIndex(this->plan_->index_oid_)->index_.get())),
      iter_(tree_->GetBeginIterator()) {}

void IndexScanExecutor::Init() { iter_ = tree_->GetBeginIterator(); }

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_.IsEnd()) {
    return false;
  }
  RID id = (*iter_).second;

  *rid = id;
  auto table_info = exec_ctx_->GetCatalog()->GetTable(tree_->GetMetadata()->GetTableName());
  table_info->table_->GetTuple(id, tuple, exec_ctx_->GetTransaction());

  ++iter_;
  return true;
}

}  // namespace bustub
