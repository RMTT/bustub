//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include <algorithm>
#include <iostream>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

const int ADD_TABLE_LOCK = 1;
const int DEL_TABLE_LOCK = 2;

const int ADD_ROW_LOCK = 1;
const int DEL_ROW_LOCK = 2;

auto LockCompatible(LockManager::LockMode a, LockManager::LockMode b) -> bool {
  switch (a) {
    case LockManager::LockMode::SHARED:
      return b == LockManager::LockMode::SHARED || b == LockManager::LockMode::INTENTION_SHARED;
      break;
    case LockManager::LockMode::EXCLUSIVE:
      return false;
      break;
    case LockManager::LockMode::INTENTION_SHARED:
      return b != LockManager::LockMode::EXCLUSIVE;
      break;
    case LockManager::LockMode::INTENTION_EXCLUSIVE:
      return b == LockManager::LockMode::INTENTION_SHARED || b == LockManager::LockMode::INTENTION_EXCLUSIVE;
      break;
    case LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE:
      return b == LockManager::LockMode::INTENTION_SHARED;
      break;
  }
  return true;
}

void ModifyTableLockForTxn(Transaction *txn, LockManager::LockMode lock_mode, const table_oid_t oid, int action) {
  std::shared_ptr<std::unordered_set<table_oid_t>> lock_set;
  if (lock_mode == LockManager::LockMode::SHARED) {
    lock_set = txn->GetSharedTableLockSet();
  } else if (lock_mode == LockManager::LockMode::EXCLUSIVE) {
    lock_set = txn->GetExclusiveTableLockSet();
  } else if (lock_mode == LockManager::LockMode::INTENTION_SHARED) {
    lock_set = txn->GetIntentionSharedTableLockSet();
  } else if (lock_mode == LockManager::LockMode::INTENTION_EXCLUSIVE) {
    lock_set = txn->GetIntentionExclusiveTableLockSet();
  } else if (lock_mode == LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE) {
    lock_set = txn->GetSharedIntentionExclusiveTableLockSet();
  }

  if (action == ADD_TABLE_LOCK) {
    lock_set->insert(oid);
  }
  if (action == DEL_TABLE_LOCK) {
    lock_set->erase(oid);
  }
}

void ModifyRowLockForTxn(Transaction *txn, LockManager::LockMode lock_mode, const table_oid_t oid, RID rid,
                         int action) {
  std::shared_ptr<std::unordered_set<RID>> lock_set;
  std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> row_lock_set;

  if (lock_mode == LockManager::LockMode::SHARED) {
    lock_set = txn->GetSharedLockSet();
    row_lock_set = txn->GetSharedRowLockSet();
  } else if (lock_mode == LockManager::LockMode::EXCLUSIVE) {
    lock_set = txn->GetExclusiveLockSet();
    row_lock_set = txn->GetExclusiveRowLockSet();
  }

  std::unordered_set<RID> &rows = (*row_lock_set)[oid];

  if (action == ADD_ROW_LOCK) {
    lock_set->insert(rid);
    rows.insert(rid);
  } else if (action == DEL_ROW_LOCK) {
    lock_set->erase(rid);
    rows.erase(rid);
  }
}

void IsolationCheck(Transaction *txn, LockManager::LockMode lock_mode) {
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::READ_UNCOMMITTED:
      if (lock_mode == LockManager::LockMode::SHARED || lock_mode == LockManager::LockMode::INTENTION_SHARED ||
          lock_mode == LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }

      if (txn->GetState() == TransactionState::SHRINKING) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::READ_COMMITTED:
      if (txn->GetState() == TransactionState::SHRINKING) {
        if (lock_mode != LockManager::LockMode::INTENTION_SHARED && lock_mode != LockManager::LockMode::SHARED) {
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        }
      }
      break;
    case IsolationLevel::REPEATABLE_READ:
      if (txn->GetState() == TransactionState::SHRINKING) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
      break;
  }
}

void UpgradeCheck(Transaction *txn, LockManager::LockMode old_mode, LockManager::LockMode lock_mode) {
  switch (old_mode) {
    case LockManager::LockMode::SHARED:
      if (lock_mode != LockManager::LockMode::EXCLUSIVE &&
          lock_mode != LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      break;
    case LockManager::LockMode::EXCLUSIVE:
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      break;
    case LockManager::LockMode::INTENTION_SHARED:
      if (lock_mode != LockManager::LockMode::SHARED && lock_mode != LockManager::LockMode::EXCLUSIVE &&
          lock_mode != LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      break;
    case LockManager::LockMode::INTENTION_EXCLUSIVE:
      if (lock_mode != LockManager::LockMode::EXCLUSIVE &&
          lock_mode != LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      break;
    case LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE:
      if (lock_mode != LockManager::LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      break;
  }
}

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  IsolationCheck(txn, lock_mode);

  std::unique_lock<std::mutex> lk(this->table_lock_map_latch_);
  LockRequestQueue &queue = table_lock_map_[oid];
  lk.unlock();

  bool upgrade = false;
  LockRequest *old_request;
  {
    std::unique_lock<std::mutex> read_latch(queue.latch_);
    for (LockRequest *request : queue.request_queue_) {
      upgrade = txn->GetTransactionId() == request->txn_id_;
      if (upgrade) {
        old_request = request;
        break;
      }
    }
  }

  // new request
  if (!upgrade) {
    std::unique_lock<std::mutex> write_latch(queue.latch_);
    auto *request = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
    queue.request_queue_.push_back(request);

    queue.cv_.wait(write_latch, [request, &queue, txn] {
      if (txn->GetState() == TransactionState::ABORTED) {
        return true;
      }

      if (queue.upgrading_ != INVALID_TXN_ID) {
        return false;
      }

      for (auto &it : queue.request_queue_) {
        if (request->txn_id_ == it->txn_id_) {
          break;
        }

        if (!LockCompatible(it->lock_mode_, request->lock_mode_)) {
          return false;
        }
      }
      return true;
    });
    if (txn->GetState() == TransactionState::ABORTED) {
      queue.request_queue_.remove(request);
      delete request;
      queue.cv_.notify_all();
      return false;
    }

    request->granted_ = true;
    ModifyTableLockForTxn(txn, lock_mode, oid, ADD_TABLE_LOCK);
    queue.cv_.notify_all();

    return true;
  }

  // maybe need upgrade
  if (old_request->lock_mode_ == lock_mode) {
    return true;
  }
  UpgradeCheck(txn, old_request->lock_mode_, lock_mode);

  std::unique_lock<std::mutex> write_latch(queue.latch_);
  if (queue.upgrading_ != INVALID_TXN_ID) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }

  queue.upgrading_ = txn->GetTransactionId();
  queue.cv_.wait(write_latch, [&queue, lock_mode, old_request, txn] {
    if (txn->GetState() == TransactionState::ABORTED) {
      return true;
    }
    for (auto &it : queue.request_queue_) {
      if (old_request->txn_id_ == it->txn_id_) {
        continue;
      }

      if (it->granted_ && !LockCompatible(lock_mode, it->lock_mode_)) {
        return false;
      }
    }

    return true;
  });
  if (txn->GetState() == TransactionState::ABORTED) {
    queue.cv_.notify_all();
    return false;
  }

  ModifyTableLockForTxn(txn, old_request->lock_mode_, oid, DEL_TABLE_LOCK);
  old_request->lock_mode_ = lock_mode;
  ModifyTableLockForTxn(txn, lock_mode, oid, ADD_TABLE_LOCK);
  queue.upgrading_ = INVALID_TXN_ID;
  queue.cv_.notify_all();

  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  std::unique_lock<std::mutex> write_lock(table_lock_map_latch_);
  LockRequestQueue &queue = table_lock_map_[oid];
  write_lock.unlock();

  std::unique_lock<std::mutex> lk(queue.latch_);
  LockRequest *request = nullptr;
  for (auto &it : queue.request_queue_) {
    if (it->txn_id_ == txn->GetTransactionId()) {
      request = it;
      break;
    }
  }

  if (request == nullptr) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  auto shared_lock_set = txn->GetSharedRowLockSet();
  if (!(*shared_lock_set)[oid].empty()) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }
  auto ex_lock_set = txn->GetExclusiveRowLockSet();
  if (!(*ex_lock_set)[oid].empty()) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }

  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::READ_UNCOMMITTED:
      if (txn->GetState() == TransactionState::GROWING && request->lock_mode_ == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      }
      break;
    case IsolationLevel::REPEATABLE_READ:
      if (txn->GetState() == TransactionState::GROWING &&
          (request->lock_mode_ == LockMode::EXCLUSIVE || request->lock_mode_ == LockMode::SHARED)) {
        txn->SetState(TransactionState::SHRINKING);
      }
      break;
    case IsolationLevel::READ_COMMITTED:
      if (txn->GetState() == TransactionState::GROWING && request->lock_mode_ == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      }
      break;
  }

  request->granted_ = false;
  queue.request_queue_.remove(request);
  ModifyTableLockForTxn(txn, request->lock_mode_, request->oid_, DEL_TABLE_LOCK);
  delete request;
  queue.cv_.notify_all();

  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  if (lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::INTENTION_EXCLUSIVE ||
      lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }

  IsolationCheck(txn, lock_mode);

  std::unique_lock<std::mutex> lk(row_lock_map_latch_);
  LockRequestQueue &queue = row_lock_map_[rid];
  lk.unlock();

  bool upgrade = false;
  LockRequest *old_request;
  {
    std::unique_lock<std::mutex> read_latch(queue.latch_);
    for (LockRequest *request : queue.request_queue_) {
      upgrade = txn->GetTransactionId() == request->txn_id_;
      if (upgrade) {
        old_request = request;
        break;
      }
    }
  }

  // new request
  if (!upgrade) {
    std::unique_lock<std::mutex> write_latch(queue.latch_);
    auto *request = new LockRequest(txn->GetTransactionId(), lock_mode, oid, rid);
    queue.request_queue_.push_back(request);

    queue.cv_.wait(write_latch, [request, &queue, txn] {
      if (txn->GetState() == TransactionState::ABORTED) {
        return true;
      }

      if (queue.upgrading_ != INVALID_TXN_ID) {
        return false;
      }

      for (auto &it : queue.request_queue_) {
        if (request->txn_id_ == it->txn_id_) {
          break;
        }

        if (!LockCompatible(it->lock_mode_, request->lock_mode_)) {
          return false;
        }
      }
      return true;
    });
    if (txn->GetState() == TransactionState::ABORTED) {
      queue.request_queue_.remove(request);
      delete request;
      queue.cv_.notify_all();
      return false;
    }

    request->granted_ = true;
    ModifyRowLockForTxn(txn, lock_mode, oid, rid, ADD_TABLE_LOCK);
    queue.cv_.notify_all();

    return true;
  }

  // maybe need upgrade
  if (old_request->lock_mode_ == lock_mode) {
    return true;
  }

  std::unique_lock<std::mutex> write_latch(queue.latch_);
  if (queue.upgrading_ != INVALID_TXN_ID) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }

  queue.upgrading_ = txn->GetTransactionId();
  queue.cv_.wait(write_latch, [&queue, lock_mode, old_request, txn] {
    if (txn->GetState() == TransactionState::ABORTED) {
      return true;
    }
    for (auto &it : queue.request_queue_) {
      if (old_request->txn_id_ == it->txn_id_) {
        continue;
      }

      if (it->granted_ && !LockCompatible(lock_mode, it->lock_mode_)) {
        return false;
      }
    }

    return true;
  });
  if (txn->GetState() == TransactionState::ABORTED) {
    queue.cv_.notify_all();
    return false;
  }

  ModifyRowLockForTxn(txn, old_request->lock_mode_, oid, rid, DEL_TABLE_LOCK);
  old_request->lock_mode_ = lock_mode;
  ModifyRowLockForTxn(txn, lock_mode, oid, rid, ADD_TABLE_LOCK);
  queue.upgrading_ = INVALID_TXN_ID;
  queue.cv_.notify_all();

  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  std::unique_lock<std::mutex> write_lock(row_lock_map_latch_);
  LockRequestQueue &queue = row_lock_map_[rid];
  write_lock.unlock();

  std::unique_lock<std::mutex> lk(queue.latch_);
  LockRequest *request = nullptr;
  for (auto &it : queue.request_queue_) {
    if (it->txn_id_ == txn->GetTransactionId()) {
      request = it;
      break;
    }
  }

  if (request == nullptr) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::READ_UNCOMMITTED:
      if (txn->GetState() == TransactionState::GROWING && request->lock_mode_ == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      }
      break;
    case IsolationLevel::REPEATABLE_READ:
      if (txn->GetState() == TransactionState::GROWING &&
          (request->lock_mode_ == LockMode::EXCLUSIVE || request->lock_mode_ == LockMode::SHARED)) {
        txn->SetState(TransactionState::SHRINKING);
      }
      break;
    case IsolationLevel::READ_COMMITTED:
      if (txn->GetState() == TransactionState::GROWING && request->lock_mode_ == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      }
      break;
  }

  request->granted_ = false;
  queue.request_queue_.remove(request);
  ModifyRowLockForTxn(txn, request->lock_mode_, request->oid_, rid, DEL_TABLE_LOCK);
  delete request;
  queue.cv_.notify_all();

  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  std::lock_guard<std::mutex> lk(waits_for_latch_);
  auto &points = waits_for_[t1];
  points.push_back(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  std::lock_guard<std::mutex> lk(waits_for_latch_);
  auto &points = waits_for_[t1];
  auto position = std::find(points.begin(), points.end(), t2);
  if (position != points.end()) {
    points.erase(position);
  }
}

auto LockManager::CheckCycle(txn_id_t current, std::unordered_map<txn_id_t, int> &state,
                             std::vector<txn_id_t> &previous) -> txn_id_t {
  state[current] = 1;
  previous.push_back(current);
  auto &points = waits_for_[current];
  for (auto next : points) {
    if (state.count(next) == 0) {
      auto result = CheckCycle(next, state, previous);

      if (result != INVALID_TXN_ID) {
        return result;
      }
    }

    if (state[next] == 1) {
      txn_id_t r = -1;
      for (auto it = previous.rbegin(); it != previous.rend(); ++it) {
        r = std::max(r, *it);

        if (*it == next) {
          break;
        }
      }

      return r;
    }
  }
  state[current] = 2;
  previous.pop_back();

  return INVALID_TXN_ID;
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  txn_id_t max_txn_id;
  std::unordered_map<txn_id_t, int> state;
  std::vector<txn_id_t> previous;
  std::set<txn_id_t> start_points;

  waits_for_.clear();
  for (auto &item : table_lock_map_) {
    LockRequestQueue &queue = item.second;
    for (auto &a : queue.request_queue_) {
      if (!a->granted_) {
        continue;
      }

      if (TransactionManager::GetTransaction(a->txn_id_)->GetState() == TransactionState::ABORTED) {
        continue;
      }

      for (auto &b : queue.request_queue_) {
        if (a->txn_id_ == b->txn_id_) {
          continue;
        }

        if (TransactionManager::GetTransaction(b->txn_id_)->GetState() == TransactionState::ABORTED) {
          continue;
        }

        if (b->granted_) {
          continue;
        }

        if (!LockCompatible(a->lock_mode_, b->lock_mode_)) {
          auto &txns = waits_for_[b->txn_id_];
          txns.push_back(a->txn_id_);
        }
      }
    }
  }

  for (auto &item : row_lock_map_) {
    LockRequestQueue &queue = item.second;
    for (auto &a : queue.request_queue_) {
      if (!a->granted_) {
        continue;
      }

      if (TransactionManager::GetTransaction(a->txn_id_)->GetState() == TransactionState::ABORTED) {
        continue;
      }

      for (auto &b : queue.request_queue_) {
        if (a->txn_id_ == b->txn_id_) {
          continue;
        }

        if (TransactionManager::GetTransaction(b->txn_id_)->GetState() == TransactionState::ABORTED) {
          continue;
        }

        if (b->granted_) {
          continue;
        }

        if (!LockCompatible(a->lock_mode_, b->lock_mode_)) {
          auto &txns = waits_for_[b->txn_id_];
          txns.push_back(a->txn_id_);
        }
      }
    }
  }

  for (auto &item : waits_for_) {
    auto &points = item.second;
    std::sort(points.begin(), points.end());
    start_points.insert(item.first);
  }

  for (auto current : start_points) {
    max_txn_id = CheckCycle(current, state, previous);

    if (max_txn_id != INVALID_TXN_ID) {
      *txn_id = max_txn_id;
      return true;
    }
  }

  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);

  for (auto &item : waits_for_) {
    for (auto point : item.second) {
      edges.push_back(std::make_pair(item.first, point));
    }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
      std::lock_guard<std::mutex> lk_graph(waits_for_latch_);
      std::lock_guard<std::mutex> lk_table(table_lock_map_latch_);
      std::lock_guard<std::mutex> lk_row(row_lock_map_latch_);

      txn_id_t r;
      while (HasCycle(&r)) {
        TransactionManager::GetTransaction(r)->SetState(TransactionState::ABORTED);
        for (auto &item : table_lock_map_) {
          item.second.cv_.notify_all();
        }
        for (auto &item : row_lock_map_) {
          item.second.cv_.notify_all();
        }
      }
    }
  }
}

}  // namespace bustub
