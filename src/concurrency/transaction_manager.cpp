//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Begins a new transaction.
 * @param isolation_level an optional isolation level of the transaction.
 * @return an initialized transaction
 */
auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  // TODO(fall2023): set the timestamps here. Watermark updated below.
  txn_ref->read_ts_ = last_commit_ts_.load();
  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

/** @brief Verify if a txn satisfies serializability. We will not test this function and you can change / remove it as
 * you want. */
auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

/**
 * Commits a transaction.
 * @param txn the transaction to commit, the txn will be managed by the txn manager so no need to delete it by
 * yourself
 */
auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  // TODO(fall2023): acquire commit ts!

  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  // Get commit timestamp
  auto commit_ts = last_commit_ts_.load() + 1;

  // Update all modified tuples with commit timestamp
  for (const auto &write_record : txn->write_set_) {
    auto table_oid = write_record.first;
    auto rid_set = write_record.second;
    auto table = catalog_->GetTable(table_oid);
    for (const auto &rid : rid_set) {
      auto tuple_meta = table->table_->GetTupleMeta(rid);
      tuple_meta.ts_ = commit_ts;
      table->table_->UpdateTupleMeta(tuple_meta, rid);
    }
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::COMMITTED;
  txn->commit_ts_ = commit_ts;
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);
  last_commit_ts_.fetch_add(1);

  return true;
}

/**
 * Aborts a transaction
 * @param txn the transaction to abort, the txn will be managed by the txn manager so no need to delete it by yourself
 */
void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  // TODO(fall2023): Implement the abort logic!

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

/** @brief Stop-the-world garbage collection. Will be called only when all transactions are not accessing the table
 * heap. */
void TransactionManager::GarbageCollection() {
  const auto watermark = running_txns_.GetWatermark();

  // Track number of invisible logs per transaction
  std::unordered_map<txn_id_t, size_t> invisible_log_counts;

  // Scan all tables to find invisible undo logs
  for (const auto &table_name : catalog_->GetTableNames()) {
    auto table_info = catalog_->GetTable(table_name);
    auto table_iter = table_info->table_->MakeIterator();

    // Scan all tuples in the table
    while (!table_iter.IsEnd()) {
      auto rid = table_iter.GetRID();
      auto tuple_meta = table_info->table_->GetTupleMeta(rid);

      // If base tuple's timestamp <= watermark, all version chain logs are invisible
      bool is_version_chain_invisible = tuple_meta.ts_ <= watermark;

      // Traverse the version chain
      auto undo_link = GetUndoLink(rid);
      while (undo_link.has_value() && undo_link->IsValid()) {
        auto undo_log = GetUndoLogOptional(undo_link.value());
        if (!undo_log.has_value()) {
          break;
        }

        // Count invisible logs
        if (is_version_chain_invisible) {
          invisible_log_counts[undo_link->prev_txn_]++;
        }

        // If we hit a version with ts <= watermark, remaining chain becomes invisible
        if (undo_log->ts_ <= watermark) {
          is_version_chain_invisible = true;
        }

        undo_link = undo_log->prev_version_;
      }
      ++table_iter;
    }
  }

  // Collect transactions where all logs are invisible and transaction is completed
  std::vector<txn_id_t> txns_to_remove;
  for (const auto &[txn_id, txn] : txn_map_) {
    bool is_completed = txn->GetTransactionState() == TransactionState::COMMITTED ||
                        txn->GetTransactionState() == TransactionState::ABORTED;
    bool all_logs_invisible = invisible_log_counts[txn_id] == txn->GetUndoLogNum();

    if (is_completed && all_logs_invisible) {
      txn->ClearUndoLog();
      txns_to_remove.push_back(txn_id);
    }
  }

  // Remove collected transactions
  for (auto txn_id : txns_to_remove) {
    txn_map_.erase(txn_id);
  }
}
}  // namespace bustub
