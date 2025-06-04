//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/update_executor.h"
#include <cstdio>
#include <memory>
#include <vector>
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"

namespace bustub {

/**
 * Construct a new UpdateExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The update plan to be executed
 * @param child_executor The child executor that feeds the update
 */
UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan_->GetTableOid()).get()),
      child_executor_(std::move(child_executor)) {
  // Get the index infos
  if (table_info_ != nullptr) {
    index_infos_ = exec_ctx->GetCatalog()->GetTableIndexes(table_info_->name_);
  }
}

/** Initialize the update */
void UpdateExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    update_tuples_.emplace_back(tuple, rid);
  }
  update_iter_ = update_tuples_.begin();
  is_updated_ = false;
}

/**
 * Yield the next tuple from the update.
 * @param[out] tuple The next tuple produced by the update
 * @param[out] rid The next tuple RID produced by the update (ignore this)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: UpdateExecutor::Next() does not use the `rid` out-parameter.
 */
auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_updated_ || table_info_ == nullptr) {
    return false;
  }

  auto txn = exec_ctx_->GetTransaction();
  auto txn_mgr = exec_ctx_->GetTransactionManager();

  // Check for write-write conflict
  auto check_conflict = [txn](const TupleMeta &meta, const Tuple &tuple, RID rid, std::optional<UndoLink> undo_link) {
    if (meta.ts_ > txn->GetReadTs() && meta.ts_ != txn->GetTransactionTempTs()) {
      txn->SetTainted();
      return false;
    }
    return true;
  };

  // Find primary key index if it exists
  const IndexInfo *pk_index = nullptr;
  for (auto &index_info : index_infos_) {
    if (index_info->is_primary_key_) {
      pk_index = index_info.get();
      break;
    }
  }

  // First pass: Generate all updated tuples and check for PK changes
  std::vector<std::tuple<RID, Tuple, Tuple>> updates;  // (rid, old_tuple, new_tuple)
  bool has_pk_updates = false;
  while (update_iter_ != update_tuples_.end()) {
    auto [child_tuple, child_rid] = *update_iter_;
    update_iter_++;

    auto [table_meta, table_tuple, table_undo_link] =
        GetTupleAndUndoLink(txn_mgr, table_info_->table_.get(), child_rid);

    // Generate updated tuple values
    std::vector<Value> values;
    values.reserve(table_info_->schema_.GetColumnCount());
    for (const auto &target_expr : plan_->target_expressions_) {
      values.push_back(target_expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
    }
    Tuple new_tuple{values, &table_info_->schema_};

    // Check if PK is being updated
    if (pk_index != nullptr) {
      auto old_key =
          table_tuple.KeyFromTuple(table_info_->schema_, pk_index->key_schema_, pk_index->index_->GetKeyAttrs());
      auto new_key =
          new_tuple.KeyFromTuple(table_info_->schema_, pk_index->key_schema_, pk_index->index_->GetKeyAttrs());
      if (!old_key.GetValue(&pk_index->key_schema_, 0)
               .CompareExactlyEquals(new_key.GetValue(&pk_index->key_schema_, 0))) {
        has_pk_updates = true;
      }
    }
    updates.emplace_back(child_rid, table_tuple, new_tuple);
  }
  if (has_pk_updates) {
    // First delete all the old tuples
    std::unordered_set<RID> append_write_set;
    for (auto &[table_rid, old_tuple, new_tuple] : updates) {
      auto [table_meta, table_tuple, table_undo_link] =
          GetTupleAndUndoLink(txn_mgr, table_info_->table_.get(), table_rid);

      // Check for write-write conflicts
      if (table_meta.ts_ > txn->GetReadTs() && table_meta.ts_ != txn->GetTransactionTempTs()) {
        txn->SetTainted();
        throw ExecutionException("Write-write conflict detected in update");
      }

      std::optional<UndoLink> new_undo_link = std::nullopt;
      if (table_meta.ts_ != txn->GetTransactionTempTs()) {
        // Not self modification - create new undo log
        // If table_undo_link is not null, use it as the previous undo link
        // Create a new undo log and append it to the transaction
        // Link the new undo link to the table heap later
        auto prev_undo_link = table_undo_link.has_value() ? table_undo_link.value() : UndoLink{};
        auto undo_log =
            GenerateNewUndoLog(&table_info_->schema_, &table_tuple, nullptr, table_meta.ts_, prev_undo_link);
        new_undo_link = txn->AppendUndoLog(undo_log);
      } else {
        // Self modification - update the undo log in the current transaction
        // if one exists
        if (table_undo_link.has_value()) {
          BUSTUB_ASSERT(table_undo_link.value().prev_txn_ == txn->GetTransactionId(),
                        "Undo link is not from the current transaction");
          auto log_idx = table_undo_link.value().prev_log_idx_;
          auto prev_log = txn_mgr->GetUndoLog(table_undo_link.value());
          auto undo_log = GenerateUpdatedUndoLog(&table_info_->schema_, &table_tuple, nullptr, prev_log);
          txn->ModifyUndoLog(log_idx, undo_log);
          new_undo_link = UndoLink{txn->GetTransactionId(), log_idx};
        }
      }

      // Delete the tuple in the table heap and the undo link
      auto delete_succeeded =
          UpdateTupleAndUndoLink(txn_mgr, table_rid, new_undo_link, table_info_->table_.get(), txn,
                                 TupleMeta{txn->GetTransactionTempTs(), true}, table_tuple, check_conflict);

      if (!delete_succeeded) {
        throw ExecutionException("Write-write conflict detected in delete");
      }
      append_write_set.insert(table_rid);
    }
    // Second pass: Insert new tuples and check for PK constraints
    for (auto &[table_rid, old_tuple, new_tuple] : updates) {
      std::optional<RID> deleted_rid = std::nullopt;
      BUSTUB_ASSERT(pk_index != nullptr, "Primary key index is not found");
      auto key = new_tuple.KeyFromTuple(table_info_->schema_, pk_index->key_schema_, pk_index->index_->GetKeyAttrs());
      std::vector<RID> result;
      pk_index->index_->ScanKey(key, &result, txn);
      if (!result.empty()) {
        auto pk_meta = table_info_->table_->GetTupleMeta(result[0]);
        if (!pk_meta.is_deleted_) {
          txn->SetTainted();
          throw ExecutionException("Unique key constraint violation in primary key index");
        }
        // Found reusable deleted tuple
        deleted_rid = result[0];
      }
      // If there's a reusable deleted tuple, insert is just an update of the deleted tuple
      if (deleted_rid.has_value()) {
        auto [table_meta, table_tuple, table_undo_link] =
            GetTupleAndUndoLink(txn_mgr, table_info_->table_.get(), deleted_rid.value());

        BUSTUB_ASSERT(table_meta.is_deleted_, "Table meta is not deleted");

        std::optional<UndoLink> new_undo_link = std::nullopt;
        if (table_meta.ts_ != txn->GetTransactionTempTs()) {
          // Not self modification - create new undo log
          // If table_undo_link is not null, use it as the previous undo link
          // Create a new undo log and append it to the transaction
          // Link the new undo link to the table heap later
          auto prev_undo_link = table_undo_link.has_value() ? table_undo_link.value() : UndoLink{};
          auto undo_log =
              GenerateNewUndoLog(&table_info_->schema_, nullptr, &new_tuple, table_meta.ts_, prev_undo_link);
          new_undo_link = txn->AppendUndoLog(undo_log);
        } else {
          // Self modification - update the undo log in the current transaction
          // if one exists
          if (table_undo_link.has_value()) {
            BUSTUB_ASSERT(table_undo_link.value().prev_txn_ == txn->GetTransactionId(),
                          "Undo link is not from the current transaction");
            auto log_idx = table_undo_link.value().prev_log_idx_;
            auto prev_log = txn_mgr->GetUndoLog(table_undo_link.value());
            auto undo_log = GenerateUpdatedUndoLog(&table_info_->schema_, nullptr, &new_tuple, prev_log);
            txn->ModifyUndoLog(log_idx, undo_log);
            new_undo_link = UndoLink{txn->GetTransactionId(), log_idx};
          }
        }

        // Update the tuple in the table heap and the undo link
        auto update_succeeded =
            UpdateTupleAndUndoLink(txn_mgr, deleted_rid.value(), new_undo_link, table_info_->table_.get(), txn,
                                   TupleMeta{txn->GetTransactionTempTs(), false}, new_tuple, check_conflict);

        if (!update_succeeded) {
          throw ExecutionException("Write-write conflict detected in update");
        }
        *tuple = new_tuple;
        *rid = deleted_rid.value();
        if (append_write_set.find(deleted_rid.value()) == append_write_set.end()) {
          txn->AppendWriteSet(table_info_->oid_, deleted_rid.value());
        }
      } else {
        // Insert a new tuple and add key
        // Set tuple metadata with transaction's temporary timestamp
        TupleMeta insert_meta{txn->GetTransactionTempTs(), false};

        // Insert the an empty tuple into the table
        auto insert_rid = table_info_->table_->InsertTuple(insert_meta, new_tuple, exec_ctx_->GetLockManager(), txn,
                                                           table_info_->oid_);

        // If the tuple is inserted successfully, update the tuple, meta and undo link, and the indexes
        if (insert_rid.has_value()) {
          // Try to insert into primary key index first if it exists
          BUSTUB_ASSERT(pk_index != nullptr, "Primary key index is not found");

          auto new_key =
              new_tuple.KeyFromTuple(table_info_->schema_, pk_index->key_schema_, pk_index->index_->GetKeyAttrs());
          if (!pk_index->index_->InsertEntry(new_key, insert_rid.value(), txn)) {
            // Another transaction inserted the same key between our check and insert
            txn->SetTainted();
            throw ExecutionException("Concurrent insert detected in primary key index");
          }

          // P4: Update the tuple, meta and undo link
          // Add RID to transaction's write set
          UpdateTupleAndUndoLink(txn_mgr, insert_rid.value(), std::nullopt, table_info_->table_.get(), txn, insert_meta,
                                 new_tuple, check_conflict);
          if (append_write_set.find(insert_rid.value()) == append_write_set.end()) {
            txn->AppendWriteSet(table_info_->oid_, insert_rid.value());
          }
        }
        *tuple = new_tuple;
        *rid = insert_rid.value();
      }
      for (auto rid : append_write_set) {
        txn->AppendWriteSet(table_info_->oid_, rid);
      }
    }
  } else {
    // Perform actual updates
    for (auto &[table_rid, old_tuple, new_tuple] : updates) {
      // Get the tuple metadata, tuple, and undo link from the table heap
      auto [table_meta, table_tuple, table_undo_link] =
          GetTupleAndUndoLink(txn_mgr, table_info_->table_.get(), table_rid);

      // Check for write-write conflicts
      if (table_meta.ts_ > txn->GetReadTs() && table_meta.ts_ != txn->GetTransactionTempTs()) {
        txn->SetTainted();
        throw ExecutionException("Write-write conflict detected in update");
      }

      std::optional<UndoLink> new_undo_link = std::nullopt;
      if (table_meta.ts_ != txn->GetTransactionTempTs()) {
        // Not self modification - create new undo log
        // If table_undo_link is not null, use it as the previous undo link
        // Create a new undo log and append it to the transaction
        // Link the new undo link to the table heap later
        auto prev_undo_link = table_undo_link.has_value() ? table_undo_link.value() : UndoLink{};
        auto undo_log =
            GenerateNewUndoLog(&table_info_->schema_, &table_tuple, &new_tuple, table_meta.ts_, prev_undo_link);
        new_undo_link = txn->AppendUndoLog(undo_log);
      } else {
        // Self modification - update the undo log in the current transaction
        // if one exists
        if (table_undo_link.has_value()) {
          BUSTUB_ASSERT(table_undo_link.value().prev_txn_ == txn->GetTransactionId(),
                        "Undo link is not from the current transaction");
          auto log_idx = table_undo_link.value().prev_log_idx_;
          auto prev_log = txn_mgr->GetUndoLog(table_undo_link.value());
          auto undo_log = GenerateUpdatedUndoLog(&table_info_->schema_, &table_tuple, &new_tuple, prev_log);
          txn->ModifyUndoLog(log_idx, undo_log);
          new_undo_link = UndoLink{txn->GetTransactionId(), log_idx};
        }
      }
      // Update the tuple in the table heap and the undo link
      auto update_succeeded =
          UpdateTupleAndUndoLink(txn_mgr, table_rid, new_undo_link, table_info_->table_.get(), txn,
                                 TupleMeta{txn->GetTransactionTempTs(), false}, new_tuple, check_conflict);

      if (!update_succeeded) {
        throw ExecutionException("Write-write conflict detected in update");
      }

      if (table_meta.ts_ != txn->GetTransactionTempTs()) {
        // Add to write set
        txn->AppendWriteSet(table_info_->oid_, table_rid);
      }

      // Update all indexes with new tuple if update succeeded
      for (auto &index_info : index_infos_) {
        auto new_key =
            new_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        index_info->index_->InsertEntry(new_key, table_rid, exec_ctx_->GetTransaction());
      }
      *tuple = new_tuple;
      *rid = table_rid;
    }
  }
  is_updated_ = true;
  return true;
}

}  // namespace bustub
