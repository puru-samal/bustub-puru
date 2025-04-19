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

  while (update_iter_ != update_tuples_.end()) {
    auto [child_tuple, child_rid] = *update_iter_;
    update_iter_++;

    // Get the tuple metadata, tuple, and undo link from the table heap
    auto [table_meta, table_tuple, table_undo_link] =
        GetTupleAndUndoLink(txn_mgr, table_info_->table_.get(), child_rid);

    // Check for write-write conflicts
    if (table_meta.ts_ > txn->GetReadTs() && table_meta.ts_ != txn->GetTransactionTempTs()) {
      txn->SetTainted();
      throw ExecutionException("Write-write conflict detected in update");
    }

    // Delete old tuple from all indexes
    for (auto &index_info : index_infos_) {
      auto old_key =
          child_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(old_key, child_rid, exec_ctx_->GetTransaction());
    }

    // Generate updated tuple values
    std::vector<Value> values;
    values.reserve(table_info_->schema_.GetColumnCount());
    for (const auto &target_expr : plan_->target_expressions_) {
      values.push_back(target_expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
    }

    // Create updated tuple
    *tuple = Tuple{values, &table_info_->schema_};
    *rid = child_rid;

    if (table_meta.ts_ == txn->GetTransactionTempTs()) {
      // Self modification
      // Update the tuple in the table heap
      TupleMeta new_meta{txn->GetTransactionTempTs(), false};
      table_info_->table_->UpdateTupleInPlace(new_meta, *tuple, *rid);
      // Update existing undo log in the current transaction if it exists
      if (table_undo_link.has_value()) {
        auto log_idx = table_undo_link.value().prev_log_idx_;
        auto prev_log = txn_mgr->GetUndoLog(table_undo_link.value());
        auto undo_log = GenerateUpdatedUndoLog(&table_info_->schema_, &table_tuple, tuple, prev_log);
        txn->ModifyUndoLog(log_idx, undo_log);
      }
    } else {
      // Not self modification: Link a new undo log
      auto prev_undo_link = table_undo_link.has_value() ? table_undo_link.value() : UndoLink{};
      auto undo_log = GenerateNewUndoLog(&table_info_->schema_, &table_tuple, tuple, table_meta.ts_, prev_undo_link);
      auto new_undo_link = txn->AppendUndoLog(undo_log);
      UpdateTupleAndUndoLink(txn_mgr, *rid, std::optional<UndoLink>(new_undo_link), table_info_->table_.get(), txn,
                             TupleMeta{txn->GetTransactionTempTs(), false}, *tuple, nullptr);
    }

    // Update all indexes with new tuple if update succeeded
    for (auto &index_info : index_infos_) {
      auto new_key =
          tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(new_key, child_rid, exec_ctx_->GetTransaction());
    }
    // Add to write set
    txn->AppendWriteSet(table_info_->oid_, *rid);
    return true;
  }
  is_updated_ = true;
  return false;
}

}  // namespace bustub
