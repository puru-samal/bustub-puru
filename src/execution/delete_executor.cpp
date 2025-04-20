//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/delete_executor.h"
#include <memory>
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"

namespace bustub {

/**
 * Construct a new DeleteExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The delete plan to be executed
 * @param child_executor The child executor that feeds the delete
 */
DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
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
/** Initialize the delete */
void DeleteExecutor::Init() { child_executor_->Init(); }

/**
 * Yield the number of rows deleted from the table.
 * @param[out] tuple The integer tuple indicating the number of rows deleted from the table
 * @param[out] rid The next tuple RID produced by the delete (ignore, not used)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: DeleteExecutor::Next() does not use the `rid` out-parameter.
 * NOTE: DeleteExecutor::Next() returns true with the number of deleted rows produced only once.
 */
auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_deleted_ || table_info_ == nullptr) {
    return false;
  }

  int32_t deleted_rows = 0;
  Tuple delete_tuple;
  RID delete_rid;
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

  // Process each tuple from the child executor
  while (child_executor_->Next(&delete_tuple, &delete_rid)) {
    // Get current tuple metadata
    auto [table_meta, table_tuple, table_undo_link] =
        GetTupleAndUndoLink(txn_mgr, table_info_->table_.get(), delete_rid);

    // Check for write-write conflicts
    if (table_meta.ts_ > txn->GetReadTs() && table_meta.ts_ != txn->GetTransactionTempTs()) {
      txn->SetTainted();
      throw ExecutionException("Write-write conflict detected in delete");
    }

    std::optional<UndoLink> new_undo_link = std::nullopt;

    if (table_meta.ts_ != txn->GetTransactionTempTs()) {
      // Not self modification - create new undo log
      // If table_undo_link is not null, use it as the previous undo link
      // Create a new undo log and append it to the transaction
      // Link the new undo link to the table heap later
      auto prev_undo_link = table_undo_link.has_value() ? table_undo_link.value() : UndoLink{};
      auto undo_log = GenerateNewUndoLog(&table_info_->schema_, &table_tuple, nullptr, table_meta.ts_, prev_undo_link);
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
        UpdateTupleAndUndoLink(txn_mgr, delete_rid, new_undo_link, table_info_->table_.get(), txn,
                               TupleMeta{txn->GetTransactionTempTs(), true}, table_tuple, check_conflict);

    if (!delete_succeeded) {
      throw ExecutionException("Write-write conflict detected in delete");
    }

    if (table_meta.ts_ != txn->GetTransactionTempTs()) {
      // Add to write set
      txn->AppendWriteSet(table_info_->oid_, delete_rid);
    }
    deleted_rows++;
  }

  // Yield the number of rows updated
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(TypeId::INTEGER, deleted_rows);
  *tuple = Tuple(values, &GetOutputSchema());
  is_deleted_ = true;
  return true;
}

}  // namespace bustub
