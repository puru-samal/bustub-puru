//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
namespace bustub {

/**
 * Construct a new SeqScanExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The sequential scan plan to be executed
 */
SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan_->GetTableOid()).get()) {}

/** Initialize the sequential scan */
void SeqScanExecutor::Init() {
  // Create an iterator for the table
  if (table_info_ != nullptr) {
    table_iter_.emplace(table_info_->table_->MakeIterator());
  }
}

/**
 * Yield the next tuple from the sequential scan.
 * @param[out] tuple The next tuple produced by the scan
 * @param[out] rid The next tuple RID produced by the scan
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!table_iter_.has_value() || table_info_ == nullptr) {
    return false;
  }

  auto txn_mgr = exec_ctx_->GetTransactionManager();
  auto txn = exec_ctx_->GetTransaction();

  while (!table_iter_->IsEnd()) {
    RID curr_rid = table_iter_->GetRID();

    ++(table_iter_.value());

    auto [base_meta, base_tuple, undo_link] = GetTupleAndUndoLink(txn_mgr, table_info_->table_.get(), curr_rid);

    // Collect undo logs to reconstruct the tuple at our read timestamp
    auto undo_logs = CollectUndoLogs(curr_rid, base_meta, base_tuple, undo_link, txn, txn_mgr);

    // If no valid version exists at our timestamp, skip this tuple
    if (!undo_logs.has_value()) {
      continue;
    }

    // Reconstruct the tuple using collected undo logs
    auto reconstructed_tuple = ReconstructTuple(&table_info_->schema_, base_tuple, base_meta, undo_logs.value());

    // Skip if tuple was deleted at our timestamp
    if (!reconstructed_tuple.has_value()) {
      continue;
    }

    // Update output parameters with reconstructed tuple
    *tuple = reconstructed_tuple.value();
    *rid = curr_rid;

    // Skip tuples that don't satisfy the predicate if it exists
    if (plan_->filter_predicate_) {
      auto eval_result = plan_->filter_predicate_->Evaluate(tuple, table_info_->schema_);
      if (eval_result.GetTypeId() == TypeId::BOOLEAN && !eval_result.GetAs<bool>()) {
        continue;
      }
    }
    return true;
  }
  return false;
}
}  // namespace bustub
