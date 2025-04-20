//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/index_scan_executor.h"
#include <cstddef>
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/expressions/constant_value_expression.h"
#include "storage/index/b_plus_tree_index.h"

namespace bustub {

/**
 * Creates a new index scan executor.
 * @param exec_ctx the executor context
 * @param plan the index scan plan to be executed
 */
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      index_info_(exec_ctx->GetCatalog()->GetIndex(plan_->GetIndexOid()).get()),
      table_info_(exec_ctx->GetCatalog()->GetTable(index_info_->table_name_).get()),
      tree_(dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get())),
      index_iter_(plan_->filter_predicate_ == nullptr ? tree_->GetBeginIterator()
                                                      : BPlusTreeIndexIteratorForTwoIntegerColumn()) {}

void IndexScanExecutor::Init() {
  result_.clear();
  // Point lookup
  if (plan_->filter_predicate_ != nullptr) {
    for (const auto &key : plan_->pred_keys_) {
      auto constant_expr = dynamic_cast<ConstantValueExpression *>(key.get());
      Tuple tuple(std::vector<Value>{constant_expr->val_}, index_info_->index_->GetKeySchema());
      std::vector<RID> tmp_result;
      tree_->ScanKey(tuple, &tmp_result, exec_ctx_->GetTransaction());
      result_.insert(result_.end(), tmp_result.begin(), tmp_result.end());
    }
  } else {
    // Ordered scan
    while (index_iter_ != tree_->GetEndIterator()) {
      auto rid = (*index_iter_).second;
      result_.push_back(rid);
      ++index_iter_;
    }
  }
  result_iter_ = result_.begin();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto txn_mgr = exec_ctx_->GetTransactionManager();
  auto txn = exec_ctx_->GetTransaction();

  while (result_iter_ != result_.end()) {
    RID curr_rid = *result_iter_;

    ++result_iter_;

    auto [base_meta, base_tuple, undo_link] = GetTupleAndUndoLink(txn_mgr, table_info_->table_.get(), curr_rid);
    if (base_meta.ts_ <= txn->GetReadTs() || base_meta.ts_ == txn->GetTransactionTempTs()) {
      // Read by current transaction or another uncommitted transaction: Read the tuple in the table heap

      // Skip if tuple was deleted at our timestamp
      if (base_meta.is_deleted_) {
        continue;
      }

      *tuple = base_tuple;
      *rid = curr_rid;

    } else {
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
    }

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
