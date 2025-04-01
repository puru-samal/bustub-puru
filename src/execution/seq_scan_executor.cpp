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
#include <cstdio>
#include "common/macros.h"

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
  if (table_info_) {
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
  if (!table_iter_.has_value() || !table_info_) {
    return false;
  }

  while (!table_iter_->IsEnd()) {
    auto [meta, curr_tuple] = table_iter_->GetTuple();
    RID curr_rid = table_iter_->GetRID();

    // Update output parameters
    *tuple = curr_tuple;
    *rid = curr_rid;

    ++(table_iter_.value());

    // Skip deleted tuples
    if (meta.is_deleted_) {
      continue;
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
