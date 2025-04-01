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
                                                      : BPlusTreeIndexIteratorForTwoIntegerColumn()),
      is_scanned_(false) {}

void IndexScanExecutor::Init() {
  // Point lookup
  if (plan_->filter_predicate_ != nullptr) {
    for (const auto &key : plan_->pred_keys_) {
      auto constant_expr = dynamic_cast<ConstantValueExpression *>(key.get());
      Tuple tuple(std::vector<Value>{constant_expr->val_}, index_info_->index_->GetKeySchema());
      std::vector<RID> tmp_result;
      tree_->ScanKey(tuple, &tmp_result, exec_ctx_->GetTransaction());
      result_.insert(result_.end(), tmp_result.begin(), tmp_result.end());
    }
    result_iter_ = result_.begin();
  }
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_scanned_) {
    return false;
  }

  // Point lookup
  if (plan_->filter_predicate_ != nullptr) {
    while (result_iter_ != result_.end()) {
      auto [meta, tup] = table_info_->table_->GetTuple(*result_iter_);
      *tuple = tup;
      *rid = *result_iter_;
      ++result_iter_;
      // Skip deleted tuples
      if (meta.is_deleted_) {
        continue;
      }
      return true;
    }
    is_scanned_ = true;
    return false;
  }

  // Ordered scan
  while (index_iter_ != tree_->GetEndIterator()) {
    *rid = (*index_iter_).second;
    auto [meta, tup] = table_info_->table_->GetTuple(*rid);
    *tuple = tup;
    ++index_iter_;
    // Skip deleted tuples
    if (meta.is_deleted_) {
      continue;
    }
    return true;
  }
  is_scanned_ = true;
  return false;
}

}  // namespace bustub
