//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.h
//
// Identification: src/include/execution/executors/index_scan_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include "common/rid.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/index_scan_plan.h"
#include "storage/index/b_plus_tree_index.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * IndexScanExecutor executes an index scan over a table.
 */

class IndexScanExecutor : public AbstractExecutor {
 public:
  IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan);

  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  void Init() override;

  auto Next(Tuple *tuple, RID *rid) -> bool override;

 private:
  /** The index scan plan node to be executed. */
  const IndexScanPlanNode *plan_;

  /** The index info from the catalog. */
  const IndexInfo *index_info_;

  /** Metadata identifying the table that should be scanned */
  const TableInfo *table_info_;

  /** The tree for the index. */
  BPlusTreeIndexForTwoIntegerColumn *tree_;

  /** The iterator for the index. */
  BPlusTreeIndexIteratorForTwoIntegerColumn index_iter_;

  /** The result of the index scan for the filter predicate. */
  std::vector<RID> result_;

  /** The iterator for the result of the index scan for the filter predicate. */
  std::vector<RID>::iterator result_iter_;
};
}  // namespace bustub
