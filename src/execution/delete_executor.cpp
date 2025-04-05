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
  TupleMeta delete_meta{0, true};

  // Process each tuple from the child executor
  while (child_executor_->Next(&delete_tuple, &delete_rid)) {
    // Mark the old tuple as deleted
    table_info_->table_->UpdateTupleMeta(delete_meta, delete_rid);

    // Delete old tuple from all indexes
    for (auto &index_info : index_infos_) {
      auto delete_key =
          delete_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(delete_key, delete_rid, exec_ctx_->GetTransaction());
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
