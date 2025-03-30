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

#include <memory>
#include "common/macros.h"

#include "execution/executors/update_executor.h"

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
      child_executor_(std::move(child_executor)),
      is_updated_(false) {
  // Get the index infos
  if (table_info_) {
    index_infos_ = exec_ctx->GetCatalog()->GetTableIndexes(table_info_->name_);
  }
}

/** Initialize the update */
void UpdateExecutor::Init() { child_executor_->Init(); }

/**
 * Yield the next tuple from the update.
 * @param[out] tuple The next tuple produced by the update
 * @param[out] rid The next tuple RID produced by the update (ignore this)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: UpdateExecutor::Next() does not use the `rid` out-parameter.
 */
auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_updated_ || !table_info_) {
    return false;
  }

  int32_t updated_rows = 0;
  Tuple old_tuple;
  RID old_rid;

  // Process each tuple from the child executor
  while (child_executor_->Next(&old_tuple, &old_rid)) {
    // Mark the old tuple as deleted
    TupleMeta delete_meta{0, true};
    table_info_->table_->UpdateTupleMeta(delete_meta, old_rid);

    // Delete old tuple from all indexes
    for (auto &index_info : index_infos_) {
      auto old_key =
          old_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(old_key, old_rid, exec_ctx_->GetTransaction());
    }

    // Generate updated tuple values
    std::vector<Value> values;
    values.reserve(child_executor_->GetOutputSchema().GetColumnCount());
    for (const auto &target_expr : plan_->target_expressions_) {
      values.push_back(target_expr->Evaluate(&old_tuple, child_executor_->GetOutputSchema()));
    }

    // Create and insert updated tuple
    Tuple update_tuple{values, &child_executor_->GetOutputSchema()};
    TupleMeta update_meta{0, false};
    auto insert_rid = table_info_->table_->InsertTuple(update_meta, update_tuple, exec_ctx_->GetLockManager(),
                                                       exec_ctx_->GetTransaction(), table_info_->oid_);

    // Update all indexes with new tuple if insert succeeded
    if (insert_rid.has_value()) {
      for (auto &index_info : index_infos_) {
        auto new_key =
            update_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        index_info->index_->InsertEntry(new_key, insert_rid.value(), exec_ctx_->GetTransaction());
      }
      updated_rows++;
    }
  }

  // Yield the number of rows updated
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(TypeId::INTEGER, updated_rows);
  *tuple = Tuple(values, &GetOutputSchema());
  is_updated_ = true;
  return true;
}

}  // namespace bustub
