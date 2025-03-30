//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "common/macros.h"

#include "execution/executors/insert_executor.h"

namespace bustub {

/**
 * Construct a new InsertExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The insert plan to be executed
 * @param child_executor The child executor from which inserted tuples are pulled
 */
InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan_->GetTableOid()).get()),
      child_executor_(std::move(child_executor)),
      is_inserted_(false) {
  // Get the index infos
  if (table_info_) {
    index_infos_ = exec_ctx->GetCatalog()->GetTableIndexes(table_info_->name_);
  }
}

/** Initialize the insert */
void InsertExecutor::Init() { child_executor_->Init(); }

/**
 * Yield the number of rows inserted into the table.
 * @param[out] tuple The integer tuple indicating the number of rows inserted into the table
 * @param[out] rid The next tuple RID produced by the insert (ignore, not used)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: InsertExecutor::Next() does not use the `rid` out-parameter.
 * NOTE: InsertExecutor::Next() returns true with number of inserted rows produced only once.
 */
auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_inserted_ || !table_info_) {
    return false;
  }

  Tuple insert_tuple;
  TupleMeta insert_meta{0, false};
  RID insert_rid;
  int32_t inserted_rows = 0;

  while (child_executor_->Next(&insert_tuple, &insert_rid)) {
    // Insert the tuple into the table
    auto insert_rid = table_info_->table_->InsertTuple(insert_meta, insert_tuple, exec_ctx_->GetLockManager(),
                                                       exec_ctx_->GetTransaction(), table_info_->oid_);

    // If the tuple is inserted successfully, update the indexes
    if (insert_rid.has_value()) {
      for (auto &index_info : index_infos_) {
        auto index_key =
            insert_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        index_info->index_->InsertEntry(index_key, insert_rid.value(), exec_ctx_->GetTransaction());
      }
      inserted_rows++;
    }
  }
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(TypeId::INTEGER, inserted_rows);
  *tuple = Tuple(values, &GetOutputSchema());
  is_inserted_ = true;
  return true;
}

}  // namespace bustub
