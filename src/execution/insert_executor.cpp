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

#include "execution/executors/insert_executor.h"
#include <memory>
#include "concurrency/transaction_manager.h"

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
      child_executor_(std::move(child_executor)) {
  // Get the index infos
  if (table_info_ != nullptr) {
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
  if (is_inserted_ || table_info_ == nullptr) {
    return false;
  }

  Tuple insert_tuple;
  RID insert_rid;
  int32_t inserted_rows = 0;
  auto txn = exec_ctx_->GetTransaction();
  auto txn_mgr = exec_ctx_->GetTransactionManager();

  // Find primary key index if it exists
  const IndexInfo *pk_index = nullptr;
  for (auto &index_info : index_infos_) {
    if (index_info->is_primary_key_) {
      pk_index = index_info.get();
      break;
    }
  }

  while (child_executor_->Next(&insert_tuple, &insert_rid)) {
    // If there's a primary key index, check for existing entries first
    if (pk_index != nullptr) {
      auto key =
          insert_tuple.KeyFromTuple(table_info_->schema_, pk_index->key_schema_, pk_index->index_->GetKeyAttrs());
      std::vector<RID> result;
      pk_index->index_->ScanKey(key, &result, txn);

      // If key exists, abort transaction
      if (!result.empty()) {
        auto [pk_meta, pk_tuple, pk_undo_link] = GetTupleAndUndoLink(txn_mgr, table_info_->table_.get(), result[0]);
        if (!pk_meta.is_deleted_) {
          txn->SetTainted();
          throw ExecutionException("Duplicate key in primary key index");
        }
      }
    }

    // Set tuple metadata with transaction's temporary timestamp
    TupleMeta insert_meta{txn->GetTransactionTempTs(), false};

    // Insert the an empty tuple into the table
    auto insert_rid = table_info_->table_->InsertTuple(insert_meta, insert_tuple, exec_ctx_->GetLockManager(), txn,
                                                       table_info_->oid_);

    // If the tuple is inserted successfully, update the tuple, meta and undo link, and the indexes
    if (insert_rid.has_value()) {
      // Try to insert into primary key index first if it exists
      if (pk_index != nullptr) {
        auto key =
            insert_tuple.KeyFromTuple(table_info_->schema_, pk_index->key_schema_, pk_index->index_->GetKeyAttrs());
        if (!pk_index->index_->InsertEntry(key, insert_rid.value(), txn)) {
          // Another transaction inserted the same key between our check and insert
          txn->SetTainted();
          throw ExecutionException("Concurrent insert detected in primary key index");
        }
      }
      // P4: Update the tuple, meta and undo link
      // Add RID to transaction's write set
      UpdateTupleAndUndoLink(txn_mgr, insert_rid.value(), std::nullopt, table_info_->table_.get(), txn, insert_meta,
                             insert_tuple, nullptr);
      txn->AppendWriteSet(table_info_->oid_, insert_rid.value());
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
