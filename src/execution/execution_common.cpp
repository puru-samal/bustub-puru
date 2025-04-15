//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// execution_common.cpp
//
// Identification: src/execution/execution_common.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/execution_common.h"

#include "catalog/catalog.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"

namespace bustub {

TupleComparator::TupleComparator(std::vector<OrderBy> order_bys) : order_bys_(std::move(order_bys)) {}

/** TODO(P3): Implement the comparison method */
auto TupleComparator::operator()(const SortEntry &entry_a, const SortEntry &entry_b) const -> bool {
  const auto &key_a = entry_a.first;
  const auto &key_b = entry_b.first;

  // Compare each key component according to the order-by type
  for (size_t i = 0; i < order_bys_.size(); i++) {
    const auto &order_by = order_bys_[i];
    const auto &value_a = key_a[i];
    const auto &value_b = key_b[i];

    // Compare values
    if (value_a.CompareEquals(value_b) == CmpBool::CmpTrue) {
      continue;  // Values are equal, move to next key component
    }

    // Values are different, return comparison result based on order type
    bool less_than = value_a.CompareLessThan(value_b) == CmpBool::CmpTrue;
    // Default is ASC, so if DESC, return the opposite of the comparison result
    return order_by.first == OrderByType::DESC ? !less_than : less_than;
  }

  // All key components are equal
  return false;
}

/**
 * Generate sort key for a tuple based on the order by expressions.
 *
 * TODO(P3): Implement this method.
 */
auto GenerateSortKey(const Tuple &tuple, const std::vector<OrderBy> &order_bys, const Schema &schema) -> SortKey {
  SortKey key;
  key.reserve(order_bys.size());

  // Generate a key component for each order-by expression
  for (const auto &order_by : order_bys) {
    key.push_back(order_by.second->Evaluate(&tuple, schema));
  }

  return key;
}

/**
 * Above are all you need for P3.
 * You can ignore the remaining part of this file until P4.
 */

/**
 * @brief Reconstruct a tuple by applying the provided undo logs from the base tuple. All logs in the undo_logs are
 * applied regardless of the timestamp
 *
 * @param schema The schema of the base tuple and the returned tuple.
 * @param base_tuple The base tuple to start the reconstruction from.
 * @param base_meta The metadata of the base tuple.
 * @param undo_logs The list of undo logs to apply during the reconstruction, the front is applied first.
 * @return An optional tuple that represents the reconstructed tuple. If the tuple is deleted as the result, returns
 * std::nullopt.
 */
auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  // Start with the base tuple's Values
  std::vector<Value> values;
  for (size_t i = 0; i < schema->GetColumnCount(); i++) {
    values.push_back(base_tuple.GetValue(schema, i));
  }
  bool is_deleted = base_meta.is_deleted_;

  // Apply each undo log in order (most recent to oldest)
  for (const auto &log : undo_logs) {
    is_deleted = log.is_deleted_;

    // If the undo log is a deletion, skip the rest of the loop
    if (is_deleted) {
      continue;
    }

    // First collect modified attributes and create partial schema
    std::vector<uint32_t> modified_attrs;
    modified_attrs.reserve(log.modified_fields_.size());
    for (size_t i = 0; i < log.modified_fields_.size(); i++) {
      if (log.modified_fields_[i]) {
        modified_attrs.push_back(i);
      }
    }
    auto partial_schema = Schema::CopySchema(schema, modified_attrs);

    // Then process the modifications in a single pass
    for (size_t i = 0; i < modified_attrs.size(); i++) {
      values[modified_attrs[i]] = log.tuple_.GetValue(&partial_schema, i);
    }
  }

  // If the final state is deleted, return nullopt
  if (is_deleted) {
    return std::nullopt;
  }

  // Create and return the reconstructed tuple
  return Tuple(values, schema);
}

/**
 * @brief Collects the undo logs sufficient to reconstruct the tuple w.r.t. the txn.
 *
 * @param rid The RID of the tuple.
 * @param base_meta The metadata of the base tuple.
 * @param base_tuple The base tuple.
 * @param undo_link The undo link to the latest undo log.
 * @param txn The transaction.
 * @param txn_mgr The transaction manager.
 * @return An optional vector of undo logs to pass to ReconstructTuple(). std::nullopt if the tuple did not exist at the
 * time.
 */
auto CollectUndoLogs(RID rid, const TupleMeta &base_meta, const Tuple &base_tuple, std::optional<UndoLink> undo_link,
                     Transaction *txn, TransactionManager *txn_mgr) -> std::optional<std::vector<UndoLog>> {
  std::vector<UndoLog> undo_logs;

  // Case 1: The base tuple is the most recent version relative to the transaction
  if (base_meta.ts_ <= txn->GetReadTs()) {
    return undo_logs;
  }

  // Case 2: The transaction is reading it's own write
  if (base_meta.ts_ == txn->GetTransactionTempTs()) {
    return undo_logs;
  }

  // Case 3: if undo_link doesn't exist, it means the tuple doesn't exist at this timestamp
  if (!undo_link.has_value()) {
    return std::nullopt;
  }

  // Case 4: The base tuple is newer than the transaction or has been modified by another uncommitted transaction
  // Need to traverse the version chain and collect ALL undo logs up to read timestamp
  auto current_link = undo_link.value();
  bool found_valid_version = false;

  while (current_link.IsValid()) {
    auto log = txn_mgr->GetUndoLog(current_link);
    undo_logs.push_back(log);

    if (log.ts_ <= txn->GetReadTs()) {
      found_valid_version = true;
      break;
    }
    current_link = log.prev_version_;
  }

  // If we never found a valid version, the tuple didn't exist at this timestamp
  if (!found_valid_version) {
    return std::nullopt;
  }

  return undo_logs;
}

/**
 * @brief Generates a new undo log as the transaction tries to modify this tuple at the first time.
 *
 * @param schema The schema of the table.
 * @param base_tuple The base tuple before the update, the one retrieved from the table heap. nullptr if the tuple is
 * deleted.
 * @param target_tuple The target tuple after the update. nullptr if this is a deletion.
 * @param ts The timestamp of the base tuple.
 * @param prev_version The undo link to the latest undo log of this tuple.
 * @return The generated undo log.
 */
auto GenerateNewUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple, timestamp_t ts,
                        UndoLink prev_version) -> UndoLog {
  UNIMPLEMENTED("not implemented");
}

/**
 * @brief Generate the updated undo log to replace the old one, whereas the tuple is already modified by this txn once.
 *
 * @param schema The schema of the table.
 * @param base_tuple The base tuple before the update, the one retrieved from the table heap. nullptr if the tuple is
 * deleted.
 * @param target_tuple The target tuple after the update. nullptr if this is a deletion.
 * @param log The original undo log.
 * @return The updated undo log.
 */
auto GenerateUpdatedUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple,
                            const UndoLog &log) -> UndoLog {
  UNIMPLEMENTED("not implemented");
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  fmt::println(stderr, "debug_hook: {}", info);

  auto iter = table_heap->MakeIterator();
  while (!iter.IsEnd()) {
    auto [meta, tuple] = iter.GetTuple();
    auto rid = iter.GetRID();
    auto undo_link = txn_mgr->GetUndoLink(rid);

    // Print base tuple info
    std::string ts_str =
        meta.ts_ >= TXN_START_ID ? fmt::format("txn{}", meta.ts_ - TXN_START_ID) : std::to_string(meta.ts_);
    if (meta.is_deleted_) {
      fmt::println(stderr, "RID={}/{} ts={} <del marker> tuple={}", rid.GetPageId(), rid.GetSlotNum(), ts_str,
                   tuple.ToString(&table_info->schema_));
    } else {
      fmt::println(stderr, "RID={}/{} ts={} tuple={}", rid.GetPageId(), rid.GetSlotNum(), ts_str,
                   tuple.ToString(&table_info->schema_));
    }

    if (undo_link.has_value()) {
      // Print version chain
      auto current_link = undo_link.value();
      while (current_link.IsValid()) {
        auto log = txn_mgr->GetUndoLog(current_link);

        // For version chain entries, print the tuple with modified fields only
        std::string tuple_str;
        if (log.is_deleted_) {
          tuple_str = "<del>";
        } else {
          // Create partial schema for only modified fields
          std::vector<uint32_t> modified_attrs;
          for (size_t i = 0; i < log.modified_fields_.size(); i++) {
            if (log.modified_fields_[i]) {
              modified_attrs.push_back(i);
            }
          }
          auto partial_schema = Schema::CopySchema(&table_info->schema_, modified_attrs);
          tuple_str = log.tuple_.ToString(&partial_schema);
          // Replace unmodified fields with "_"
          for (size_t i = 0; i < log.modified_fields_.size(); i++) {
            if (!log.modified_fields_[i]) {
              tuple_str = tuple_str.substr(0, tuple_str.find_last_of(')')) + ", _)";
            }
          }
        }

        fmt::println(stderr, "  txn{}@{} {} ts={}", current_link.prev_txn_ - TXN_START_ID, current_link.prev_log_idx_,
                     tuple_str, log.ts_);
        current_link = log.prev_version_;
      }
    }

    ++iter;
  }
}

}  // namespace bustub
