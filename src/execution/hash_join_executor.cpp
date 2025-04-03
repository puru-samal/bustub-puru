//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "common/macros.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Construct a new HashJoinExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The HashJoin join plan to be executed
 * @param left_child The child executor that produces tuples for the left side of join
 * @param right_child The child executor that produces tuples for the right side of join
 */
HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for Spring 2025: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

/** Initialize the join */
void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();

  Tuple right_tuple;
  RID right_rid;

  // Build the hash table on the right child
  while (right_child_->Next(&right_tuple, &right_rid)) {
    auto join_key = MakeRightJoinKey(&right_tuple);
    hash_table_[join_key].push_back(right_tuple);
  }
}

/**
 * Yield the next tuple from the join.
 * @param[out] tuple The next tuple produced by the join.
 * @param[out] rid The next tuple RID, not used by hash join.
 * @return `true` if a tuple was produced, `false` if there are no more tuples.
 */
auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // If we have matches from previous left tuple, output them first
  while (!current_matches_.empty()) {
    auto right_tuple = current_matches_.back();
    current_matches_.pop_back();

    std::vector<Value> values;
    // Add all values from left tuple
    for (uint32_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); i++) {
      values.push_back(current_left_tuple_.GetValue(&left_child_->GetOutputSchema(), i));
    }
    // Add all values from right tuple
    for (uint32_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); i++) {
      values.push_back(right_tuple.GetValue(&right_child_->GetOutputSchema(), i));
    }
    *tuple = Tuple(values, &GetOutputSchema());
    return true;
  }

  // Get the next tuple from left child
  while (left_child_->Next(&current_left_tuple_, rid)) {
    auto join_key = MakeLeftJoinKey(&current_left_tuple_);
    auto it = hash_table_.find(join_key);

    if (it != hash_table_.end()) {
      // Found matches - save them and return the first one
      current_matches_ = it->second;
      found_match_ = true;

      // Return first match (if any)
      if (!current_matches_.empty()) {
        auto right_tuple = current_matches_.back();
        current_matches_.pop_back();

        std::vector<Value> values;
        // Add all values from left tuple
        for (uint32_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); i++) {
          values.push_back(current_left_tuple_.GetValue(&left_child_->GetOutputSchema(), i));
        }
        // Add all values from right tuple
        for (uint32_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); i++) {
          values.push_back(right_tuple.GetValue(&right_child_->GetOutputSchema(), i));
        }
        *tuple = Tuple(values, &GetOutputSchema());
        return true;
      }
    } else if (plan_->GetJoinType() == JoinType::LEFT) {
      // Left join: output tuple with NULL values for right side
      std::vector<Value> values;
      // Add all values from left tuple
      for (uint32_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); i++) {
        values.push_back(current_left_tuple_.GetValue(&left_child_->GetOutputSchema(), i));
      }
      // Add NULL values for right tuple
      for (uint32_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); i++) {
        values.push_back(ValueFactory::GetNullValueByType(right_child_->GetOutputSchema().GetColumn(i).GetType()));
      }
      *tuple = Tuple(values, &GetOutputSchema());
      return true;
    }
  }

  // No more tuples from left child
  return false;
}

}  // namespace bustub
