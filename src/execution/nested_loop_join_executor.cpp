//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "common/macros.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Construct a new NestedLoopJoinExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The nested loop join plan to be executed
 * @param left_executor The child executor that produces tuple for the left side of join
 * @param right_executor The child executor that produces tuple for the right side of join
 */
NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for Spring 2025: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

/** Initialize the join */
void NestedLoopJoinExecutor::Init() {
  // Initialize both executors
  left_executor_->Init();
  right_executor_->Init();

  // Get the first left tuple ready
  Tuple left_tuple;
  RID left_rid;
  has_left_ = left_executor_->Next(&left_tuple, &left_rid);
  if (has_left_) {
    left_tuple_ = left_tuple;
  }
}

/**
 * Yield the next tuple from the join.
 * @param[out] tuple The next tuple produced by the join
 * @param[out] rid The next tuple RID produced, not used by nested loop join.
 * @return `true` if a tuple was produced, `false` if there are no more tuples.
 */
auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // Get the join schemas
  const auto &left_schema = left_executor_->GetOutputSchema();
  const auto &right_schema = right_executor_->GetOutputSchema();

  while (has_left_) {
    Tuple right_tuple;
    RID right_rid;

    // Try to get the next right tuple
    bool has_right = right_executor_->Next(&right_tuple, &right_rid);
    // If we've exhausted the right relation
    if (!has_right) {
      // For LEFT JOIN: If we never found a match for this left tuple, output with NULL right tuple
      if (plan_->GetJoinType() == JoinType::LEFT && !found_match_) {
        std::vector<Value> values;

        // Add all values from left tuple
        for (uint32_t i = 0; i < left_schema.GetColumnCount(); i++) {
          values.push_back(left_tuple_.GetValue(&left_schema, i));
        }

        // Add NULL values for right tuple
        for (uint32_t i = 0; i < right_schema.GetColumnCount(); i++) {
          values.push_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(i).GetType()));
        }

        *tuple = Tuple(values, &GetOutputSchema());

        // Move to next left tuple
        Tuple left_tuple;
        RID left_rid;
        has_left_ = left_executor_->Next(&left_tuple, &left_rid);
        if (has_left_) {
          left_tuple_ = left_tuple;
          found_match_ = false;
        }
        right_executor_->Init();  // Reset right executor for next left tuple
        return true;
      }

      // Get next left tuple and reset right executor
      Tuple left_tuple;
      RID left_rid;
      has_left_ = left_executor_->Next(&left_tuple, &left_rid);
      if (has_left_) {
        left_tuple_ = left_tuple;
        found_match_ = false;
        right_executor_->Init();
        continue;
      }
      return false;
    }

    // If right tuple exists, evaluate join predicate
    Value predicate_value = plan_->Predicate()->EvaluateJoin(&left_tuple_, left_schema, &right_tuple, right_schema);

    // If predicate is true, output joined tuple
    if (!predicate_value.IsNull() && predicate_value.GetAs<bool>()) {
      std::vector<Value> values;

      // Add all values from left tuple
      for (uint32_t i = 0; i < left_schema.GetColumnCount(); i++) {
        values.push_back(left_tuple_.GetValue(&left_schema, i));
      }

      // Add all values from right tuple
      for (uint32_t i = 0; i < right_schema.GetColumnCount(); i++) {
        values.push_back(right_tuple.GetValue(&right_schema, i));
      }

      *tuple = Tuple(values, &GetOutputSchema());
      found_match_ = true;
      return true;
    }
  }

  return false;
}

}  // namespace bustub
