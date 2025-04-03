//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  void Init() override;

  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  auto MakeLeftJoinKey(const Tuple *left_tuple) -> HashJoinKey {
    std::vector<Value> values;
    for (const auto &expr : plan_->LeftJoinKeyExpressions()) {
      values.emplace_back(expr->Evaluate(left_tuple, left_child_->GetOutputSchema()));
    }
    return {values};
  };
  auto MakeRightJoinKey(const Tuple *right_tuple) -> HashJoinKey {
    std::vector<Value> values;
    for (const auto &expr : plan_->RightJoinKeyExpressions()) {
      values.emplace_back(expr->Evaluate(right_tuple, right_child_->GetOutputSchema()));
    }
    return {values};
  };

  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;

  /** The left child of the join. */
  std::unique_ptr<AbstractExecutor> left_child_;

  /** The right child of the join. */
  std::unique_ptr<AbstractExecutor> right_child_;

  /** The hash table for the left child. */
  std::unordered_map<HashJoinKey, std::vector<Tuple>> hash_table_{};

  /** The current left tuple being processed */
  Tuple current_left_tuple_;

  /** The current matches for the current left tuple */
  std::vector<Tuple> current_matches_;

  /** Whether we found a match for the current left tuple (for LEFT JOIN) */
  bool found_match_{false};
};

}  // namespace bustub
