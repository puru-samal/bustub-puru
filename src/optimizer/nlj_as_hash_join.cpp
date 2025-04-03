//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nlj_as_hash_join.cpp
//
// Identification: src/optimizer/nlj_as_hash_join.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

/**
 * @brief optimize nested loop join into hash join.
 * In the starter code, we will check NLJs with exactly one equal condition. You can further support optimizing joins
 * with multiple eq conditions.
 */
auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for Spring 2025: You should support join keys of any number of conjunction of equi-conditions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
  // Optimize children
  std::vector<AbstractPlanNodeRef> optimized_children;
  for (const auto &child : plan->GetChildren()) {
    optimized_children.push_back(OptimizeNLJAsHashJoin(child));
  }

  // Clone the plan with optimized children
  auto optimized_plan = plan->CloneWithChildren(std::move(optimized_children));

  // Only optimize NestedLoopJoin nodes
  if (optimized_plan->GetType() != PlanType::NestedLoopJoin) {
    return optimized_plan;
  }

  // Check if the plan is a NestedLoopJoin
  const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
  auto join_predicate = nlj_plan.Predicate();

  // Try to optimize the nlj as a hash join by recursively trying to optimize the join predicate
  auto hash_join_plan = TryOptimizeNLJAsHashJoin(optimized_plan->output_schema_, nlj_plan.GetLeftPlan(),
                                                 nlj_plan.GetRightPlan(), nlj_plan.GetJoinType(), join_predicate);

  // If a hash join plan is found, return it
  if (hash_join_plan.has_value()) {
    return hash_join_plan.value();
  }

  // If no hash join plan is found, return the original plan
  return optimized_plan;
}

auto Optimizer::TryOptimizeNLJAsHashJoin(const SchemaRef &output_schema, AbstractPlanNodeRef left,
                                         AbstractPlanNodeRef right, JoinType join_type,
                                         const AbstractExpressionRef &predicate) -> std::optional<AbstractPlanNodeRef> {
  BUSTUB_ASSERT(predicate != nullptr, "Predicate should not be nullptr");

  // Base case: predicate is a ComparisonExpression
  auto cmp_predicate = dynamic_cast<ComparisonExpression *>(predicate.get());
  if (cmp_predicate != nullptr) {
    // If the predicate is not an equality predicate, return nullopt
    if (cmp_predicate->comp_type_ != ComparisonType::Equal) {
      return std::nullopt;
    }

    auto expr_0 = dynamic_cast<ColumnValueExpression *>(cmp_predicate->GetChildAt(0).get());
    auto expr_1 = dynamic_cast<ColumnValueExpression *>(cmp_predicate->GetChildAt(1).get());
    std::vector<AbstractExpressionRef> left_key_expressions;
    std::vector<AbstractExpressionRef> right_key_expressions;

    if (expr_0 == nullptr || expr_1 == nullptr) {
      return std::nullopt;
    }

    // Check which side of the table the columd expressions belong to
    if (expr_0->GetTupleIdx() == 0 && expr_1->GetTupleIdx() == 1) {
      left_key_expressions.push_back(cmp_predicate->GetChildAt(0));
      right_key_expressions.push_back(cmp_predicate->GetChildAt(1));
    } else if (expr_0->GetTupleIdx() == 1 && expr_1->GetTupleIdx() == 0) {
      left_key_expressions.push_back(cmp_predicate->GetChildAt(1));
      right_key_expressions.push_back(cmp_predicate->GetChildAt(0));
    }

    // Create a hash join plan
    auto hash_join_plan = std::make_shared<HashJoinPlanNode>(output_schema, left, right, left_key_expressions,
                                                             right_key_expressions, join_type);

    return hash_join_plan;
  }

  // Recursive case: predicate is a LogicExpression which may have leaf nodes with ComparisonExpression
  auto logic_predicate = dynamic_cast<LogicExpression *>(predicate.get());
  if (logic_predicate == nullptr || logic_predicate->logic_type_ != LogicType::And) {
    return std::nullopt;
  }

  // Try to optimize the left and right children
  auto left_plan = TryOptimizeNLJAsHashJoin(output_schema, left, right, join_type, logic_predicate->GetChildAt(0));
  auto right_plan = TryOptimizeNLJAsHashJoin(output_schema, left, right, join_type, logic_predicate->GetChildAt(1));

  // If both children are not optimized, return the original plan
  if (!(left_plan.has_value() && right_plan.has_value())) {
    return std::nullopt;
  }

  auto left_hash_join_plan = dynamic_cast<const HashJoinPlanNode *>(left_plan.value().get());
  auto right_hash_join_plan = dynamic_cast<const HashJoinPlanNode *>(right_plan.value().get());

  // Merge the left and right key expressions
  std::vector<AbstractExpressionRef> left_key_expressions;
  std::vector<AbstractExpressionRef> right_key_expressions;
  left_key_expressions.insert(left_key_expressions.end(), left_hash_join_plan->left_key_expressions_.begin(),
                              left_hash_join_plan->left_key_expressions_.end());

  left_key_expressions.insert(left_key_expressions.end(), right_hash_join_plan->left_key_expressions_.begin(),
                              right_hash_join_plan->left_key_expressions_.end());

  right_key_expressions.insert(right_key_expressions.end(), left_hash_join_plan->right_key_expressions_.begin(),
                               left_hash_join_plan->right_key_expressions_.end());

  right_key_expressions.insert(right_key_expressions.end(), right_hash_join_plan->right_key_expressions_.begin(),
                               right_hash_join_plan->right_key_expressions_.end());

  // Create a hash join plan
  auto hash_join_plan = std::make_shared<HashJoinPlanNode>(output_schema, left, right, left_key_expressions,
                                                           right_key_expressions, join_type);

  return hash_join_plan;
}

}  // namespace bustub
