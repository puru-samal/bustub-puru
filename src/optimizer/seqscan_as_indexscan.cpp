//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seqscan_as_indexscan.cpp
//
// Identification: src/optimizer/seqscan_as_indexscan.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

/**
 * @brief Optimizes seq scan as index scan if there's an index on a table
 */
auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(P3): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule

  // Optimize children
  std::vector<AbstractPlanNodeRef> optimized_children;
  for (const auto &child : plan->GetChildren()) {
    optimized_children.push_back(OptimizeSeqScanAsIndexScan(child));
  }

  // Clone the plan with optimized children
  auto optimized_plan = plan->CloneWithChildren(std::move(optimized_children));

  // Only optimize SeqScan nodes
  if (optimized_plan->GetType() != PlanType::SeqScan) {
    return optimized_plan;
  }

  const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);

  // If there is no filter predicate, this plan cannot be optimized to an index scan
  if (seq_scan_plan.filter_predicate_ == nullptr) {
    return optimized_plan;
  }

  // If there is no index on the table, this plan cannot be optimized to an index scan
  const auto table_info = catalog_.GetTable(seq_scan_plan.table_oid_);
  auto indexes = catalog_.GetTableIndexes(table_info->name_);
  if (indexes.empty()) {
    return optimized_plan;
  }

  // Try to optimize the seq scan as an index scan by recursively trying to optimize the filter predicate
  auto index_scan_plan = TryOptimizeSeqScanAsIndexScan(seq_scan_plan.output_schema_, seq_scan_plan.table_oid_, indexes,
                                                       seq_scan_plan.filter_predicate_);

  // If an index scan plan is found, return it
  if (index_scan_plan.has_value()) {
    return index_scan_plan.value();
  }

  // If no index scan plan is found, return the original plan
  return optimized_plan;
}

auto Optimizer::TryOptimizeSeqScanAsIndexScan(const SchemaRef &output_schema, table_oid_t table_oid,
                                              const std::vector<std::shared_ptr<IndexInfo>> &indexes,
                                              const AbstractExpressionRef &predicate)
    -> std::optional<AbstractPlanNodeRef> {
  BUSTUB_ASSERT(predicate != nullptr, "Predicate should not be nullptr");

  // Base case: predicate is a ComparisonExpression
  auto cmp_predicate = dynamic_cast<ComparisonExpression *>(predicate.get());
  if (cmp_predicate != nullptr) {
    // If the predicate is not an equality predicate, return nullopt
    if (cmp_predicate->comp_type_ != ComparisonType::Equal) {
      return std::nullopt;
    }

    AbstractExpression *column_expr = nullptr;
    AbstractExpression *constant_expr = nullptr;

    // Check if the left expression is a ColumnValueExpression
    // and the right expression is a ConstantValueExpression
    if (dynamic_cast<ColumnValueExpression *>(cmp_predicate->GetChildAt(0).get()) != nullptr &&
        dynamic_cast<ConstantValueExpression *>(cmp_predicate->GetChildAt(1).get()) != nullptr) {
      column_expr = cmp_predicate->GetChildAt(0).get();
      constant_expr = cmp_predicate->GetChildAt(1).get();
    }

    // Check if the left expression is a ConstantValueExpression
    // and the right expression is a ColumnValueExpression
    if (dynamic_cast<ConstantValueExpression *>(cmp_predicate->GetChildAt(0).get()) != nullptr &&
        dynamic_cast<ColumnValueExpression *>(cmp_predicate->GetChildAt(1).get()) != nullptr) {
      column_expr = cmp_predicate->GetChildAt(1).get();
      constant_expr = cmp_predicate->GetChildAt(0).get();
    }

    // If the column expression or constant expression is not valid, return nullopt
    if (column_expr == nullptr || constant_expr == nullptr) {
      return std::nullopt;
    }

    auto col_expr = dynamic_cast<ColumnValueExpression *>(column_expr);
    auto const_expr = dynamic_cast<ConstantValueExpression *>(constant_expr);

    // Find matching index on the column
    const auto table_info = catalog_.GetTable(table_oid);
    auto index_match = MatchIndex(table_info->name_, col_expr->GetColIdx());
    if (!index_match.has_value()) {
      return std::nullopt;
    }

    // Create an index scan plan
    auto index_oid = std::get<0>(index_match.value());
    auto index_scan_plan = std::make_shared<IndexScanPlanNode>(
        output_schema, table_oid, index_oid, predicate,
        std::vector<AbstractExpressionRef>{std::make_shared<ConstantValueExpression>(const_expr->val_)});

    return index_scan_plan;
  }

  // Recursive case: predicate is a LogicExpression which may have leaf nodes with ComparisonExpression
  auto logic_predicate = dynamic_cast<LogicExpression *>(predicate.get());
  if (logic_predicate == nullptr || logic_predicate->logic_type_ != LogicType::Or) {
    return std::nullopt;
  }

  // Try to optimize the left and right children
  auto left_plan = TryOptimizeSeqScanAsIndexScan(output_schema, table_oid, indexes, logic_predicate->GetChildAt(0));
  auto right_plan = TryOptimizeSeqScanAsIndexScan(output_schema, table_oid, indexes, logic_predicate->GetChildAt(1));

  // If both children are not optimized, return the original plan
  if (!(left_plan.has_value() && right_plan.has_value())) {
    return std::nullopt;
  }

  auto left_index_scan_plan = dynamic_cast<const IndexScanPlanNode *>(left_plan.value().get());
  auto right_index_scan_plan = dynamic_cast<const IndexScanPlanNode *>(right_plan.value().get());

  // If the left and right index scan plan are not on the same index, return nullopt
  auto index_match = left_index_scan_plan->GetIndexOid() == right_index_scan_plan->GetIndexOid();
  if (!index_match) {
    return std::nullopt;
  }

  // Merge the two index scan plans by joining the pred_keys
  auto merged_pred_keys = left_index_scan_plan->pred_keys_;
  merged_pred_keys.insert(merged_pred_keys.end(), right_index_scan_plan->pred_keys_.begin(),
                          right_index_scan_plan->pred_keys_.end());

  auto merged_index_scan_plan = std::make_shared<IndexScanPlanNode>(
      output_schema, table_oid, left_index_scan_plan->GetIndexOid(), predicate, merged_pred_keys);

  return merged_index_scan_plan;
}

}  // namespace bustub
