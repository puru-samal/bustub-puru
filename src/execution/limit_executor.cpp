//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"
#include "common/macros.h"

namespace bustub {

/**
 * Construct a new LimitExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The limit plan to be executed
 * @param child_executor The child executor from which limited tuples are pulled
 */
LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/** Initialize the limit */
void LimitExecutor::Init() {
  if (!is_initialized_) {
    child_executor_->Init();
    Tuple tuple;
    RID rid;
    while (child_executor_->Next(&tuple, &rid)) {
      output_tuples_.push_back(tuple);
    }
    is_initialized_ = true;
  }
  output_count_ = 0;
  current_tuple_ = output_tuples_.begin();
}

/**
 * Yield the next tuple from the limit.
 * @param[out] tuple The next tuple produced by the limit
 * @param[out] rid The next tuple RID produced by the limit
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (output_count_ >= plan_->GetLimit() || current_tuple_ == output_tuples_.end() || !is_initialized_) {
    return false;
  }
  *tuple = *current_tuple_;
  *rid = (*current_tuple_).GetRid();
  current_tuple_++;
  output_count_++;
  return true;
}

}  // namespace bustub
