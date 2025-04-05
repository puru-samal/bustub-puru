//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// external_merge_sort_executor.cpp
//
// Identification: src/execution/external_merge_sort_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/external_merge_sort_executor.h"
#include <vector>
#include "execution/plans/sort_plan.h"

namespace bustub {

template <size_t K>
ExternalMergeSortExecutor<K>::ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                                                        std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      cmp_(plan->GetOrderBy()),
      child_executor_(std::move(child_executor)),
      bpm_(exec_ctx->GetBufferPoolManager()) {}

/** Initialize the external merge sort */
template <size_t K>
void ExternalMergeSortExecutor<K>::Init() {
  child_executor_->Init();

  // Create initial runs
  std::vector<page_id_t> page_ids;
  std::optional<WritePageGuard> curr_write_guard;
  Tuple tuple;
  RID rid;

  // Initial run: Pass 0
  while (child_executor_->Next(&tuple, &rid)) {
    // Get a new page if needed
    if (!curr_write_guard.has_value()) {
      auto page_id = bpm_->NewPage();
      curr_write_guard = bpm_->WritePage(page_id);
      auto sort_page = curr_write_guard->AsMut<SortPage>();
      sort_page->Init(GetOutputSchema());
      page_ids.push_back(page_id);
    }

    // Append tuple to the current page while it is not full
    auto sort_page = curr_write_guard->AsMut<SortPage>();
    auto full_after_append = sort_page->AppendTuple(tuple);

    // Page is full, sort, create a run, and release the page
    if (full_after_append) {
      sort_page->SortTuples(GetOutputSchema(), plan_, cmp_);
      runs_.emplace_back(MergeSortRun(page_ids, bpm_));
      page_ids.clear();
      curr_write_guard = std::nullopt;
    }
  }

  // Handle the last page if it exists
  if (curr_write_guard.has_value()) {
    auto sort_page = curr_write_guard->AsMut<SortPage>();
    sort_page->SortTuples(GetOutputSchema(), plan_, cmp_);
    runs_.emplace_back(MergeSortRun(page_ids, bpm_));
    page_ids.clear();
    curr_write_guard = std::nullopt;
  }

  // Merge phase: merge K runs at a time until only one run remains
  while (runs_.size() > 1) {
    std::vector<MergeSortRun> new_runs;
    // Take K runs at a time and merge them
    for (size_t i = 0; i < runs_.size(); i += K) {
      std::vector<MergeSortRun> to_merge;
      size_t runs_to_merge = std::min(K, runs_.size() - i);
      to_merge.reserve(runs_to_merge);

      for (size_t j = 0; j < runs_to_merge; j++) {
        to_merge.push_back(std::move(runs_[i + j]));
      }
      new_runs.push_back(MergeRuns(to_merge));
    }
    runs_ = std::move(new_runs);
  }

  if (runs_.size() == 1) {
    current_iterator_ = runs_[0].Begin();
    is_initialized_ = true;
  }
}

/**
 * Yield the next tuple from the external merge sort.
 * @param[out] tuple The next tuple produced by the external merge sort.
 * @param[out] rid The next tuple RID produced by the external merge sort.
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
template <size_t K>
auto ExternalMergeSortExecutor<K>::Next(Tuple *tuple, RID *rid) -> bool {
  if (!is_initialized_ || current_iterator_ == runs_[0].End()) {
    return false;
  }

  *tuple = *current_iterator_;
  *rid = (*current_iterator_).GetRid();
  ++current_iterator_;
  return true;
}

template <size_t K>
auto ExternalMergeSortExecutor<K>::MergeRuns(const std::vector<MergeSortRun> &input_runs) -> MergeSortRun {
  // Initialize merged pages, write guard, and run iterators
  std::vector<page_id_t> merged_pages;
  std::optional<WritePageGuard> write_guard;
  std::vector<RunIterator> run_iters;

  // Initialize run iterators
  for (const auto &run : input_runs) {
    auto iter = std::make_shared<MergeSortRun::Iterator>(run.Begin());
    if (*iter != run.End()) {
      run_iters.push_back(RunIterator{iter, &run});
    }
  }

  // Merge runs
  while (!run_iters.empty()) {
    // Get new page if needed
    if (!write_guard.has_value()) {
      auto page_id = bpm_->NewPage();
      write_guard = bpm_->WritePage(page_id);
      auto sort_page = write_guard->AsMut<SortPage>();
      sort_page->Init(GetOutputSchema());
      merged_pages.push_back(page_id);
    }

    // Find iterator with smallest tuple
    size_t min_idx = 0;
    for (size_t i = 1; i < run_iters.size(); i++) {
      if (cmp_(
              {GenerateSortKey(*(*run_iters[i].iter_), plan_->GetOrderBy(), GetOutputSchema()), *(*run_iters[i].iter_)},
              {GenerateSortKey(*(*run_iters[min_idx].iter_), plan_->GetOrderBy(), GetOutputSchema()),
               *(*run_iters[min_idx].iter_)})) {
        min_idx = i;
      }
    }

    // append smallest tuple
    auto sort_page = write_guard->AsMut<SortPage>();
    auto full_after_append = sort_page->AppendTuple(*(*run_iters[min_idx].iter_));

    // Advance iterator
    ++(*run_iters[min_idx].iter_);

    // Remove finished iterator
    if (*run_iters[min_idx].iter_ == run_iters[min_idx].run_->End()) {
      run_iters.erase(run_iters.begin() + min_idx);
    }

    // Release page if it is full
    if (full_after_append) {
      write_guard = std::nullopt;
    }
  }

  return {std::move(merged_pages), bpm_};
}
}  // namespace bustub

template class bustub::ExternalMergeSortExecutor<2>;
