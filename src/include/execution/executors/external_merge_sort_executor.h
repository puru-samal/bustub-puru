//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// external_merge_sort_executor.h
//
// Identification: src/include/execution/executors/external_merge_sort_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <cstddef>
#include <memory>
#include <utility>
#include <vector>
#include "common/config.h"
#include "common/macros.h"
#include "execution/execution_common.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/sort_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * Page to hold the intermediate data for external merge sort.
 *
 * Only fixed-length data will be supported in Spring 2025.
 */
class SortPage {
 public:
  /**
   * TODO(P3): Define and implement the methods for reading data from and writing data to the sort
   * page. Feel free to add other helper methods.
   */

  // Header format:
  // Header format (size in byte, 12 bytes in total):
  // ---------------------------------------------------------
  // | MaxTupleCount (4) | TupleCount (4) | TupleSize (4) |  ...                  |
  // ---------------------------------------------------------
  static constexpr int MAX_TUPLE_COUNT_OFFSET = 0;
  static constexpr int TUPLE_COUNT_OFFSET = sizeof(int);
  static constexpr int TUPLE_SIZE_OFFSET = sizeof(int) * 2;
  static constexpr int HEADER_SIZE = sizeof(int) * 3;

  // Delete all constructor / destructor to ensure memory safety
  SortPage() = delete;
  SortPage(const SortPage &other) = delete;

  void Init(const Schema &schema) {
    // Need to account for both the schema size AND serialization overhead
    tuple_data_size_ = schema.GetInlinedStorageSize() + sizeof(uint32_t);  // uint32_t for size prefix
    max_tuple_count_ = (BUSTUB_PAGE_SIZE - HEADER_SIZE) / tuple_data_size_;
    tuple_count_ = 0;
  }
  // Helper methods
  auto GetMaxTuples() const -> int { return max_tuple_count_; }
  auto GetTupleCount() const -> int { return tuple_count_; }
  auto GetTupleSize() const -> int { return tuple_data_size_; }
  auto IsFull() const -> bool { return GetTupleCount() == GetMaxTuples(); }
  auto IsEmpty() const -> bool { return GetTupleCount() == 0; }

  void WriteTuple(const Tuple &tuple, int slot_idx) {
    BUSTUB_ASSERT(0 <= GetTupleCount() && GetTupleCount() <= GetMaxTuples(), "[SortPage:WriteTuple] Page is full");
    BUSTUB_ASSERT(0 <= slot_idx && slot_idx <= GetTupleCount(), "[SortPage:WriteTuple] Invalid slot index");
    // Use Tuple's SerializeTo method
    tuple.SerializeTo(tuple_data_ + (slot_idx * tuple_data_size_));
  }

  void ReadTuple(Tuple &tuple, int slot_idx) const {
    BUSTUB_ASSERT(0 <= slot_idx && slot_idx < GetTupleCount(), "[SortPage:ReadTuple] Invalid slot index");
    tuple.DeserializeFrom(tuple_data_ + (slot_idx * tuple_data_size_));
  }

  /**
   * Append a tuple to the sort page.
   * @param tuple The tuple to append.
   * @return `true` if the the page is full after appending the tuple, `false` if the page is not full.
   */
  auto AppendTuple(const Tuple &tuple) -> bool {
    BUSTUB_ASSERT(!IsFull(), "[SortPage:AppendTuple] Page is full");
    WriteTuple(tuple, tuple_count_);
    tuple_count_++;
    return IsFull();
  }

  /**
   * Sort the tuples in the sort page.
   * @param schema The schema of the tuples.
   * @param plan The sort plan.
   * @param comp The comparator for the tuples.
   */
  void SortTuples(const Schema &schema, const SortPlanNode *plan, const TupleComparator &comp) {
    std::vector<SortEntry> entries;
    entries.reserve(tuple_count_);

    // Create SortEntry for each tuple
    for (int i = 0; i < tuple_count_; i++) {
      Tuple tuple;
      ReadTuple(tuple, i);
      entries.emplace_back(GenerateSortKey(tuple, plan->GetOrderBy(), schema), tuple);
    }

    // Sort using the comparator
    std::sort(entries.begin(), entries.end(), comp);

    // Write back the sorted tuples
    for (int i = 0; i < tuple_count_; i++) {
      WriteTuple(entries[i].second, i);
    }
  }

 private:
  /**
   * TODO(P3): Define the private members. You may want to have some necessary metadata for
   * the sort page before the start of the actual data.
   */
  // Header fields
  int max_tuple_count_;  // maximum number of tuples in the page
  int tuple_count_;      // number of tuples in the page
  int tuple_data_size_;  // size including serialization metadata
  char tuple_data_[];    // flexible array member for tuple storage
};

/**
 * A data structure that holds the sorted tuples as a run during external merge sort.
 * Tuples might be stored in multiple pages, and tuples are ordered both within one page
 * and across pages.
 */
class MergeSortRun {
 public:
  MergeSortRun() = default;
  MergeSortRun(std::vector<page_id_t> pages, BufferPoolManager *bpm) : pages_(std::move(pages)), bpm_(bpm) {}

  MergeSortRun(MergeSortRun &&other) noexcept : pages_(std::move(other.pages_)), bpm_(other.bpm_) {
    other.pages_.clear();  // Clear the moved-from object's pages
    other.bpm_ = nullptr;  // Clear the moved-from object's bpm
  }

  ~MergeSortRun() {
    if (bpm_ != nullptr) {
      for (const auto &page_id : pages_) {
        bpm_->DeletePage(page_id);
      }
    }
  }

  auto GetPageCount() const -> size_t { return pages_.size(); }

  /** Iterator for iterating on the sorted tuples in one run. */
  class Iterator {
    friend class MergeSortRun;

   public:
    Iterator() = default;
    /**
     * Advance the iterator to the next tuple. If the current sort page is exhausted, move to the
     * next sort page.
     */
    auto operator++() -> Iterator & {
      BUSTUB_ASSERT(page_index_ >= 0 && page_index_ < page_count_, "[Iterator:operator++] Page index out of bounds");
      BUSTUB_ASSERT(tuple_index_ >= 0 && tuple_index_ < read_guard_.template As<SortPage>()->GetTupleCount(),
                    "[Iterator:operator++] Tuple index out of bounds");
      BUSTUB_ASSERT(read_guard_.IsValid(), "[Iterator:operator++] Read guard is invalid");

      tuple_index_++;
      auto sort_page = read_guard_.template As<SortPage>();

      // If the current page is exhausted, move to the next page
      if (tuple_index_ >= sort_page->GetTupleCount()) {
        tuple_index_ = 0;
        read_guard_.Drop();
        run_->bpm_->DeletePage(run_->pages_[page_index_]);
        page_index_++;
        // If there are no more pages, return the end iterator
        if (page_index_ >= page_count_) {
          return *this;
        }
        read_guard_ = run_->bpm_->ReadPage(run_->pages_[page_index_]);
        BUSTUB_ASSERT(read_guard_.IsValid(), "[Iterator:operator++] Read guard is invalid");
      }
      return *this;
    }

    /**
     * Dereference the iterator to get the current tuple in the sorted run that the iterator is
     * pointing to.
     */
    auto operator*() const -> Tuple {
      BUSTUB_ASSERT(page_index_ >= 0 && page_index_ < page_count_, "[Iterator:operator*] Page index out of bounds");
      BUSTUB_ASSERT(read_guard_.IsValid(), "[Iterator:operator*] Read guard is invalid");
      auto sort_page = read_guard_.template As<SortPage>();
      BUSTUB_ASSERT(tuple_index_ >= 0 && tuple_index_ < sort_page->GetTupleCount(),
                    "[Iterator:operator*] Tuple index out of bounds");
      Tuple tuple;
      sort_page->ReadTuple(tuple, tuple_index_);
      return tuple;
    }

    /**
     * Checks whether two iterators are pointing to the same tuple in the same sorted run.
     */
    auto operator==(const Iterator &other) const -> bool {
      return run_ == other.run_ && page_index_ == other.page_index_ && tuple_index_ == other.tuple_index_;
    }

    /**
     * Checks whether two iterators are pointing to different tuples in a sorted run or iterating
     * on different sorted runs.
     */
    auto operator!=(const Iterator &other) const -> bool { return !(*this == other); }

   private:
    enum class IteratorType { Begin, End };

    explicit Iterator(const MergeSortRun *run, IteratorType type) : run_(run) {
      page_count_ = run->GetPageCount();
      tuple_index_ = 0;
      page_index_ = type == IteratorType::Begin ? 0 : page_count_;
      if (page_count_ > 0 && type == IteratorType::Begin) {
        read_guard_ = run->bpm_->ReadPage(run->pages_[0]);
        BUSTUB_ASSERT(read_guard_.IsValid(), "[Iterator:Iterator] Read guard is invalid");
      }
    }

    /** The sorted run that the iterator is iterating on. */
    [[maybe_unused]] const MergeSortRun *run_;

    /**
     * TODO(P3): Add your own private members here. You may want something to record your current
     * position in the sorted run. Also feel free to add additional constructors to initialize
     * your private members.
     */
    int page_count_{0};
    int page_index_{-1};          // index of the current page in the run
    int tuple_index_{-1};         // index of the current tuple in the current page
    ReadPageGuard read_guard_{};  // guard for the current page
  };

  /**
   * Get an iterator pointing to the beginning of the sorted run, i.e. the first tuple.
   */
  auto Begin() const -> Iterator { return Iterator(this, Iterator::IteratorType::Begin); }

  /**
   * Get an iterator pointing to the end of the sorted run, i.e. the position after the last tuple.
   */
  auto End() const -> Iterator { return Iterator(this, Iterator::IteratorType::End); }

 private:
  /** The page IDs of the sort pages that store the sorted tuples. */
  std::vector<page_id_t> pages_;
  /**
   * The buffer pool manager used to read sort pages. The buffer pool manager is responsible for
   * deleting the sort pages when they are no longer needed.
   */
  [[maybe_unused]] BufferPoolManager *bpm_;
};

/**
 * ExternalMergeSortExecutor executes an external merge sort.
 *
 * In Spring 2025, only 2-way external merge sort is required.
 */
template <size_t K>
class ExternalMergeSortExecutor : public AbstractExecutor {
 public:
  ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                            std::unique_ptr<AbstractExecutor> &&child_executor);

  void Init() override;

  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the external merge sort */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  struct RunIterator {
    std::shared_ptr<MergeSortRun::Iterator> iter_;
    const MergeSortRun *run_;
  };

  /** Merge K runs at a time */
  auto MergeRuns(const std::vector<MergeSortRun> &input_runs) -> MergeSortRun;

  /** The sort plan node to be executed */
  const SortPlanNode *plan_;

  /** Compares tuples based on the order-bys */
  TupleComparator cmp_;

  /** TODO(P3): You will want to add your own private members here. */
  /** The child executor */
  std::unique_ptr<AbstractExecutor> child_executor_;

  /** The buffer pool manager */
  BufferPoolManager *bpm_;

  /** The current set of runs */
  std::vector<MergeSortRun> runs_;

  /** The iterator for the current run */
  MergeSortRun::Iterator current_iterator_;

  /** Whether Init() has been called */
  bool is_initialized_{false};
};

}  // namespace bustub
