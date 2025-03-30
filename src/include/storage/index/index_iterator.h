//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_iterator.h
//
// Identification: src/include/storage/index/index_iterator.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include <utility>
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables

  // Constructor that takes pointer to bpm, leaf page id, and index
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;
  IndexIterator();
  IndexIterator(BufferPoolManager *bpm, page_id_t leaf_page_id, int index);
  IndexIterator(const IndexIterator &) = delete;
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> std::pair<const KeyType &, const ValueType &>;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    return bpm_ == itr.bpm_ && leaf_page_id_ == itr.leaf_page_id_ && index_ == itr.index_;
  }

  auto operator!=(const IndexIterator &itr) const -> bool { return !(*this == itr); }

 private:
  // add your own private member variables here
  BufferPoolManager *bpm_{nullptr};
  page_id_t leaf_page_id_{INVALID_PAGE_ID};
  int index_{-1};
  ReadPageGuard read_guard_;
};

}  // namespace bustub
