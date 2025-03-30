//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_iterator.cpp
//
// Identification: src/storage/index/index_iterator.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * index_iterator.cpp
 */
#include "storage/index/index_iterator.h"
#include <cassert>
#include <utility>

namespace bustub {

/**
 * @note you can change the destructor/constructor method here
 * set your own input parameters
 */

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, page_id_t leaf_page_id, int index)
    : bpm_(bpm), leaf_page_id_(leaf_page_id), index_(index) {
  if (leaf_page_id_ != INVALID_PAGE_ID) {
    read_guard_ = bpm_->ReadPage(leaf_page_id_);
    BUSTUB_ASSERT(read_guard_.IsValid(), "Read guard must be valid");
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (read_guard_.IsValid()) {
    read_guard_.Drop();
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  if (leaf_page_id_ == INVALID_PAGE_ID) {
    return true;
  }
  auto leaf_page = read_guard_.template As<LeafPage>();
  return leaf_page->GetNextPageId() == INVALID_PAGE_ID && index_ == leaf_page->GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> std::pair<const KeyType &, const ValueType &> {
  BUSTUB_ASSERT(leaf_page_id_ != INVALID_PAGE_ID, "[IndexIterator:operator*] Leaf page id is invalid");
  BUSTUB_ASSERT(read_guard_.IsValid(), "[IndexIterator:operator*] Read guard is invalid");
  BUSTUB_ASSERT(index_ >= 0 && index_ < read_guard_.template As<LeafPage>()->GetSize(),
                "[IndexIterator:operator*] Index is out of bounds");
  auto leaf_page = read_guard_.template As<LeafPage>();
  return {leaf_page->KeyAtRef(index_), leaf_page->ValueAtRef(index_)};
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  BUSTUB_ASSERT(leaf_page_id_ != INVALID_PAGE_ID, "[IndexIterator:operator++] Leaf page id is invalid");
  BUSTUB_ASSERT(read_guard_.IsValid(), "[IndexIterator:operator++] Read guard is invalid");
  auto leaf_page = read_guard_.template As<LeafPage>();
  BUSTUB_ASSERT(index_ >= 0 && index_ <= leaf_page->GetSize(), "[IndexIterator:operator++] Index is out of bounds");
  index_++;
  if (index_ >= leaf_page->GetSize()) {
    auto next_page_id = leaf_page->GetNextPageId();
    if (next_page_id == INVALID_PAGE_ID) {
      return *this;
    }
    leaf_page_id_ = next_page_id;
    read_guard_.Drop();
    read_guard_ = bpm_->ReadPage(leaf_page_id_);
    BUSTUB_ASSERT(read_guard_.IsValid(), "[IndexIterator:operator++] Read guard is invalid");
    index_ = 0;
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
