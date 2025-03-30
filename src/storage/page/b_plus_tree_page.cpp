//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_page.cpp
//
// Identification: src/storage/page/b_plus_tree_page.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*
 * Helper methods to get/set page type
 * Page type enum class is defined in b_plus_tree_page.h
 */
auto BPlusTreePage::IsLeafPage() const -> bool { return page_type_ == IndexPageType::LEAF_PAGE; }
void BPlusTreePage::SetPageType(IndexPageType page_type) { page_type_ = page_type; }

/*
 * Helper methods to get/set size (number of key/value pairs stored in that
 * page)
 */
auto BPlusTreePage::GetSize() const -> int { return size_; }
void BPlusTreePage::SetSize(int size) { size_ = size; }
void BPlusTreePage::ChangeSizeBy(int amount) { size_ += amount; }

/*
 * Helper methods to get/set max size (capacity) of the page
 */
auto BPlusTreePage::GetMaxSize() const -> int { return max_size_; }
void BPlusTreePage::SetMaxSize(int size) { max_size_ = size; }

/*
 * Helper method to get min page size
 * Generally, min page size == max page size / 2
 * But whether you will take ceil() or floor() depends on your implementation
 */
auto BPlusTreePage::GetMinSize() const -> int {
  if (IsLeafPage()) {
    return max_size_ / 2;
  }
  return 1 + ((max_size_ - 1) / 2);
}

/**
 * @brief Check if the page is safe for insertion.
 * Leaf page: Split condition: size == max_size after insertion
 * Internal page: Split condition: size == max_size before insertion
 *
 * @return true if the page is safe for insertion, false otherwise
 */
auto BPlusTreePage::IsInsertSafe() const -> bool {
  if (IsLeafPage()) {
    return GetSize() + 1 < GetMaxSize();
  }
  return GetSize() < GetMaxSize();
}

/**
 * @brief Check if the page is safe for removal.
 * Leaf page: Merge condition: size == min_size before removal
 * Internal page: Merge condition: size == min_size before removal
 *
 * @return true if the page is safe for removal, false otherwise
 */
auto BPlusTreePage::IsRemoveSafe() const -> bool { return GetSize() > GetMinSize(); }

auto BPlusTreePage::TooFew() const -> bool { return GetSize() < GetMinSize(); }

auto BPlusTreePage::IsRootRemoveSafe() const -> bool {
  if (IsLeafPage()) {
    return GetSize() > 1;
  }
  return GetSize() > 2;
}

}  // namespace bustub
