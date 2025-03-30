//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_internal_page.cpp
//
// Identification: src/storage/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/b_plus_tree_internal_page.h"
#include <algorithm>
#include <utility>
#include "common/macros.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * @brief Init method after creating a new internal page.
 *
 * Writes the necessary header information to a newly created page,
 * including set page type, set current size, set page id, set parent id and set max page size,
 * must be called after the creation of a new page to make a valid BPlusTreeInternalPage.
 *
 * @param max_size Maximal size of the page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetMaxSize(max_size);
  SetSize(0);
}

/**
 * @brief Helper method to get/set the key associated with input "index"(a.k.a
 * array offset).
 *
 * @param index The index of the key to get. Index must be non-zero.
 * @return Key at index
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  BUSTUB_ASSERT(index >= 1 && index < GetSize(), "[InternalPage:KeyAt] Invalid index");
  return key_array_[index];
}

/**
 * @brief Set key at the specified index.
 *
 * @param index The index of the key to set. Index must be non-zero.
 * @param key The new value for key
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  BUSTUB_ASSERT(index >= 1 && index < GetSize(), "[InternalPage:SetKeyAt] Invalid index");
  key_array_[index] = key;
}

/**
 * @brief Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 *
 * @param index The index of the value to get.
 * @return Value at index
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "[InternalPage:ValueAt] Invalid index");
  return page_id_array_[index];
}

/**
 * @param value The value to search for
 * @return The index that corresponds to the specified value or -1 if not found
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  for (int i = 0; i < GetSize(); i++) {
    if (ValueAt(i) == value) {
      return i;
    }
  }
  return -1;
}

/**
 * @brief Set the value at the specified index.
 *
 * @param index The index of the value to set.
 * @param value The new value for value
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "[InternalPage:SetValueAt] Invalid index");
  page_id_array_[index] = value;
}

/**
 * @brief Find the index of the first key that is greater than or equal to the given key.
 *
 * @param key
 * @param comparator
 * @return Index of the first key that is greater than or equal to the given key
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindGreaterEqual(const KeyType &key, const KeyComparator &comparator) const
    -> int {
  // Adapt comparator to return bool as required by lower_bound
  auto comp = [&comparator](const KeyType &key1, const KeyType &key2) { return comparator(key1, key2) < 0; };
  int index = std::lower_bound(key_array_ + 1, key_array_ + GetSize(), key, comp) - key_array_;
  BUSTUB_ASSERT(index >= 1 && index <= GetSize(), "[InternalPage:FindGreaterEqual] Invalid index");
  BUSTUB_ASSERT(index == GetSize() || comparator(key_array_[index], key) >= 0,
                "[InternalPage:FindGreaterEqual] Key not found");
  return index;
}

/**
 * @brief Check if the key exists in the internal page
 *
 * @param key
 * @param comparator
 * @return True if the key exists, false otherwise
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyExists(const KeyType &key, const KeyComparator &comparator) const -> bool {
  // Adapt comparator to return bool as required by binary_search
  auto comp = [&comparator](const KeyType &key1, const KeyType &key2) { return comparator(key1, key2) < 0; };
  return std::binary_search(key_array_ + 1, key_array_ + GetSize(), key, comp);
}

/**
 * @brief Find the value of the next page to traverse to to reach the internal page that might contain the given key.
 *
 * @param key
 * @param comparator
 * @return Value at index
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const -> ValueType {
  // Find the first key that is greater than or equal to the given key
  int index = FindGreaterEqual(key, comparator);
  // If the key is greater than all keys in the page, or the key is less than the key at the index,
  // then decrement the index
  if (index == GetSize() || comparator(key, key_array_[index]) < 0) {
    index--;
  }
  // printf("[InternalPage:Lookup] index: %d\n", index);
  return ValueAt(index);
}

/*****************************************************************************
 * INSERTION HELPER METHODS
 *****************************************************************************/

/**
 * @brief Insert a key-value pair into the internal page. Assumes that the page is safe for insertion.
 *
 * @param insert_key
 * @param insert_value
 * @param comparator
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SafeInsert(const KeyType &key, const ValueType &value,
                                                const KeyComparator &comparator) {
  BUSTUB_ASSERT(IsInsertSafe(), "[InternalPage:SafeInsert] !IsInsertSafe()");

  // Find position to insert, only searching valid keys (index 1 onwards)
  int insert_pos = FindGreaterEqual(key, comparator);
  // printf("[InternalPage:SafeInsert] insert_pos: %d\n", insert_pos);

  // Shift existing entries to make space, starting from valid keys only
  std::move_backward(key_array_ + insert_pos, key_array_ + GetSize(), key_array_ + GetSize() + 1);
  std::move_backward(page_id_array_ + insert_pos, page_id_array_ + GetSize(), page_id_array_ + GetSize() + 1);

  // Insert the new key and value
  key_array_[insert_pos] = key;
  page_id_array_[insert_pos] = value;

  // Increase size
  ChangeSizeBy(1);
}

/**
 * @brief Distribute the current internal page's entries with a new key-value pair
 * between itself and the recipient page
 *
 * @param recipient The recipient internal page
 * @param key The new key to insert
 * @param value The new value to insert
 * @param comparator The key comparator
 * @return The key to be pushed up
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Distribute(BPlusTreeInternalPage *recipient, const KeyType &key,
                                                const ValueType &value, const KeyComparator &comparator) -> KeyType {
  // Pre-conditions
  BUSTUB_ASSERT(!IsInsertSafe(), "[InternalPage:Distribute] IsInsertSafe()");
  BUSTUB_ASSERT(GetSize() == GetMaxSize(), "[InternalPage:Distribute] GetSize() != GetMaxSize()");
  BUSTUB_ASSERT(recipient->GetSize() == 0, "[InternalPage:Distribute] Recipient page should be empty");

  // Find insertion position (skip invalid first key)
  int insert_pos = FindGreaterEqual(key, comparator);

  // Create temporary arrays to hold all entries including the new one
  std::vector<KeyType> temp_keys(key_array_, key_array_ + GetSize());
  std::vector<ValueType> temp_values(page_id_array_, page_id_array_ + GetSize());

  // Insert the new entry into temporary arrays
  temp_keys.insert(temp_keys.begin() + insert_pos, key);
  temp_values.insert(temp_values.begin() + insert_pos, value);

  // Calculate split point after insertion (n + 1 is the total number of entries after insertion)
  const int total_entries = GetSize() + 1;
  const int split_pos = 1 + (total_entries - 1) / 2;  // Ceiling of (n+1)/2
  KeyType split_key = temp_keys[split_pos];

  // Copy entries back to original page (left half)
  // Include the pointer at split_pos but not the key
  std::move(temp_keys.begin(), temp_keys.begin() + split_pos, key_array_);
  std::move(temp_values.begin(), temp_values.begin() + split_pos, page_id_array_);

  // Copy entries to recipient page (right half)
  // First valid key in recipient starts at index 1
  std::move(temp_keys.begin() + split_pos + 1, temp_keys.end(), recipient->key_array_ + 1);
  std::move(temp_values.begin() + split_pos, temp_values.end(), recipient->page_id_array_);

  // Update sizes
  SetSize(split_pos);
  recipient->SetSize(total_entries - split_pos);

  // printf("[InternalPage:Distribute] Left page size: %d, Right page size: %d\n", GetSize(), recipient->GetSize());

  // Post-condition checks
  BUSTUB_ASSERT(GetSize() >= GetMinSize(), "[InternalPage:Distribute] Left page violates minimum size");
  BUSTUB_ASSERT(recipient->GetSize() >= GetMinSize(), "[InternalPage:Distribute] Right page violates minimum size");

  // Verify ordering
  BUSTUB_ASSERT(GetSize() > 1 && comparator(key_array_[GetSize() - 1], split_key) < 0,
                "[InternalPage:Distribute] Split key not greater than all left keys");
  BUSTUB_ASSERT(comparator(split_key, recipient->key_array_[1]) < 0,
                "[InternalPage:Distribute] Split key not less than all right keys");

  return split_key;
}

/*****************************************************************************
 * REMOVE HELPER METHODS
 *****************************************************************************/

/**
 * @brief Get the previous and next page id for the given key and the separator key
 *
 * @param key
 * @param comparator
 * @return Pair of previous and next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetNeighborInfo(const KeyType &key, const KeyComparator &comparator) const
    -> std::pair<NeighborInfo, NeighborInfo> {
  // Find the index where the key belongs
  int index = FindGreaterEqual(key, comparator);
  if (index == GetSize() || comparator(key, key_array_[index]) < 0) {
    index--;
  }
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "[InternalPage:GetPrevAndNextPageId] Invalid index");

  // Get prev info
  NeighborInfo prev;
  if (index > 0) {
    prev.value_ = ValueAt(index - 1);
    prev.separator_key_ = KeyAt(index);
    // printf("[InternalPage:GetNeighborInfo] prev.value at index %d, prev.separator_key at index %d\n", index - 1,
    //        index);
  }

  // Get next info
  NeighborInfo next;
  if (index < GetSize() - 1) {
    next.value_ = ValueAt(index + 1);
    next.separator_key_ = KeyAt(index + 1);
    // printf("[InternalPage:GetNeighborInfo] next.value at index %d, next.separator_key at index %d\n", index + 1,
    //        index + 1);
  }

  return {prev, next};
}

/**
 * @brief Replace a key in the internal page with a new key
 *
 * @param old_key
 * @param new_key
 * @param comparator
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::ReplaceKey(const KeyType &old_key, const KeyType &new_key,
                                                const KeyComparator &comparator) {
  BUSTUB_ASSERT(KeyExists(old_key, comparator), "[InternalPage:ReplaceKey] Old key not found");
  BUSTUB_ASSERT(!KeyExists(new_key, comparator), "[InternalPage:ReplaceKey] New key already exists");

  // Find the index of the old key
  int index = FindGreaterEqual(old_key, comparator);
  BUSTUB_ASSERT(index < GetSize() && comparator(key_array_[index], old_key) == 0,
                "[InternalPage:ReplaceKey] Key not found");

  // Replace the key
  key_array_[index] = new_key;
}

/**
 * @brief Remove a key-value pair from the internal page
 *
 * @param key
 * @param comparator
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(const KeyType &key, const KeyComparator &comparator) {
  // Should exist
  BUSTUB_ASSERT(KeyExists(key, comparator), "[InternalPage:Remove] Key not found");

  // Find index of the key to remove
  int index = FindGreaterEqual(key, comparator);
  BUSTUB_ASSERT(index < GetSize() && comparator(key_array_[index], key) == 0, "[InternalPage:Remove] Key not found");

  // Shift the keys and values to the left to fill the gap
  std::move(key_array_ + index + 1, key_array_ + GetSize(), key_array_ + index);
  std::move(page_id_array_ + index + 1, page_id_array_ + GetSize(), page_id_array_ + index);
  ChangeSizeBy(-1);
}

/**
 * @brief Merge the current internal page with an successor neighbor internal page
 *
 * @param neighbor The neighbor internal page
 * @param separator_key The key to be used as the separator
 * @param comparator The key comparator
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Merge(BPlusTreeInternalPage *neighbor, const KeyType &separator_key,
                                           const KeyComparator &comparator) {
  BUSTUB_ASSERT(GetSize() + neighbor->GetSize() <= GetMaxSize(),
                "[InternalPage:Merge] Merge size should not exceed max size");
  BUSTUB_ASSERT(GetSize() <= 1 || comparator(separator_key, KeyAt(GetSize() - 1)) > 0,
                "[InternalPage:Merge] Successor page keys should be ordered after predecessor page keys");
  BUSTUB_ASSERT(neighbor->GetSize() <= 1 || comparator(separator_key, neighbor->KeyAt(1)) < 0,
                "[InternalPage:Merge] Successor page keys should be ordered after predecessor page keys");

  // Insert the separator key into the current page
  key_array_[GetSize()] = separator_key;

  // Move all elements from recipient to current page
  std::move(neighbor->key_array_ + 1, neighbor->key_array_ + neighbor->GetSize(), key_array_ + GetSize() + 1);
  std::move(neighbor->page_id_array_, neighbor->page_id_array_ + neighbor->GetSize(), page_id_array_ + GetSize());

  // Update size
  ChangeSizeBy(neighbor->GetSize());
  neighbor->SetSize(0);
}

/**
 * @brief Redistribute the current internal page's entries with a new key-value pair
 * between itself and the recipient page
 *
 * @param neighbor The recipient internal page
 * @param separator_key The key to be used as the separator
 * @param comparator The key comparator
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Redistribute(BPlusTreeInternalPage *neighbor, const KeyType &separator_key,
                                                  NeighborType neighbor_type, const KeyComparator &comparator)
    -> KeyType {
  BUSTUB_ASSERT(GetSize() == GetMinSize() - 1, "[InternalPage:Redistribute] Current page should have min size - 1");
  BUSTUB_ASSERT(neighbor->GetSize() >= GetMinSize() + 1,
                "[InternalPage:Redistribute] Neighbor page should have min size + 1");

  // Redistribute from predecessor
  if (neighbor_type == NeighborType::PREDECESSOR) {
    BUSTUB_ASSERT(neighbor->GetSize() <= 1 || comparator(neighbor->KeyAt(neighbor->GetSize() - 1), separator_key) < 0,
                  "[InternalPage:Redistribute] Predecessor page keys should be ordered before successor page keys");
    BUSTUB_ASSERT(GetSize() <= 1 || comparator(separator_key, KeyAt(1)) < 0,
                  "[InternalPage:Redistribute] Successor page keys should be ordered after predecessor page keys");

    // Get last key and value from neighbor and remove it from neighbor
    KeyType last_key = neighbor->KeyAt(neighbor->GetSize() - 1);
    ValueType last_value = neighbor->ValueAt(neighbor->GetSize() - 1);

    // Remove last key and value from neighbor
    neighbor->ChangeSizeBy(-1);

    // Shift current page keys and values to the right to make space for the new key-value pair
    std::move_backward(key_array_, key_array_ + GetSize(), key_array_ + GetSize() + 1);
    std::move_backward(page_id_array_, page_id_array_ + GetSize(), page_id_array_ + GetSize() + 1);

    // Insert last key and value into the first position of current page
    key_array_[1] = separator_key;
    page_id_array_[0] = last_value;
    ChangeSizeBy(1);
    return last_key;
  }

  BUSTUB_ASSERT(GetSize() <= 1 || comparator(KeyAt(GetSize() - 1), separator_key) < 0,
                "[InternalPage:Redistribute] Predecessor page keys should be ordered before successor page keys");
  BUSTUB_ASSERT(neighbor->GetSize() <= 1 || comparator(separator_key, neighbor->KeyAt(1)) < 0,
                "[InternalPage:Redistribute] Successor page keys should be ordered after predecessor page keys");

  // Redistribute from successor
  KeyType first_key = neighbor->KeyAt(1);
  ValueType first_value = neighbor->ValueAt(0);

  // Remove first key and value from neighbor by shifting elements in the neighbor page to the left
  std::move(neighbor->key_array_ + 1, neighbor->key_array_ + neighbor->GetSize(), neighbor->key_array_);
  std::move(neighbor->page_id_array_ + 1, neighbor->page_id_array_ + neighbor->GetSize(), neighbor->page_id_array_);
  neighbor->ChangeSizeBy(-1);

  key_array_[GetSize()] = separator_key;
  page_id_array_[GetSize()] = first_value;
  ChangeSizeBy(1);
  return first_key;
}
// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
