//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_leaf_page.cpp
//
// Identification: src/storage/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/b_plus_tree_leaf_page.h"
#include <algorithm>
#include "common/exception.h"
#include "common/macros.h"
#include "common/rid.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * @brief Init method after creating a new leaf page
 *
 * After creating a new leaf page from buffer pool, must call initialize method to set default values,
 * including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size.
 *
 * @param max_size Max size of the leaf node
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetMaxSize(max_size);
  SetSize(0);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index" (a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "Invalid index");
  return key_array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAtRef(int index) const -> const KeyType & {
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "Invalid index");
  return key_array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "Invalid index");
  key_array_[index] = key;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "Invalid index");
  return rid_array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAtRef(int index) const -> const ValueType & {
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "Invalid index");
  return rid_array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "Invalid index");
  rid_array_[index] = value;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::FindGreaterEqual(const KeyType &key, const KeyComparator &comparator) const -> int {
  // Adapt comparator to return bool as required by lower_bound
  auto comp = [&comparator](const KeyType &key1, const KeyType &key2) { return comparator(key1, key2) < 0; };

  int index = std::lower_bound(key_array_, key_array_ + GetSize(), key, comp) - key_array_;
  BUSTUB_ASSERT(index >= 0 && index <= GetSize(), "[LeafPage:FindGreaterEqual] Invalid index");
  BUSTUB_ASSERT(index == GetSize() || comparator(key_array_[index], key) >= 0,
                "[LeafPage:FindGreaterEqual] Key not found");
  return index;
}

/**
 * @brief Check if the key exists in the leaf page
 *
 * @param key
 * @param comparator
 * @return true if the key exists, false otherwise
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyExists(const KeyType &key, const KeyComparator &comparator) const -> bool {
  // Adapt comparator to return bool as required by binary_search
  auto comp = [&comparator](const KeyType &key1, const KeyType &key2) { return comparator(key1, key2) < 0; };
  return std::binary_search(key_array_, key_array_ + GetSize(), key, comp);
}

/**
 * @brief Lookup a key in the leaf page
 *
 * @param key
 * @param comparator
 * @return The value associated with the key, or std::nullopt if the key does not exist
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const
    -> std::optional<ValueType> {
  // Find index of the key greater than or equal to the given key
  int index = FindGreaterEqual(key, comparator);
  // printf("[LeafPage:Lookup] index: %d\n", index);

  // If the key exists, return the value
  if (index < GetSize() && comparator(key_array_[index], key) == 0) {
    return ValueAt(index);
  }

  // Otherwise, return std::nullopt
  return std::nullopt;
}

/*****************************************************************************
 * INSERTION HELPER METHODS
 *****************************************************************************/

/**
 * Insert a key-value pair into the leaf page. Only works when the leaf page is safe for insertion.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SafeInsert(const KeyType &key, const ValueType &value,
                                            const KeyComparator &comparator) {
  BUSTUB_ASSERT(IsInsertSafe(), "[LeafPage:SafeInsert] Called on an unsafe leaf page");
  BUSTUB_ASSERT(!KeyExists(key, comparator),
                "[LeafPage:SafeInsert] Key already exists in the leaf page, check KeyExists");

  // Find the position to insert the key, pos -> [key_array_, key_array_ + GetSize())
  int insert_index = FindGreaterEqual(key, comparator);
  // printf("[LeafPage:SafeInsert] insert_index: %d\n", insert_index);

  // Shift the keys and values to the right to make space for the new key-value pair
  std::move_backward(key_array_ + insert_index, key_array_ + GetSize(), key_array_ + GetSize() + 1);
  std::move_backward(rid_array_ + insert_index, rid_array_ + GetSize(), rid_array_ + GetSize() + 1);

  // Insert the key-value pair
  key_array_[insert_index] = key;
  rid_array_[insert_index] = value;
  ChangeSizeBy(1);
}

/**
 * @brief Distribute the current leaf page's entries with a new key-value pair
 * between itself and the recipient page
 *
 * @param recipient The recipient leaf page
 * @param key The new key to insert
 * @param value The new value to insert
 * @param comparator The key comparator
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Distribute(BPlusTreeLeafPage *recipient, const KeyType &key, const ValueType &value,
                                            const KeyComparator &comparator) {
  BUSTUB_ASSERT(!IsInsertSafe(), "[LeafPage:Distribute] Called on an safe leaf page");
  BUSTUB_ASSERT(recipient->GetSize() == 0, "[LeafPage:Distribute] Recipient page should be empty");

  int insert_index = FindGreaterEqual(key, comparator);
  const int copy_size = GetMinSize();
  const int recipient_size = GetSize() + 1 - copy_size;
  // printf("[LeafPage:Distribute] insert_index: %d, copy_size: %d, recipient_size: %d\n", insert_index, copy_size,
  //        recipient_size);
  BUSTUB_ASSERT(insert_index >= 0 && insert_index <= GetSize(), "[LeafPage:Distribute] Invalid insert index");

  // Create temporary arrays to store keys and values
  std::vector<KeyType> temp_keys(GetSize() + 1);
  std::vector<ValueType> temp_values(GetSize() + 1);

  // Move elements: [0: insert_index) -> [0: insert_index)
  std::move(key_array_, key_array_ + insert_index, temp_keys.begin());
  std::move(rid_array_, rid_array_ + insert_index, temp_values.begin());

  // Insert the new key-value pair
  temp_keys[insert_index] = key;
  temp_values[insert_index] = value;

  // Move elements: [insert_index: GetSize()) -> [insert_index + 1: GetSize() + 1)
  std::move(key_array_ + insert_index, key_array_ + GetSize(), temp_keys.begin() + insert_index + 1);
  std::move(rid_array_ + insert_index, rid_array_ + GetSize(), temp_values.begin() + insert_index + 1);

  // Now distribute between the two pages
  // Move elements: [0: copy_size) -> [0: copy_size)
  std::move(temp_keys.begin(), temp_keys.begin() + copy_size, key_array_);
  std::move(temp_values.begin(), temp_values.begin() + copy_size, rid_array_);

  // Move elements: [copy_size: GetMaxSize()) -> [0: GetMaxSize() - copy_size)
  std::move(temp_keys.begin() + copy_size, temp_keys.end(), recipient->key_array_);
  std::move(temp_values.begin() + copy_size, temp_values.end(), recipient->rid_array_);

  // Update sizes
  SetSize(copy_size);
  recipient->SetSize(recipient_size);
}

/*****************************************************************************
 * REMOVE HELPER METHODS
 *****************************************************************************/

/**
 * @brief Remove a key-value pair from the leaf page
 *
 * @param key
 * @param comparator
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key, const KeyComparator &comparator) {
  BUSTUB_ASSERT(KeyExists(key, comparator), "[LeafPage:Remove] Key not found");

  // Find index of the key to remove
  int index = FindGreaterEqual(key, comparator);
  // printf("[LeafPage:Remove] index: %d\n", index);
  BUSTUB_ASSERT(index < GetSize() && comparator(key_array_[index], key) == 0, "[LeafPage:Remove] Key not found");

  // Shift the keys and values to the left to fill the gap
  std::move(key_array_ + index + 1, key_array_ + GetSize(), key_array_ + index);
  std::move(rid_array_ + index + 1, rid_array_ + GetSize(), rid_array_ + index);
  ChangeSizeBy(-1);
}

/**
 * @brief Merge the current leaf page with an unfull successor neighbor leaf page
 *
 * @param neighbor The neighbor leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Merge(BPlusTreeLeafPage *neighbor, const KeyComparator &comparator) {
  BUSTUB_ASSERT(GetSize() + neighbor->GetSize() <= GetMaxSize(),
                "[LeafPage:Merge] Merge size should not exceed max size");
  // Neighbor is Successor
  // Move all elements from recipient to current page
  std::move(neighbor->key_array_, neighbor->key_array_ + neighbor->GetSize(), key_array_ + GetSize());
  std::move(neighbor->rid_array_, neighbor->rid_array_ + neighbor->GetSize(), rid_array_ + GetSize());

  // Set next page id to what neighbor's next page id was
  SetNextPageId(neighbor->GetNextPageId());

  // Set neighbor's next page id to INVALID_PAGE_ID, it will be deleted
  neighbor->SetNextPageId(INVALID_PAGE_ID);

  // Update size
  SetSize(GetSize() + neighbor->GetSize());
  neighbor->SetSize(0);
}

/**
 * @brief Redistribute the neighbor leaf page's entries with a current leaf page
 *
 * @param neighbor The neighbor leaf page
 * @param neighbor_type The neighbor type
 * @param comparator The key comparator
 *
 * @return The key that will be moved to the parent page
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Redistribute(BPlusTreeLeafPage *neighbor, NeighborType neighbor_type,
                                              const KeyComparator &comparator) -> KeyType {
  BUSTUB_ASSERT(GetSize() == GetMinSize() - 1, "[LeafPage:Redistribute] Current page should 1 less than min size");
  BUSTUB_ASSERT(neighbor->GetSize() >= GetMinSize() + 1,
                "[LeafPage:Redistribute] Neighbor page should be at least 1 more than min size");

  if (neighbor_type == NeighborType::PREDECESSOR) {
    BUSTUB_ASSERT(GetSize() == 0 || comparator(neighbor->KeyAt(neighbor->GetSize() - 1), KeyAt(0)) < 0,
                  "[LeafPage:Redistribute] Predecessor page keys should be ordered before successor page keys");

    // Redistribute from predecessor

    // Get last key and value from neighbor and remove it from neighbor
    KeyType last_key = neighbor->KeyAt(neighbor->GetSize() - 1);
    ValueType last_value = neighbor->ValueAt(neighbor->GetSize() - 1);

    // Remove last key and value from neighbor
    neighbor->ChangeSizeBy(-1);

    // Shift current page keys and values to the right to make space for the new key-value pair
    std::move_backward(key_array_, key_array_ + GetSize(), key_array_ + GetSize() + 1);
    std::move_backward(rid_array_, rid_array_ + GetSize(), rid_array_ + GetSize() + 1);

    // Insert last key and value into the first position of current page
    key_array_[0] = last_key;
    rid_array_[0] = last_value;
    ChangeSizeBy(1);

    // Return first key of current page
    return KeyAt(0);
  }

  // Redistribute from successor

  // Get first key and value from neighbor
  KeyType first_key = neighbor->KeyAt(0);
  ValueType first_value = neighbor->ValueAt(0);

  // Remove first key and value from neighbor by shifting elements in the neighbor page to the left
  std::move(neighbor->key_array_ + 1, neighbor->key_array_ + neighbor->GetSize(), neighbor->key_array_);
  std::move(neighbor->rid_array_ + 1, neighbor->rid_array_ + neighbor->GetSize(), neighbor->rid_array_);
  neighbor->ChangeSizeBy(-1);

  // Insert first key and value into last position of current page
  ChangeSizeBy(1);
  key_array_[GetSize() - 1] = first_key;
  rid_array_[GetSize() - 1] = first_value;

  // Return first key of the neighbor page
  return neighbor->KeyAt(0);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
