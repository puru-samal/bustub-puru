//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// skiplist.cpp
//
// Identification: src/primer/skiplist.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/skiplist.h"
#include <cassert>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
#include "fmt/core.h"

namespace bustub {

/** @brief Checks whether the container is empty. */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::Empty() -> bool { return size_ == 0; }

/** @brief Returns the number of elements in the skip list. */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::Size() -> size_t { return size_; }

/**
 * @brief Iteratively deallocate all the nodes.
 *
 * We do this to avoid stack overflow when the skip list is large.
 *
 * If we let the compiler handle the deallocation, it will recursively call the destructor of each node,
 * which could block up the the stack.
 */
SKIPLIST_TEMPLATE_ARGUMENTS void SkipList<K, Compare, MaxHeight, Seed>::Drop() {
  for (size_t i = 0; i < MaxHeight; i++) {
    auto curr = std::move(header_->links_[i]);
    while (curr != nullptr) {
      // std::move sets `curr` to the old value of `curr->links_[i]`,
      // and then resets `curr->links_[i]` to `nullptr`.
      curr = std::move(curr->links_[i]);
    }
  }
}

/**
 * @brief Checks whether two keys are equal.
 *
 * @param key1 first key.
 * @param key2 second key.
 * @return bool true if the keys are equal, false otherwise.
 */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::KeyEqual(const K &key1, const K &key2) -> bool {
  return !compare_(key1, key2) && !compare_(key2, key1);
}

/**
 * @brief Finds the node that is no less than the given key.
 *
 * @param key the key to search for.
 * @param update optional vector to store the nodes that we need to update.
 *               If provided, it will be filled with update nodes.
 * @return the node that is no less than the given key.
 */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::FindNoLessThan(const K &key,
                                                                                       UpdateVector *update)
    -> std::shared_ptr<SkipNode> {
  // Start from the header
  auto curr = Header();

  // Iterate through each level of the skip list
  for (size_t i = LOWEST_LEVEL; i < height_; i++) {
    size_t level = height_ - 1 - i;

    // Traverse the list at the current level until we find a node
    // that is not less than the key at the current level
    while (curr->Next(level) != nullptr && compare_(curr->Next(level)->Key(), key)) {
      curr = curr->Next(level);
    }

    // If update is provided, store the current node at the current level
    if (update != nullptr) {
      (*update)[level] = curr;
    }
  }
  return curr;
}

/**
 * @brief Inserts a new node with the given key.
 *
 * @param key the key to insert.
 * @param node the node that is no less than the given key.
 * @param update the update vector to store the nodes that we need to update.
 * @return true if the node is inserted, false otherwise.
 */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::InsertNode(const K &key,
                                                                                   std::shared_ptr<SkipNode> node,
                                                                                   UpdateVector *update) -> bool {
  // check if the node is already in the skip list
  node = node->Next(LOWEST_LEVEL);
  if (node != nullptr && KeyEqual(node->Key(), key)) {
    return false;
  }

  // generate a random height for the new node
  size_t new_height = RandomHeight();

  // initialize update vector for any levels above current height
  for (size_t i = height_; i < new_height; i++) {
    (*update)[i] = Header();
  }
  height_ = std::max<size_t>(height_, new_height);

  // create a new node with the given key
  auto new_node = std::make_shared<SkipNode>(new_height, key);
  // update the update vector
  for (size_t i = LOWEST_LEVEL; i < new_height; i++) {
    new_node->SetNext(i, (*update)[i]->Next(i));
    (*update)[i]->SetNext(i, new_node);
  }
  size_++;
  return true;
}

/**
 * @brief Erases the node with the given key.
 *
 * @param key the key to erase.
 * @param node the node no less than the given key.
 * @param update the update vector to store the nodes that we need to update.
 * @return true if the node is erased, false otherwise.
 */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::EraseNode(const K &key,
                                                                                  std::shared_ptr<SkipNode> node,
                                                                                  UpdateVector *update) -> bool {
  // check if the node is in the skip list
  node = node->Next(LOWEST_LEVEL);
  if (node == nullptr || !KeyEqual(node->Key(), key)) {
    return false;
  }

  // erase the node
  for (size_t i = LOWEST_LEVEL; i < node->Height(); i++) {
    if ((*update)[i]->Next(i) == node) {
      (*update)[i]->SetNext(i, node->Next(i));
    }
  }
  node = nullptr;

  // update the height of the skip list
  while (height_ > LOWEST_LEVEL && Header()->Next(height_ - 1) == nullptr) {
    height_--;
  }

  // update the size of the skip list
  size_--;
  return true;
}

/**
 * @brief Removes all elements from the skip list.
 *
 * Note: You might want to use the provided `Drop` helper function.
 */
SKIPLIST_TEMPLATE_ARGUMENTS void SkipList<K, Compare, MaxHeight, Seed>::Clear() {
  Drop();
  size_ = 0;
  height_ = 1;
}

/**
 * @brief Inserts a key into the skip list.
 *
 * Note: `Insert` will not insert the key if it already exists in the skip list.
 *
 * @param key key to insert.
 * @return true if the insertion is successful, false if the key already exists.
 */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::Insert(const K &key) -> bool {
  // Acquire write lock
  std::unique_lock lock(rwlock_);

  // vector update to store the nodes that we need to update
  UpdateVector update(MaxHeight, nullptr);

  // find the node that is no less than the given key
  // insert a node with the given key
  return InsertNode(key, FindNoLessThan(key, &update), &update);
}

/**
 * @brief Erases the key from the skip list.
 *
 * @param key key to erase.
 * @return bool true if the element got erased, false otherwise.
 */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::Erase(const K &key) -> bool {
  // Acquire write lock
  std::unique_lock lock(rwlock_);

  // vector update to store the nodes that we need to update
  UpdateVector update(MaxHeight, nullptr);

  // find the node that is no less than the given key
  // erase the node with the given key
  return EraseNode(key, FindNoLessThan(key, &update), &update);
}

/**
 * @brief Checks whether a key exists in the skip list.
 *
 * @param key key to look up.
 * @return bool true if the element exists, false otherwise.
 */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::Contains(const K &key) -> bool {
  // Acquire read lock
  std::shared_lock lock(rwlock_);

  // find the node that is no less than the given key
  auto curr = FindNoLessThan(key, nullptr)->Next(LOWEST_LEVEL);

  // check if the key exists while still holding the lock
  return curr != nullptr && KeyEqual(curr->Key(), key);
}

/**
 * @brief Prints the skip list for debugging purposes.
 *
 * Note: You may modify the functions in any way and the output is not tested.
 */
SKIPLIST_TEMPLATE_ARGUMENTS void SkipList<K, Compare, MaxHeight, Seed>::Print() {
  // Print from top level to bottom
  for (size_t i = LOWEST_LEVEL; i < height_; i++) {
    size_t level = height_ - 1 - i;
    fmt::print("Level {}: ", level);
    auto curr = Header()->Next(level);
    while (curr != nullptr) {
      fmt::print("{} -> ", curr->Key());
      curr = curr->Next(level);
    }
    fmt::println("nullptr");
  }
  fmt::println("Size: {}", size_);
}

/**
 * @brief Generate a random height. The height should be cappped at `MaxHeight`.
 * Note: we implement/simulate the geometric process to ensure platform independence.
 */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::RandomHeight() -> size_t {
  // Branching factor (1 in 4 chance), see Pugh's paper.
  static constexpr unsigned int branching_factor = 4;
  // Start with the minimum height
  size_t height = 1;
  while (height < MaxHeight && (rng_() % branching_factor == 0)) {
    height++;
  }
  return height;
}

/**
 * @brief Gets the current node height.
 */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::SkipNode::Height() const -> size_t {
  return links_.size();
}

/**
 * @brief Gets the next node by following the link at `level`.
 *
 * @param level index to the link.
 * @return std::shared_ptr<SkipNode> the next node, or `nullptr` if such node does not exist.
 */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::SkipNode::Next(size_t level) const
    -> std::shared_ptr<SkipNode> {
  return links_[level];
}

/**
 * @brief Set the `node` to be linked at `level`.
 *
 * @param level index to the link.
 */
SKIPLIST_TEMPLATE_ARGUMENTS void SkipList<K, Compare, MaxHeight, Seed>::SkipNode::SetNext(
    size_t level, const std::shared_ptr<SkipNode> &node) {
  links_[level] = node;
}

/** @brief Returns a reference to the key stored in the node. */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::SkipNode::Key() const -> const K & {
  return key_;
}

// Below are explicit instantiation of template classes.
template class SkipList<int>;
template class SkipList<std::string>;
template class SkipList<int, std::greater<>>;
template class SkipList<int, std::less<>, 8>;

}  // namespace bustub
