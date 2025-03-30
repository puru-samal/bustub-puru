//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// skiplist.h
//
// Identification: src/include/primer/skiplist.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <random>
#include <shared_mutex>
#include <utility>
#include <vector>

namespace bustub {

/** @brief Common template arguments used for the `SkipList` class. */
#define SKIPLIST_TEMPLATE_ARGUMENTS template <typename K, typename Compare, size_t MaxHeight, uint32_t Seed>

/**
 * @brief A probablistic data structure that implements the ordered set abstract data type.
 *
 * The skip list is implemented as a linked list of nodes. Each node has a list of forward links. The number of
 * forward links is determined by a geometric distribution. The skip list maintains a header node that is always at the
 * maximum height of the skip list. The skip list is always sorted by the key using the comparison function provided.
 *
 * @tparam K the type of key.
 * @tparam Compare the comparison function for the key.
 * @tparam MaxHeight the maximum height of the skip list.
 * @tparam Seed the seed for the random number generator.
 */
template <typename K, typename Compare = std::less<K>, size_t MaxHeight = 14, uint32_t Seed = 15445>
class SkipList {
 protected:
  struct SkipNode;

 public:
  /**  @brief Constructs an empty skip list with an optional custom comparison function. */
  explicit SkipList(const Compare &compare = Compare{})
      : compare_(compare), header_(std::make_shared<SkipNode>(MaxHeight, std::numeric_limits<K>::min())) {}

  /**
   * @brief Destructs the skip list.
   *
   * See `Drop()` for how we avoid blowing up the stack.
   */
  ~SkipList() { Drop(); }

  /**
   * We disable both copy & move operators/constructors as the skip list contains a mutex
   * that cannot be safely moved/copied while other threads may be using it.
   */
  SkipList(const SkipList &) = delete;
  auto operator=(const SkipList &) -> SkipList & = delete;
  SkipList(SkipList &&) = delete;
  auto operator=(SkipList &&) -> SkipList & = delete;

  auto Empty() -> bool;
  auto Size() -> size_t;

  void Clear();
  auto Insert(const K &key) -> bool;
  auto Erase(const K &key) -> bool;
  auto Contains(const K &key) -> bool;

  void Print();

 protected:
  auto Header() -> std::shared_ptr<SkipNode> { return header_; }

 private:
  auto RandomHeight() -> size_t;

  void Drop();
  // Students may add any private helper functions that they desire.
  //
  // To give you an idea, the following are some common helper functions that you may want to implement:
  // - Finds the node that is no less than the given key.
  // - Inserts a new node with the given key.
  // - Adjust height and previous pointers.

  /**
   * @brief Helper function to check if two keys are equal.
   *
   * @param key1 the first key.
   * @param key2 the second key.
   * @return true if the keys are equal, false otherwise.
   */
  auto KeyEqual(const K &key1, const K &key2) -> bool;

  /** @brief Type alias for update vector to improve readability */
  using UpdateVector = std::vector<std::shared_ptr<SkipNode>>;

  /**
   * @brief Finds the node that is no less than the given key.
   *
   * @param key the key to search for.
   * @param update optional vector to store the nodes that we need to update.
   *               If provided, it will be filled with update nodes.
   * @return the node that is no less than the given key.
   */
  auto FindNoLessThan(const K &key, UpdateVector *update = nullptr) -> std::shared_ptr<SkipNode>;

  /**
   * @brief Inserts a new node with the given key.
   *
   * @param key the key to insert.
   * @param node the node that is no less than the given key.
   * @param update the update vector to store the nodes that we need to update.
   * @return true if the node is inserted, false otherwise.
   */
  auto InsertNode(const K &key, std::shared_ptr<SkipNode> node, UpdateVector *update) -> bool;

  /**
   * @brief Erases the node with the given key.
   *
   * @param key the key to erase.
   * @param node the node no less than the given key.
   * @param update the update vector to store the nodes that we need to update.
   * @return true if the node is erased, false otherwise.
   */
  auto EraseNode(const K &key, std::shared_ptr<SkipNode> node, UpdateVector *update) -> bool;

  /** @brief Lowest level index for the skip list. */
  static constexpr size_t LOWEST_LEVEL = 0;

  /** @brief Comparison function used to check key orders and keep the skip list sorted. */
  Compare compare_;

  /**
   * @brief Header consists of a list of forward links for level 0 to `MaxHeight - 1`. The forward links at
   * level higher than current `height_` are `nullptr`.
   */
  std::shared_ptr<SkipNode> header_;

  /**
   * @brief Current height of the skip list.
   *
   * Invariant: `height_` should never be greater than `MaxHeight`.
   */
  uint32_t height_{1};

  /** @brief Number of elements in the skip list. */
  size_t size_{0};

  /** @brief Random number generator. */
  std::mt19937 rng_{Seed};

  /** @brief A reader-writer latch protecting the skip list. */
  std::shared_mutex rwlock_{};
};

/**
 * Node type for `SkipList`.
 */
SKIPLIST_TEMPLATE_ARGUMENTS struct SkipList<K, Compare, MaxHeight, Seed>::SkipNode {
  /**
   * @brief Constructs a skip list node with height number of links and given key.
   *
   * This constructor is used both for creating regular nodes (with key) and the header node (without key).
   *
   * @param height The number of links the node will have
   * @param key The key to store in the node (default empty for header)
   */
  explicit SkipNode(size_t height, K key = K{}) : links_(height), key_(std::move(key)) {}

  auto Height() const -> size_t;
  auto Next(size_t level) const -> std::shared_ptr<SkipNode>;
  void SetNext(size_t level, const std::shared_ptr<SkipNode> &node);
  auto Key() const -> const K &;

  /**
   * @brief A list of forward links.
   *
   * Note: `links_[0]` is the lowest level link, and `links_[links_.size()-1]` is the highest level link.
   *
   * We use `std::shared_ptr` to manage the memory of the next node. This is because the next node can be shared by
   * multiple links. We also use `std::vector` instead of a flexible array member for simplicity instead of performance.
   */
  std::vector<std::shared_ptr<SkipNode>> links_;
  K key_;
};

}  // namespace bustub
