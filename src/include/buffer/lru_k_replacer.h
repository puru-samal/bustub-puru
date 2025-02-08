//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <mutex>  // NOLINT
#include <optional>
#include <set>
#include <unordered_map>
#include <vector>
#include "common/config.h"
#include "common/macros.h"

namespace bustub {

enum class AccessType { Unknown = 0, Lookup, Scan, Index };

class LRUKNode {
 public:
  // Constructor
  explicit LRUKNode(size_t k, frame_id_t fid);
  void RecordAccess(size_t timestamp);
  auto IsEvictable() const -> bool;
  void SetEvictable(bool is_evictable);
  auto GetEarliestTimestamp() const -> size_t;
  auto GetAccessCount() const -> size_t;
  auto GetFrameId() const -> frame_id_t;
  auto GetK() const -> size_t;
  void Print() const;

 private:
  /** History of last seen K timestamps of this page. */
  /** Implemented as a circular buffer. */
  /** Oldest timestamp is at write_ptr_ once the buffer is full. */
  std::vector<size_t> history_;
  size_t write_ptr_{0};
  size_t access_count_{0};
  size_t k_;
  frame_id_t fid_;
  bool is_evictable_{false};
  const size_t inf_ = std::numeric_limits<size_t>::max();
};

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */
class LRUKReplacer {
 public:
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer() = default;

  auto Evict() -> std::optional<frame_id_t>;
  void RecordAccess(frame_id_t frame_id, AccessType access_type = AccessType::Unknown);
  void SetEvictable(frame_id_t frame_id, bool set_evictable);
  void Remove(frame_id_t frame_id);
  auto Size() -> size_t;
  void PrintNodeStore() const;
  void PrintEvictableFrames() const;
  void PrintState() const;

 private:
  // TODO(student): implement me! You can replace these member variables as you like.
  // Remove maybe_unused if you start using them.

  // Comparator for the evictable frames to be used to order the frames in the set
  struct EvictableFrameComparator {
    const std::unordered_map<frame_id_t, LRUKNode> &node_store_;
    const size_t k_;
    explicit EvictableFrameComparator(const std::unordered_map<frame_id_t, LRUKNode> &node_store, size_t k)
        : node_store_(node_store), k_(k) {}
    auto operator()(const frame_id_t &a, const frame_id_t &b) const -> bool;
  };
  std::unordered_map<frame_id_t, LRUKNode> node_store_;
  std::set<frame_id_t, EvictableFrameComparator> evictable_frames_;
  size_t current_timestamp_{0};
  size_t curr_size_{0};
  size_t replacer_size_;
  size_t k_;
  mutable std::mutex latch_;
};

}  // namespace bustub
