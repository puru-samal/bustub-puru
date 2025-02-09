//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <mutex>  // NOLINT
#include "common/macros.h"

namespace bustub {

/**
 * @brief Constructor for LRUKNode.
 * @param k the number of timestamps to store
 * @param fid the frame id
 */
LRUKNode::LRUKNode(size_t k, frame_id_t fid) : k_(k), fid_(fid) { history_.resize(k); }

/**
 * @brief Record the access of a frame.
 * @param timestamp the timestamp of the access
 */
void LRUKNode::RecordAccess(size_t timestamp, [[maybe_unused]] AccessType access_type) {
  // Update the history
  // Update the write pointer
  // Increment the access count
  history_[write_ptr_] = timestamp;
  write_ptr_ = (write_ptr_ + 1) % k_;
  access_count_++;
  last_access_type_ = access_type;
}

/**
 * @brief Check if the frame is evictable.
 * @return true if the frame is evictable, false otherwise
 */
auto LRUKNode::IsEvictable() const -> bool { return is_evictable_; }

/**
 * @brief Set the evictable status of the frame.
 * @param is_evictable the new evictable status of the frame
 */
void LRUKNode::SetEvictable(bool is_evictable) { is_evictable_ = is_evictable; }

/**
 * @brief Get the last access type of the frame.
 * @return the last access type of the frame
 */
auto LRUKNode::GetLastAccessType() const -> AccessType { return last_access_type_; }

/**
 * @brief Get the last timestamp of the frame.
 * @return the last timestamp of the frame
 */
auto LRUKNode::GetEarliestTimestamp() const -> size_t {
  BUSTUB_ASSERT(!history_.empty(), "Frame has no timestamps");
  if (GetAccessCount() < k_) {
    return history_[0];
  }
  return history_[write_ptr_];
}

/**
 * @brief Get the access count of the frame.
 * @return the access count of the frame
 */
auto LRUKNode::GetAccessCount() const -> size_t { return access_count_; }

/**
 * @brief Get the frame id of the node.
 * @return the frame id of the node
 */
auto LRUKNode::GetFrameId() const -> frame_id_t { return fid_; }

/**
 * @brief Get the k of the node.
 * @return the k of the node
 */
auto LRUKNode::GetK() const -> size_t { return k_; }

/**
 * @brief Print the node.
 */
void LRUKNode::Print() const {
  printf("Frame %d: [", fid_);
  for (const auto &timestamp : history_) {
    printf("%zu ", timestamp);
  }
  printf("] evictable=%s k=%zu oldest_timestamp=%zu access_count=%zu\n", is_evictable_ ? "true" : "false", k_,
         GetEarliestTimestamp(), GetAccessCount());
}

/**
 * @brief Comparator for the evictable frames to be used to order the frames in the set
 * @param a the first frame id
 * @param b the second frame id
 * @return true if the first frame should be evicted before the second frame, false otherwise
 */
auto LRUKReplacer::EvictableFrameComparator::operator()(const frame_id_t &a, const frame_id_t &b) const -> bool {
  const auto &node_a = node_store_.at(a);
  const auto &node_b = node_store_.at(b);

  // Prioritize eviction of pages accessed primarily by scan operations
  if (node_a.GetLastAccessType() == AccessType::Scan && node_b.GetLastAccessType() != AccessType::Scan) {
    return true;
  }
  if (node_b.GetLastAccessType() == AccessType::Scan && node_a.GetLastAccessType() != AccessType::Scan) {
    return false;
  }

  // Compare whether nodes have reached k references
  bool a_has_k = node_a.GetAccessCount() >= k_;
  bool b_has_k = node_b.GetAccessCount() >= k_;

  if (a_has_k != b_has_k) {
    // Prioritize evicting nodes with fewer than k references
    return !a_has_k;
  }

  // If both nodes are in the same category (both have/don't have k refs),
  // evict the one with earlier last access
  return node_a.GetEarliestTimestamp() < node_b.GetEarliestTimestamp();
}

/**
 * @brief Constructor for LRUKReplacer.
 * @param num_frames the maximum number of frames the LRUReplacer will be required to store
 */
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k)
    : evictable_frames_(EvictableFrameComparator(node_store_, k)), replacer_size_(num_frames), k_(k) {}

/**
 * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
 * that are marked as 'evictable' are candidates for eviction.
 *
 * A frame with less than k historical references is given +inf as its backward k-distance.
 * If multiple frames have inf backward k-distance, then evict frame whose oldest timestamp
 * is furthest in the past.
 *
 * Successful eviction of a frame should decrement the size of replacer and remove the frame's
 * access history.
 *
 * @return true if a frame is evicted successfully, false if no frames can be evicted.
 */
auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  // Acquire lock
  std::scoped_lock lock(latch_);

  // Assert that the size of the replacer is equal to the number of evictable frames
  BUSTUB_ASSERT(curr_size_ == evictable_frames_.size(),
                "Size of replacer is not equal to the number of evictable frames");

  // Assert that the evictable frames are evictable
  for (const auto &frame_id : evictable_frames_) {
    BUSTUB_ASSERT(node_store_.at(frame_id).IsEvictable(), "Frame is not evictable");
  }

  // If there are no evictable frames, return nullopt
  if (evictable_frames_.empty()) {
    return std::nullopt;
  }

  // Get the frame id with the largest backward k-distance
  auto frame_id = *evictable_frames_.begin();

  // Remove from evictable frames
  // Decrement the size of the replacer
  // Return the frame id
  evictable_frames_.erase(frame_id);
  node_store_.erase(frame_id);
  curr_size_--;
  BUSTUB_ASSERT(curr_size_ == evictable_frames_.size(),
                "Size of replacer is not equal to the number of evictable frames");
  return std::make_optional(frame_id);
}

/**
 * @brief Record the event that the given frame id is accessed at current timestamp.
 * Create a new entry for access history if frame id has not been seen before.
 *
 * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
 * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
 *
 * @param frame_id id of frame that received a new access.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 */
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  // Acquire lock
  std::scoped_lock lock(latch_);

  // Assert that the size of the replacer is equal to the number of evictable frames
  BUSTUB_ASSERT(curr_size_ == evictable_frames_.size(),
                "Size of replacer is not equal to the number of evictable frames");
  // Check if frame is valid
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) < replacer_size_, "Invalid frame id");

  // Create a new node if it doesn't exist
  auto [it, inserted] = node_store_.try_emplace(frame_id, k_, frame_id);

  // If the frame is evictable, remove it from the evictable set
  if (it->second.IsEvictable()) {
    // Assert that the frame is in the evictable set
    BUSTUB_ASSERT(evictable_frames_.find(it->first) != evictable_frames_.end(),
                  "Frame is evictable but not in evictable set");
    evictable_frames_.erase(it->first);
  }

  // Record the access
  it->second.RecordAccess(current_timestamp_, access_type);

  // If the frame is evictable, insert it into the evictable set
  // This is to ensure that the set is reordered correctly
  if (it->second.IsEvictable()) {
    evictable_frames_.insert(it->first);
  }

  // Increment the timestamp
  current_timestamp_++;
}

/**
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  // Acquire lock
  std::scoped_lock lock(latch_);

  // Assert that the size of the replacer is equal to the number of evictable frames
  BUSTUB_ASSERT(curr_size_ == evictable_frames_.size(),
                "Size of replacer is not equal to the number of evictable frames");
  // Check if frame is valid
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) < replacer_size_, "Invalid frame id");

  // Check if frame exists
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    return;
  }

  // If the evictable status is not changing, return
  if (it->second.IsEvictable() == set_evictable) {
    return;
  }

  // Set the evictable status
  it->second.SetEvictable(set_evictable);

  // Modify the size if the evictable status is changing
  if (set_evictable) {
    // Assert that the frame is not in the evictable set
    // Increment the size
    // Insert the frame into the evictable set
    BUSTUB_ASSERT(evictable_frames_.find(it->first) == evictable_frames_.end(),
                  "Frame is not evictable but in evictable set");

    curr_size_++;
    evictable_frames_.insert(it->first);
  } else {
    // Assert that the frame is in the evictable set
    // Decrement the size
    // Remove the frame from the evictable set
    BUSTUB_ASSERT(evictable_frames_.find(it->first) != evictable_frames_.end(),
                  "Frame is evictable but not in evictable set");
    curr_size_--;
    evictable_frames_.erase(it->first);
  }
  // Assert that the size of the replacer is equal to the number of evictable frames
  BUSTUB_ASSERT(curr_size_ == evictable_frames_.size(),
                "Size of replacer is not equal to the number of evictable frames");
}

/**
 * @brief Remove an evictable frame from replacer, along with its access history.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * with largest backward k-distance. This function removes specified frame id,
 * no matter what its backward k-distance is.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void LRUKReplacer::Remove(frame_id_t frame_id) {
  // Acquire lock
  std::scoped_lock lock(latch_);

  // Assert that the size of the replacer is equal to the number of evictable frames
  BUSTUB_ASSERT(curr_size_ == evictable_frames_.size(),
                "Size of replacer is not equal to the number of evictable frames");
  // Check if frame is valid
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) < replacer_size_, "Invalid frame id");

  // Check if frame exists
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    return;
  }

  // Check if frame is evictable
  BUSTUB_ASSERT(it->second.IsEvictable(), "Frame is not evictable");
  // Assert that the frame is in the evictable set
  BUSTUB_ASSERT(evictable_frames_.find(it->first) != evictable_frames_.end(),
                "Frame is evictable but not in evictable set");

  // Remove from evictable frames
  // Decrement the size of the replacer
  // Clear the history of the frame
  evictable_frames_.erase(it->first);
  node_store_.erase(it->first);
  curr_size_--;
  BUSTUB_ASSERT(curr_size_ == evictable_frames_.size(),
                "Size of replacer is not equal to the number of evictable frames");
}

/**
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto LRUKReplacer::Size() -> size_t {
  // Acquire lock
  std::scoped_lock lock(latch_);
  return curr_size_;
}

/**
 * @brief Print the node store.
 */
void LRUKReplacer::PrintNodeStore() const {
  printf("\n=== Node Store ===\n");
  for (const auto &[fid, node] : node_store_) {
    node.Print();
  }
}

/**
 * @brief Print the evictable frames.
 */
void LRUKReplacer::PrintEvictableFrames() const {
  printf("\n=== Evictable Frames ===\n");
  printf("Current size: %zu\n", curr_size_);
  for (const auto &fid : evictable_frames_) {
    auto it = node_store_.find(fid);
    if (it != node_store_.end()) {
      it->second.Print();
    }
  }
}

/**
 * @brief Print the state of the replacer.
 */
void LRUKReplacer::PrintState() const {
  // Acquire lock
  std::scoped_lock lock(latch_);
  printf("\n========== LRUKReplacer State ==========\n");
  printf("Current timestamp: %zu\n", current_timestamp_);
  printf("K: %zu\n", k_);
  printf("Current size: %zu\n", curr_size_);
  printf("Replacer size: %zu\n", replacer_size_);
  PrintNodeStore();
  PrintEvictableFrames();
  printf("======================================\n\n");
}

}  // namespace bustub
