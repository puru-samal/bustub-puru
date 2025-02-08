//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer_test.cpp
//
// Identification: test/buffer/lru_k_replacer_test.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * lru_k_replacer_test.cpp
 */

#include "buffer/lru_k_replacer.h"

#include <algorithm>
#include <cstdio>
#include <memory>
#include <random>
#include <set>
#include <thread>  // NOLINT
#include <vector>

#include "gtest/gtest.h"

namespace bustub {

TEST(LRUKReplacerTest, SampleTest) {
  // Note that comparison with `std::nullopt` always results in `false`, and if the optional type actually does contain
  // a value, the comparison will compare the inner value.
  // See: https://devblogs.microsoft.com/oldnewthing/20211004-00/?p=105754
  std::optional<frame_id_t> frame;

  // Initialize the replacer.
  LRUKReplacer lru_replacer(7, 2);

  // Add six frames to the replacer. We now have frames [1, 2, 3, 4, 5]. We set frame 6 as non-evictable.
  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(2);
  lru_replacer.RecordAccess(3);
  lru_replacer.RecordAccess(4);
  lru_replacer.RecordAccess(5);
  lru_replacer.RecordAccess(6);

  lru_replacer.SetEvictable(1, true);
  lru_replacer.SetEvictable(2, true);
  lru_replacer.SetEvictable(3, true);
  lru_replacer.SetEvictable(4, true);
  lru_replacer.SetEvictable(5, true);
  lru_replacer.SetEvictable(6, false);

  // The size of the replacer is the number of frames that can be evicted, _not_ the total number of frames entered.
  ASSERT_EQ(5, lru_replacer.Size());

  // Record an access for frame 1. Now frame 1 has two accesses total.
  lru_replacer.RecordAccess(1);
  // All other frames now share the maximum backward k-distance. Since we use timestamps to break ties, where the first
  // to be evicted is the frame with the oldest timestamp, the order of eviction should be [2, 3, 4, 5, 1].

  // Evict three pages from the replacer.
  // To break ties, we use LRU with respect to the oldest timestamp, or the least recently used frame.
  ASSERT_EQ(2, lru_replacer.Evict());
  ASSERT_EQ(3, lru_replacer.Evict());
  ASSERT_EQ(4, lru_replacer.Evict());
  ASSERT_EQ(2, lru_replacer.Size());
  // Now the replacer has the frames [5, 1].

  // Insert new frames [3, 4], and update the access history for 5. Now, the ordering is [3, 1, 5, 4].
  lru_replacer.RecordAccess(3);
  lru_replacer.RecordAccess(4);
  lru_replacer.RecordAccess(5);
  lru_replacer.RecordAccess(4);
  lru_replacer.SetEvictable(3, true);
  lru_replacer.SetEvictable(4, true);
  ASSERT_EQ(4, lru_replacer.Size());

  // Look for a frame to evict. We expect frame 3 to be evicted next.
  ASSERT_EQ(3, lru_replacer.Evict());
  ASSERT_EQ(3, lru_replacer.Size());

  // Set 6 to be evictable. 6 Should be evicted next since it has the maximum backward k-distance.
  lru_replacer.SetEvictable(6, true);
  ASSERT_EQ(4, lru_replacer.Size());
  ASSERT_EQ(6, lru_replacer.Evict());
  ASSERT_EQ(3, lru_replacer.Size());

  // Mark frame 1 as non-evictable. We now have [5, 4].
  lru_replacer.SetEvictable(1, false);
  // We expect frame 5 to be evicted next.
  ASSERT_EQ(2, lru_replacer.Size());
  ASSERT_EQ(5, lru_replacer.Evict());
  ASSERT_EQ(1, lru_replacer.Size());

  // Update the access history for frame 1 and make it evictable. Now we have [4, 1].
  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(1);
  lru_replacer.SetEvictable(1, true);
  ASSERT_EQ(2, lru_replacer.Size());

  // Evict the last two frames.
  ASSERT_EQ(4, lru_replacer.Evict());
  ASSERT_EQ(1, lru_replacer.Size());
  ASSERT_EQ(1, lru_replacer.Evict());
  ASSERT_EQ(0, lru_replacer.Size());

  // Insert frame 1 again and mark it as non-evictable.
  lru_replacer.RecordAccess(1);
  lru_replacer.SetEvictable(1, false);
  ASSERT_EQ(0, lru_replacer.Size());

  // A failed eviction should not change the size of the replacer.
  frame = lru_replacer.Evict();
  ASSERT_EQ(false, frame.has_value());

  // Mark frame 1 as evictable again and evict it.
  lru_replacer.SetEvictable(1, true);
  ASSERT_EQ(1, lru_replacer.Size());
  ASSERT_EQ(1, lru_replacer.Evict());
  ASSERT_EQ(0, lru_replacer.Size());

  // There is nothing left in the replacer, so make sure this doesn't do something strange.
  frame = lru_replacer.Evict();
  ASSERT_EQ(false, frame.has_value());
  ASSERT_EQ(0, lru_replacer.Size());

  // Make sure that setting a non-existent frame as evictable or non-evictable doesn't do something strange.
  lru_replacer.SetEvictable(6, false);
  lru_replacer.SetEvictable(6, true);
}

TEST(LRUKReplacerTest, BasicFunctionalityTest) {
  LRUKReplacer lru_replacer(5, 2);

  // Test initial state
  ASSERT_EQ(0, lru_replacer.Size());

  // Add some frames and verify size
  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(2);
  lru_replacer.RecordAccess(3);
  lru_replacer.SetEvictable(1, true);
  lru_replacer.SetEvictable(2, true);
  lru_replacer.SetEvictable(3, true);
  ASSERT_EQ(3, lru_replacer.Size());

  // Test eviction order (should be LRU since all have 1 access)
  ASSERT_EQ(1, lru_replacer.Evict());
  ASSERT_EQ(2, lru_replacer.Evict());
  ASSERT_EQ(1, lru_replacer.Size());

  // Evict frame 3
  ASSERT_EQ(3, lru_replacer.Evict());
  ASSERT_EQ(0, lru_replacer.Size());

  // Check history
  // Record multiple accesses
  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(1);  // 2 accesses
  lru_replacer.RecordAccess(2);  // 1 access
  lru_replacer.RecordAccess(3);  // 1 access

  lru_replacer.SetEvictable(1, true);
  lru_replacer.SetEvictable(2, true);
  lru_replacer.SetEvictable(3, true);

  // Frame 1 should be evicted last due to having 2 accesses
  ASSERT_EQ(2, lru_replacer.Evict());
  ASSERT_EQ(3, lru_replacer.Evict());
  ASSERT_EQ(1, lru_replacer.Evict());

  // Interleave multiple accesses until all frames have k or more accesses
  lru_replacer.RecordAccess(1);  // 1 access, t = 7
  lru_replacer.RecordAccess(2);  // 1 access, t = 8
  lru_replacer.RecordAccess(1);  // 2 access, t = 9
  lru_replacer.RecordAccess(3);  // 1 access, t = 10
  lru_replacer.RecordAccess(1);  // 2 access, t = 11
  lru_replacer.RecordAccess(2);  // 2 access, t = 12
  lru_replacer.RecordAccess(1);  // 3 access, t = 13
  lru_replacer.RecordAccess(3);  // 3 access, t = 14
  lru_replacer.RecordAccess(2);  // 3 access, t = 15
  lru_replacer.RecordAccess(2);  // 4 access, t = 16
  lru_replacer.RecordAccess(1);  // 4 access, t = 17

  lru_replacer.SetEvictable(1, true);
  lru_replacer.SetEvictable(2, true);
  lru_replacer.SetEvictable(3, true);

  // 1 backward k-distance = 18 - 13 = 5, oldest timestamp = 13
  // 2 backward k-distance = 18 - 15 = 3, oldest timestamp = 15
  // 3 backward k-distance = 18 - 10 = 8, oldest timestamp = 10

  // Frame 3 should be evicted first due to the oldest timestamp as the tie-breaker
  ASSERT_EQ(3, lru_replacer.Evict());
  ASSERT_EQ(1, lru_replacer.Evict());
  ASSERT_EQ(2, lru_replacer.Evict());
}

TEST(LRUKReplacerTest, ConcurrentTest1) {
  const int num_threads = 10;
  const int num_accesses = 1000;
  LRUKReplacer lru_replacer(50, 2);

  auto record_task = [&](int start) {
    for (int i = start; i < start + num_accesses; i++) {
      frame_id_t frame_id = i % 50;  // Cycle through 50 frames
      lru_replacer.RecordAccess(frame_id);
      lru_replacer.SetEvictable(frame_id, true);
    }
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back(record_task, i * num_accesses);
  }

  for (auto &thread : threads) {
    thread.join();
  }

  // Verify all frames are present
  ASSERT_EQ(50, lru_replacer.Size());
}

TEST(LRUKReplacerTest, ConcurrentTest2) {
  const int num_threads = 5;
  LRUKReplacer lru_replacer(20, 4);
  std::atomic<int> successful_evictions(0);

  // Setup initial frames
  for (frame_id_t i = 0; i < 20; i++) {
    lru_replacer.RecordAccess(i);
    lru_replacer.SetEvictable(i, true);
  }

  auto evict_task = [&]() {
    for (int i = 0; i < 4; i++) {  // Each thread tries to evict 4 frames
      auto frame = lru_replacer.Evict();
      if (frame.has_value()) {
        successful_evictions++;
      }
    }
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back(evict_task);
  }

  for (auto &thread : threads) {
    thread.join();
  }

  ASSERT_EQ(20, successful_evictions.load());  // All frames should be evicted
  ASSERT_EQ(0, lru_replacer.Size());
}

TEST(LRUKReplacerTest, DeterministicConcurrentAccessTest) {
  const int num_threads = 4;
  const int frames_per_thread = 5;
  LRUKReplacer lru_replacer(num_threads * frames_per_thread, 2);

  std::mutex mutex;
  std::condition_variable cv;
  std::atomic<int> ready_threads(0);
  bool start_flag = false;

  auto record_task = [&](int thread_id) {
    // Signal thread is ready and wait for all threads
    {
      std::unique_lock<std::mutex> lock(mutex);
      ready_threads++;
      if (ready_threads == num_threads) {
        start_flag = true;
        cv.notify_all();
      } else {
        cv.wait(lock, [&]() { return start_flag; });
      }
    }

    // Each thread works on its own distinct set of frames
    for (int i = 0; i < frames_per_thread; i++) {
      frame_id_t frame_id = thread_id * frames_per_thread + i;
      // First access
      lru_replacer.RecordAccess(frame_id);
      lru_replacer.SetEvictable(frame_id, true);
      // Second access
      lru_replacer.RecordAccess(frame_id);
    }
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back(record_task, i);
  }

  for (auto &thread : threads) {
    thread.join();
  }

  // Verify the final state
  ASSERT_EQ(num_threads * frames_per_thread, lru_replacer.Size());

  // Verify eviction order - frames with fewer accesses should be evicted first
  std::set<frame_id_t> evicted_frames;
  for (int i = 0; i < num_threads * frames_per_thread; i++) {
    auto frame = lru_replacer.Evict();
    ASSERT_TRUE(frame.has_value());
    ASSERT_TRUE(evicted_frames.insert(frame.value()).second);  // Each frame should be evicted exactly once
  }
}

TEST(LRUKReplacerTest, DeterministicConcurrentEvictionTest) {
  const int num_threads = 4;
  const int frames_per_thread = 5;
  LRUKReplacer lru_replacer(num_threads * frames_per_thread, 2);

  std::mutex mutex;
  std::condition_variable cv;
  std::atomic<int> ready_threads(0);
  bool start_flag = false;

  std::mutex result_mutex;
  std::vector<frame_id_t> evicted_frames;  // Changed to vector to preserve eviction order

  // Initialize frames with deterministic access patterns
  for (int i = 0; i < num_threads * frames_per_thread; i++) {
    lru_replacer.RecordAccess(i);
    if (i % 2 == 0) {  // Give even-numbered frames three accesses to ensure they're evicted later
      lru_replacer.RecordAccess(i);
      lru_replacer.RecordAccess(i);
    }
    lru_replacer.SetEvictable(i, true);
  }

  auto evict_task = [&](int thread_id) {
    // Signal thread is ready and wait for all threads
    {
      std::unique_lock<std::mutex> lock(mutex);
      ready_threads++;
      if (ready_threads == num_threads) {
        start_flag = true;
        cv.notify_all();
      } else {
        cv.wait(lock, [&]() { return start_flag; });
      }
    }

    // Each thread attempts to evict frames_per_thread frames
    for (int i = 0; i < frames_per_thread; i++) {
      auto frame = lru_replacer.Evict();
      if (frame.has_value()) {
        std::lock_guard<std::mutex> lock(result_mutex);
        evicted_frames.push_back(frame.value());
      }
    }
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back(evict_task, i);
  }

  for (auto &thread : threads) {
    thread.join();
  }

  // Verify results
  ASSERT_EQ(num_threads * frames_per_thread, evicted_frames.size());

  // Since we know exactly where the transition should occur, we can assert it directly
  size_t expected_transition = num_threads * frames_per_thread / 2;
  for (size_t i = 0; i < expected_transition; i++) {
    ASSERT_EQ(1, evicted_frames[i] % 2) << "Frame " << evicted_frames[i] << " at position " << i << " should be odd";
  }
  for (size_t i = expected_transition; i < evicted_frames.size(); i++) {
    ASSERT_EQ(0, evicted_frames[i] % 2) << "Frame " << evicted_frames[i] << " at position " << i << " should be even";
  }

  // Check the size of the replacer
  ASSERT_EQ(0, lru_replacer.Size());
}

}  // namespace bustub
