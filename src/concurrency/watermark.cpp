//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// watermark.cpp
//
// Identification: src/concurrency/watermark.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/watermark.h"
#include <exception>
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }
  current_reads_[read_ts]++;
  active_ts_.insert(read_ts);
  watermark_ = *active_ts_.begin();
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  auto it = current_reads_.find(read_ts);

  if (it == current_reads_.end()) {
    return;
  }

  // Decrement the count and erase if zero
  if (--it->second == 0) {
    current_reads_.erase(it);
    active_ts_.erase(read_ts);

    // Update watermark if necessary
    if (current_reads_.empty()) {
      watermark_ = commit_ts_;
    } else if (read_ts == watermark_) {
      watermark_ = *active_ts_.begin();
    }
  }
}

}  // namespace bustub
