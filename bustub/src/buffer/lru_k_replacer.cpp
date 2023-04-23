//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <algorithm>
#include <exception>
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k, bool is_thread_safe)
    : replacer_size_(num_frames), k_(k), is_thread_safe_(is_thread_safe) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::unique_lock<std::mutex> lock =
      is_thread_safe_ ? std::unique_lock<std::mutex>(latch_) : std::unique_lock<std::mutex>();
  if (curr_size_ == 0) {
    return false;
  }
  auto check_evictable = [this, frame_id](const frame_info &it) {
    auto node = node_store_[it.first];
    if (node.is_evictable_) {
      *frame_id = node.fid_;
      if (node.cnt_ < k_) {
        temp_frame_list_.erase(node.iter_);
      } else {
        cache_frame_list_.erase(node.iter_);
      }
      node_store_.erase(node.fid_);
      curr_size_--;
      return true;
    }
    return false;
  };
  return std::any_of(temp_frame_list_.begin(), temp_frame_list_.end(), check_evictable) ||
         std::any_of(cache_frame_list_.begin(), cache_frame_list_.end(), check_evictable);
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::unique_lock<std::mutex> lock =
      is_thread_safe_ ? std::unique_lock<std::mutex>(latch_) : std::unique_lock<std::mutex>();
  auto comp = [](const frame_info &a, const frame_info &b) { return a.second < b.second; };
  current_timestamp_++;
  auto &node = node_store_[frame_id];
  node.fid_ = frame_id;
  node.cnt_++;
  node.history_.emplace_back(current_timestamp_);
  if (node.cnt_ == 1) {
    if (curr_size_ == replacer_size_) {
      bool flag = false;
      for (auto it : temp_frame_list_) {
        auto del_node = node_store_[it.first];
        if (del_node.is_evictable_) {
          temp_frame_list_.erase(del_node.iter_);
          node_store_.erase(del_node.fid_);
          curr_size_--;
          flag = true;
          break;
        }
      }
      if (!flag) {
        for (auto it : cache_frame_list_) {
          auto del_node = node_store_[it.first];
          if (del_node.is_evictable_) {
            cache_frame_list_.erase(del_node.iter_);
            node_store_.erase(del_node.fid_);
            curr_size_--;
          }
        }
      }
    }
    curr_size_++;
    temp_frame_list_.emplace_back(std::make_pair(frame_id, current_timestamp_));
    node.is_evictable_ = true;
    node.iter_ = std::prev(temp_frame_list_.end());
  }
  if (node.cnt_ == k_) {
    auto it = std::upper_bound(cache_frame_list_.begin(), cache_frame_list_.end(), *node.iter_, comp);
    it = cache_frame_list_.insert(it, *node.iter_);
    temp_frame_list_.erase(node.iter_);
    node.iter_ = it;
    return;
  }
  if (node.cnt_ > k_) {
    node.history_.pop_front();
    auto new_info = std::make_pair(frame_id, node.history_.front());
    cache_frame_list_.erase(node.iter_);
    auto it = std::upper_bound(cache_frame_list_.begin(), cache_frame_list_.end(), new_info, comp);
    node.iter_ = cache_frame_list_.insert(it, new_info);
    return;
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::unique_lock<std::mutex> lock =
      is_thread_safe_ ? std::unique_lock<std::mutex>(latch_) : std::unique_lock<std::mutex>();
  auto &node = node_store_[frame_id];
  if (node.cnt_ == 0) {
    return;
  }
  auto old_evictable = node.is_evictable_;
  node.is_evictable_ = set_evictable;
  if (old_evictable && !set_evictable) {
    --replacer_size_;
    --curr_size_;
  }
  if (!old_evictable && set_evictable) {
    ++replacer_size_;
    ++curr_size_;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::unique_lock<std::mutex> lock =
      is_thread_safe_ ? std::unique_lock<std::mutex>(latch_) : std::unique_lock<std::mutex>();
  auto &node = node_store_[frame_id];
  if (node.cnt_ == 0) {
    return;
  }
  if (!node.is_evictable_) {
    throw std::exception();
  }
  if (node.cnt_ < k_) {
    temp_frame_list_.erase(node.iter_);
    node_store_.erase(node.fid_);
    curr_size_--;
  } else {
    cache_frame_list_.erase(node.iter_);
    node_store_.erase(node.fid_);
    curr_size_--;
  }
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
