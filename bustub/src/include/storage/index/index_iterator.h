//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "buffer/buffer_pool_manager.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  // you may define your own constructor based on your member variables
  IndexIterator(page_id_t page_id, int index_in_page, BufferPoolManager *bpm);
  ~IndexIterator();

  auto IsEnd() const -> bool;

  auto operator*() -> MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    if (IsEnd()) {
      return itr.IsEnd();
    }
    return (bpm_ == itr.bpm_) && (page_id_ == itr.page_id_) && (this->index_in_page_ == itr.index_in_page_);
  }

  auto operator!=(const IndexIterator &itr) const -> bool { return !(*this == itr); }

 private:
  // add your own private member variables here
  LeafPage *cur_page_;
  page_id_t page_id_;
  int index_in_page_;
  BufferPoolManager *bpm_;
};

}  // namespace bustub
