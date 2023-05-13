//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/b_plus_tree_index.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "container/hash/hash_function.h"
#include "storage/index/b_plus_tree.h"
#include "storage/index/index.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define BPLUSTREE_INDEX_TYPE BPlusTreeIndex<KeyType, ValueType, KeyComparator, true>

template <typename KeyType, typename ValueType, typename KeyComparator = typename KeyType::Comparator,
          bool isThreadSafe = false>
class BPlusTreeIndex : public Index {};

INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeIndex<KeyType, ValueType, KeyComparator, true> : public Index {
 public:
  explicit BPlusTreeIndex(const std::string &file_name, const KeyComparator &comparator = KeyComparator(),
                          int leaf_max_size = LEAF_PAGE_SIZE, int internal_max_size = INTERNAL_PAGE_SIZE,
                          int buffer_pool_size = BUFFER_POOL_SIZE, int replacer_k = LRUK_REPLACER_K);

  ~BPlusTreeIndex() override;

  BPlusTreeIndex(std::unique_ptr<IndexMetadata> &&metadata, BufferPoolManager *buffer_pool_manager);

  auto InsertEntry(const Tuple &key, RID rid, Transaction *transaction) -> bool override;

  void DeleteEntry(const Tuple &key, RID rid, Transaction *transaction) override;

  void ScanKey(const Tuple &key, std::vector<RID> *result, Transaction *transaction) override;

  auto Empty() const -> bool;

  auto Insert(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr) -> bool;

  void Delete(const KeyType &key, Transaction *transaction = nullptr);

  auto Find(const KeyType &key, Transaction *transaction = nullptr) -> std::pair<bool, ValueType>;

  void Search(const KeyType &key, vector<ValueType> *result, const KeyComparator &comparator,
              Transaction *transaction = nullptr);

  auto GetBeginIterator() -> INDEXITERATOR_TYPE;

  auto GetBeginIterator(const KeyType &key) -> INDEXITERATOR_TYPE;

  auto GetFirstIterator(const KeyType &key, const KeyComparator &comparator) -> INDEXITERATOR_TYPE;

  auto GetIterator(const KeyType &key) -> INDEXITERATOR_TYPE;

  auto GetEndIterator() -> INDEXITERATOR_TYPE;

 public:
  DiskManager *disk_manager_;
  BufferPoolManager *bpm_;
  BPlusTree<KeyType, ValueType, KeyComparator, true> *container_;
};

/** We only support index table with one integer key for now in BusTub. Hardcode everything here. */

constexpr static const auto TWO_INTEGER_SIZE = 8;
using IntegerKeyType = GenericKey<TWO_INTEGER_SIZE>;
using IntegerValueType = RID;
using IntegerComparatorType = GenericComparator<TWO_INTEGER_SIZE>;
using BPlusTreeIndexForTwoIntegerColumn = BPlusTreeIndex<IntegerKeyType, IntegerValueType, IntegerComparatorType>;
using BPlusTreeIndexIteratorForTwoIntegerColumn =
    IndexIterator<IntegerKeyType, IntegerValueType, IntegerComparatorType>;
using IntegerHashFunctionType = HashFunction<IntegerKeyType>;

}  // namespace bustub
