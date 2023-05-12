#pragma once

#include "buffer/buffer_pool_manager.h"
#include "storage/disk/disk_manager.h"
#include "storage/index/b_plus_tree_index.h"
#include "storage/index/b_plus_tree_nts.h"
#include "storage/page/b_plus_tree_page.h"

#define BPLUSTREE_INDEX_NTS_TYPE BPlusTreeIndex<KeyType, ValueType, KeyComparator, false>

#ifdef REPLACE_STL
#include "container/stl/vector.h"
using sjtu::vector;  // NOLINT
#else
using std::vector;  // NOLINT
#endif

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeIndex<KeyType, ValueType, KeyComparator, false> : public Index {
 public:
  explicit BPlusTreeIndex(const std::string &file_name, const KeyComparator &comparator = KeyComparator(),
                          int leaf_max_size = LEAF_PAGE_SIZE, int internal_max_size = INTERNAL_PAGE_SIZE,
                          int buffer_pool_size = BUFFER_POOL_SIZE, int replacer_k = LRUK_REPLACER_K);

  ~BPlusTreeIndex() override;

  BPlusTreeIndex(std::unique_ptr<IndexMetadata> &&metadata, BufferPoolManager *buffer_pool_manager);

  auto InsertEntry(const Tuple &key, RID rid, Transaction *transaction) -> bool override;

  void DeleteEntry(const Tuple &key, RID rid, Transaction *transaction) override;

  void ScanKey(const Tuple &key, std::vector<RID> *result, Transaction *transaction) override;

  auto Insert(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr) -> bool;

  void Delete(const KeyType &key, Transaction *transaction = nullptr);

  void Search(const KeyType &key, vector<ValueType> *result, Transaction *transaction = nullptr);

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
  BPlusTree<KeyType, ValueType, KeyComparator, false> *container_;
};

}  // namespace bustub