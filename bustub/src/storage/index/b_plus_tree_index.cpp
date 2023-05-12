//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree_index.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/index/b_plus_tree_index.h"
#include "common/macros.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {
/*
 * Constructor
 */
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_INDEX_TYPE::BPlusTreeIndex(std::unique_ptr<IndexMetadata> &&metadata, BufferPoolManager *buffer_pool_manager)
    : Index(std::move(metadata)) {
  UNIMPLEMENTED("bpt index doesn't support it.");
}

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_INDEX_TYPE::BPlusTreeIndex(const std::string &file_name, const KeyComparator &comparator, int leaf_max_size,
                                     int internal_max_size, int buffer_pool_size, int replacer_k)
    : Index(nullptr) {
  disk_manager_ = new DiskManager(file_name + ".db");
  bpm_ = new BufferPoolManager(buffer_pool_size, disk_manager_, replacer_k, nullptr, true);
  int header_page_id;
  bpm_->NewPage(&header_page_id);
  auto header_page_guard = bpm_->FetchPageBasic(HEADER_PAGE_ID);
  auto header_page = header_page_guard.AsMut<BPlusTreeHeaderPage>();
  header_page->root_page_id_ = INVALID_PAGE_ID;
  container_ = new BPLUSTREE_TYPE("index", HEADER_PAGE_ID, bpm_, comparator, leaf_max_size, internal_max_size);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_INDEX_TYPE::InsertEntry(const Tuple &key, RID rid, Transaction *transaction) -> bool {
  UNIMPLEMENTED("bpt index doesn't support it.");
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_INDEX_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  return container_->Insert(key, value, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_INDEX_TYPE::DeleteEntry(const Tuple &key, RID rid, Transaction *transaction) {
  UNIMPLEMENTED("bpt index doesn't support it.");
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_INDEX_TYPE::Delete(const KeyType &key, Transaction *transaction) {
  container_->Remove(key, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_INDEX_TYPE::ScanKey(const Tuple &key, std::vector<RID> *result, Transaction *transaction) {
  UNIMPLEMENTED("bpt index doesn't support it.");
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_INDEX_TYPE::Search(const KeyType &key, vector<ValueType> *result, Transaction *transaction) {
  container_->GetValue(key, result, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_INDEX_TYPE::Search(const KeyType &key, vector<ValueType> *result, const KeyComparator &comparator,
                                      Transaction *transaction) {
  container_->GetValue(key, result, comparator, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_INDEX_TYPE::GetBeginIterator() -> INDEXITERATOR_TYPE { return container_->Begin(); }

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_INDEX_TYPE::GetBeginIterator(const KeyType &key) -> INDEXITERATOR_TYPE {
  return container_->Begin(key);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_INDEX_TYPE::GetFirstIterator(const KeyType &key, const KeyComparator &comparator) -> INDEXITERATOR_TYPE {
  return container_->First(key, comparator);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_INDEX_TYPE::GetIterator(const KeyType &key) -> INDEXITERATOR_TYPE {
  return container_->Find(key);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_INDEX_TYPE::GetEndIterator() -> INDEXITERATOR_TYPE { return container_->End(); }

}  // namespace bustub

#ifdef CUSTOMIZED_BUSTUB
#include "storage/index/custom_key.h"
BUSTUB_DECLARE(BPlusTreeIndex)
#else
#ifndef BUSTUB_DECLARE
#define BUSTUB_DECLARE(TypeName)                                                     \
  namespace bustub {                                                                 \
  template class TypeName<GenericKey<4>, RID, GenericComparator<4> >;   /* NOLINT */ \
  template class TypeName<GenericKey<8>, RID, GenericComparator<8> >;   /* NOLINT */ \
  template class TypeName<GenericKey<16>, RID, GenericComparator<16> >; /* NOLINT */ \
  template class TypeName<GenericKey<32>, RID, GenericComparator<32> >; /* NOLINT */ \
  template class TypeName<GenericKey<64>, RID, GenericComparator<64> >; /* NOLINT */ \
  }                                                                     // namespace bustub
#endif
BUSTUB_DECLARE(BPlusTreeIndex)
#endif
