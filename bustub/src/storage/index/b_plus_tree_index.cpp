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
BPLUSTREE_INDEX_TYPE::BPlusTreeIndex() : Index(nullptr) {}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_INDEX_TYPE::InsertEntry(const Tuple &key, RID rid, Transaction *transaction) -> bool {
  UNIMPLEMENTED("bpt index doesn't support it.");
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_INDEX_TYPE::Insert(const KeyType &key, const ValueType &rid, Transaction *transaction) -> bool {
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_INDEX_TYPE::DeleteEntry(const Tuple &key, RID rid, Transaction *transaction) {
  UNIMPLEMENTED("bpt index doesn't support it.");
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_INDEX_TYPE::Delete(const KeyType &key, Transaction *transaction) {}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_INDEX_TYPE::ScanKey(const Tuple &key, std::vector<RID> *result, Transaction *transaction) {
  UNIMPLEMENTED("bpt index doesn't support it.");
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_INDEX_TYPE::Search(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_INDEX_TYPE::GetBeginIterator() -> INDEXITERATOR_TYPE { return container_->Begin(); }

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_INDEX_TYPE::GetBeginIterator(const KeyType &key) -> INDEXITERATOR_TYPE { return container_->Begin(key); }

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_INDEX_TYPE::GetEndIterator() -> INDEXITERATOR_TYPE { return container_->End(); }

}  // namespace bustub

#ifdef CUSTOMIZED_BUSTUB
#include "storage/index/custom_key.h"
BUSTUB_DECLARE(BPlusTreeIndex)
#else
#define BUSTUB_DECLARE(TypeName)
namespace bustub {
template class TypeName<GenericKey<4>, RID, GenericComparator<4> >;   /* NOLINT */
template class TypeName<GenericKey<8>, RID, GenericComparator<8> >;   /* NOLINT */
template class TypeName<GenericKey<16>, RID, GenericComparator<16> >; /* NOLINT */
template class TypeName<GenericKey<32>, RID, GenericComparator<32> >; /* NOLINT */
template class TypeName<GenericKey<64>, RID, GenericComparator<64> >; /* NOLINT */
}  // namespace bustub
BUSTUB_DECLARE(BPlusTreeIndex)
#endif
