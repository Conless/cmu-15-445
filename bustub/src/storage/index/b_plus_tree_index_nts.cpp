#include "storage/index/b_plus_tree_index_nts.h"
#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "common/macros.h"
#include "storage/disk/disk_manager.h"
#include "storage/index/b_plus_tree_nts.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {
/*
 * Constructor
 */
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_INDEX_NTS_TYPE::BPlusTreeIndex(std::unique_ptr<IndexMetadata> &&metadata,
                                         BufferPoolManager *buffer_pool_manager)
    : Index(std::move(metadata)) {
  UNIMPLEMENTED("bpt index doesn't support it.");
}

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_INDEX_NTS_TYPE::BPlusTreeIndex(const std::string &file_name, const KeyComparator &comparator,
                                         int leaf_max_size, int internal_max_size, int buffer_pool_size, int replacer_k)
    : Index(nullptr) {
  disk_manager_ = new DiskManager(file_name + ".db", false);
  bpm_ = new BufferPoolManager(buffer_pool_size, disk_manager_, replacer_k, nullptr, false);
  bool file_exist_opt = disk_manager_->Initialized();
  if (!file_exist_opt) {
    int header_page_id;
    bpm_->NewPage(&header_page_id);
    auto header_page_guard = bpm_->FetchPageBasic(HEADER_PAGE_ID);
    auto header_page = header_page_guard.AsMut<BPlusTreeHeaderPage>();
    header_page->root_page_id_ = INVALID_PAGE_ID;
  } else {
    page_id_t next_page_id;
    disk_manager_->ReadLog(reinterpret_cast<char *>(&next_page_id), sizeof(page_id_t), 0);
    bpm_->SetNextPageId(next_page_id);
  }
  container_ =
      std::make_shared<BPLUSTREE_NTS_TYPE>("index", HEADER_PAGE_ID, bpm_, comparator, leaf_max_size, internal_max_size);
}

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_INDEX_NTS_TYPE::~BPlusTreeIndex() {
  auto header_page = bpm_->FetchPageBasic(HEADER_PAGE_ID).As<BPlusTreeHeaderPage>();
  bpm_->FlushAllPages();
  page_id_t next_page_id = bpm_->GetNextPageId();
  disk_manager_->WriteLog(reinterpret_cast<char *>(&next_page_id), sizeof(page_id_t));
  delete bpm_;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_INDEX_NTS_TYPE::InsertEntry(const Tuple &key, RID rid, Transaction *transaction) -> bool {
  UNIMPLEMENTED("bpt index doesn't support it.");
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_INDEX_NTS_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  return container_->Insert(key, value, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_INDEX_NTS_TYPE::DeleteEntry(const Tuple &key, RID rid, Transaction *transaction) {
  UNIMPLEMENTED("bpt index doesn't support it.");
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_INDEX_NTS_TYPE::Delete(const KeyType &key, Transaction *transaction) {
  container_->Remove(key, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_INDEX_NTS_TYPE::ScanKey(const Tuple &key, std::vector<RID> *result, Transaction *transaction) {
  UNIMPLEMENTED("bpt index doesn't support it.");
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_INDEX_NTS_TYPE::Search(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  container_->GetValue(key, result, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_INDEX_NTS_TYPE::Search(const KeyType &key, std::vector<ValueType> *result,
                                      const KeyComparator &comparator, Transaction *transaction) {
  container_->GetValue(key, result, comparator, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_INDEX_NTS_TYPE::GetBeginIterator() -> INDEXITERATOR_TYPE { return container_->Begin(); }

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_INDEX_NTS_TYPE::GetBeginIterator(const KeyType &key) -> INDEXITERATOR_TYPE {
  return container_->Begin(key);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_INDEX_NTS_TYPE::GetEndIterator() -> INDEXITERATOR_TYPE { return container_->End(); }

}  // namespace bustub

#ifdef CUSTOMIZED_BUSTUB
#include "storage/index/custom_key.h"
BUSTUB_NTS_DECLARE(BPlusTreeIndex)
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
