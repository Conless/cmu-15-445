#include <algorithm>
#include <sstream>
#include <string>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = guard.As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_ == INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SetNewRoot(page_id_t new_root_id) {
  WritePageGuard header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
  SetNewRoot(new_root_id, header_page);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SetNewRoot(page_id_t new_root_id, BPlusTreeHeaderPage *header_page) {
  header_page->root_page_id_ = new_root_id;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CreateNewRoot(IndexPageType page_type) -> page_id_t {
  WritePageGuard header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
  return CreateNewRoot(page_type, header_page);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CreateNewRoot(IndexPageType page_type, BPlusTreeHeaderPage *header_page) -> page_id_t {
  header_page->root_page_id_ = CreateNewPage(page_type);
  return header_page->root_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CreateNewPage(IndexPageType page_type) -> page_id_t {
  page_id_t new_page_id;
  bpm_->NewPageGuarded(&new_page_id);
  WritePageGuard page_guard = bpm_->FetchPageWrite(new_page_id);
  if (page_type == IndexPageType::INTERNAL_PAGE) {
    auto page = page_guard.AsMut<InternalPage>();
    page->Init(internal_max_size_);
  } else {
    auto page = page_guard.AsMut<LeafPage>();
    page->Init(leaf_max_size_);
  }
  return new_page_id;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootGuardWrite(bool create_new_root) -> WritePageGuard {
  WritePageGuard header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    if (!create_new_root) {
      return {nullptr, nullptr};
    }
    CreateNewRoot(IndexPageType::LEAF_PAGE, header_page);
  }
  auto root_guard = bpm_->FetchPageWrite(header_page->root_page_id_);
  return root_guard;
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootGuardRead() -> ReadPageGuard {
  ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<BPlusTreeHeaderPage>();
  auto root_guard = bpm_->FetchPageRead(header_page->root_page_id_);
  return root_guard;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  return GetValue(key, result, comparator_, txn);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, const KeyComparator &comparator,
                              Transaction *txn) -> bool {
  // Declaration of context instance.
  BUSTUB_ENSURE(result->empty(), "The result array should be empty.");
  Context ctx;
  ReadPageGuard root_guard = GetRootGuardRead();
  ctx.read_set_.emplace_back(std::move(root_guard));
  return GetValueInPage(key, result, &ctx, comparator_);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValueInPage(const KeyType &key, std::vector<ValueType> *result, Context *ctx,
                                    const KeyComparator &comparator) -> bool {
  auto cur_page = ctx->read_set_.back().As<BPlusTreePage>();
  if (cur_page->IsLeafPage()) {
    return GetValueInLeafPage(key, result, ctx, comparator_);
  }
  auto internal_page = reinterpret_cast<const InternalPage *>(cur_page);
  int next_search_index = internal_page->GetLastIndexL(key, comparator);
  page_id_t next_page_id = internal_page->ValueAt(next_search_index);
  auto next_guard = bpm_->FetchPageRead(next_page_id);
  ctx->read_set_.emplace_back(std::move(next_guard));
  bool res = GetValueInPage(key, result, ctx, comparator);
  ctx->read_set_.pop_back();
  return res;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValueInLeafPage(const KeyType &key, std::vector<ValueType> *result, Context *ctx,
                                        const KeyComparator &comparator) -> bool {
  auto leaf_page = ctx->read_set_.back().As<LeafPage>();
  int index = leaf_page->GetLastIndexL(key, comparator) + 1;
  int size = leaf_page->GetSize();
  for (; index < size; index++) {
    if (comparator(leaf_page->KeyAt(index), key) > 0) {
      break;
    }
    result->push_back(leaf_page->ValueAt(index));
  }
  if (index == size) {
    page_id_t next_leaf_id = leaf_page->GetNextPageId();
    if (next_leaf_id != INVALID_PAGE_ID) {
      auto next_guard = bpm_->FetchPageRead(next_leaf_id);
      ctx->read_set_.emplace_back(std::move(next_guard));
      GetValueInLeafPage(key, result, ctx, comparator);
    }
  }
  ctx->read_set_.pop_back();
  return !result->empty();
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if uscer try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  // Declaration of context instance
  Context ctx;
  WritePageGuard root_guard = GetRootGuardWrite(true);
  ctx.write_set_.emplace_back(std::move(root_guard));
  if (InsertIntoPage(key, value, &ctx)) {
    WritePageGuard cur_guard = GetRootGuardWrite();
    auto cur_page = cur_guard.AsMut<BPlusTreePage>();
    if (cur_page->SizeExceeded()) {
      page_id_t old_root_id = GetRootPageId();
      page_id_t root_id = CreateNewRoot(IndexPageType::INTERNAL_PAGE);
      WritePageGuard root_guard = bpm_->FetchPageWrite(root_id);
      auto root_page = root_guard.AsMut<InternalPage>();
      root_page->IncreaseSize(1);
      root_page->SetValueAt(0, old_root_id);
      if (cur_page->IsLeafPage()) {
        SplitLeafPage(reinterpret_cast<LeafPage *>(cur_page), root_page);
      } else {
        SplitInternalPage(reinterpret_cast<InternalPage *>(cur_page), root_page);
      }
    }
    return true;
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoPage(const KeyType &key, const ValueType &value, Context *ctx) -> bool {
  auto cur_page = ctx->write_set_.back().AsMut<BPlusTreePage>();
  if (cur_page->IsLeafPage()) {
    return InsertIntoLeafPage(key, value, ctx);
  }
  auto internal_page = reinterpret_cast<InternalPage *>(cur_page);
  int next_insert_index = internal_page->GetLastIndexLE(key, comparator_);
  page_id_t next_page_id = internal_page->ValueAt(next_insert_index);
  auto next_guard = bpm_->FetchPageWrite(next_page_id);
  ctx->write_set_.emplace_back(std::move(next_guard));
  if (InsertIntoPage(key, value, ctx)) {
    WritePageGuard cur_guard = std::move(ctx->write_set_.back());
    ctx->write_set_.pop_back();
    if (internal_page->SizeExceeded() && !ctx->write_set_.empty()) {
      auto last_page = ctx->write_set_.back().AsMut<InternalPage>();
      int index = last_page->GetLastIndexLE(key, comparator_);
      if (!ShiftInternalPage(internal_page, last_page, index)) {
        SplitInternalPage(internal_page, last_page);
      }
    }
    return true;
  }
  ctx->write_set_.pop_back();
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoLeafPage(const KeyType &key, const ValueType &value, Context *ctx) -> bool {
  auto leaf_page = ctx->write_set_.back().AsMut<LeafPage>();
  bool res = leaf_page->InsertData(key, value, comparator_) != -1;
  WritePageGuard cur_guard = std::move(ctx->write_set_.back());
  ctx->write_set_.pop_back();
  if (leaf_page->SizeExceeded() && !ctx->write_set_.empty()) {
    auto last_page = ctx->write_set_.back().AsMut<InternalPage>();
    int index = last_page->GetLastIndexLE(key, comparator_);
    if (!ShiftLeafPage(leaf_page, last_page, index)) {
      SplitLeafPage(leaf_page, last_page);
    }
  }
  return res;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ShiftLeafPage(LeafPage *cur_page, InternalPage *last_page, int index) -> bool {
  bool shifted = false;
  if (index != last_page->GetSize() - 1) {
    page_id_t next_leaf_id = last_page->ValueAt(index + 1);
    WritePageGuard next_leaf_guard = bpm_->FetchPageWrite(next_leaf_id);
    auto next_leaf_page = next_leaf_guard.AsMut<LeafPage>();
    int size_diff = cur_page->GetSize() - next_leaf_page->GetSize();
    if (size_diff >= 2) {
      cur_page->CopyLastNTo(size_diff / 2, next_leaf_page);
      last_page->SetKeyAt(index + 1, next_leaf_page->KeyAt(0));
      shifted = true;
    }
  }
  if (!shifted && index != 0) {
    page_id_t last_leaf_id = last_page->ValueAt(index - 1);
    WritePageGuard last_leaf_guard = bpm_->FetchPageWrite(last_leaf_id);
    auto last_leaf_page = last_leaf_guard.AsMut<LeafPage>();
    int size_diff = cur_page->GetSize() - last_leaf_page->GetSize();
    if (size_diff >= 2) {
      cur_page->CopyFirstNTo(size_diff / 2, last_leaf_page);
      last_page->SetKeyAt(index, cur_page->KeyAt(0));
      shifted = true;
    }
  }
  return shifted;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ShiftInternalPage(InternalPage *cur_page, InternalPage *last_page, int index) -> bool {
  bool shifted = false;
  if (index != last_page->GetSize() - 1) {
    page_id_t next_internal_id = last_page->ValueAt(index + 1);
    WritePageGuard next_internal_guard = bpm_->FetchPageWrite(next_internal_id);
    auto next_internal_page = next_internal_guard.AsMut<InternalPage>();
    int size_diff = cur_page->GetSize() - next_internal_page->GetSize();
    if (size_diff >= 2) {
      next_internal_page->SetKeyAt(0, last_page->KeyAt(index + 1));
      cur_page->CopyLastNTo(size_diff / 2, next_internal_page);
      last_page->SetKeyAt(index + 1, next_internal_page->KeyAt(0));
      next_internal_page->SetKeyAt(0, KeyType());
      shifted = true;
    }
  }
  if (!shifted && index != 0) {
    page_id_t last_internal_id = last_page->ValueAt(index - 1);
    WritePageGuard last_internal_guard = bpm_->FetchPageWrite(last_internal_id);
    auto last_internal_page = last_internal_guard.AsMut<InternalPage>();
    int size_diff = cur_page->GetSize() - last_internal_page->GetSize();
    if (size_diff >= 2) {
      cur_page->SetKeyAt(0, last_page->KeyAt(index));
      cur_page->CopyFirstNTo(size_diff / 2, last_internal_page);
      last_page->SetKeyAt(index, cur_page->KeyAt(0));
      cur_page->SetKeyAt(0, KeyType());
      shifted = true;
    }
  }
  return shifted;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitLeafPage(LeafPage *cur_page, InternalPage *last_page) -> bool {
  page_id_t new_leaf_id = CreateNewPage(IndexPageType::LEAF_PAGE);
  WritePageGuard new_leaf_guard = bpm_->FetchPageWrite(new_leaf_id);
  auto new_leaf_page = new_leaf_guard.AsMut<LeafPage>();
  cur_page->CopySecondHalfTo(new_leaf_page);
  last_page->InsertData(new_leaf_page->KeyAt(0), new_leaf_id, comparator_);
  new_leaf_page->SetNextPageId(cur_page->GetNextPageId());
  cur_page->SetNextPageId(new_leaf_id);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitInternalPage(InternalPage *cur_page, InternalPage *last_page) -> bool {
  page_id_t new_internal_id = CreateNewPage(IndexPageType::INTERNAL_PAGE);
  WritePageGuard new_internal_guard = bpm_->FetchPageWrite(new_internal_id);
  auto new_internal_page = new_internal_guard.AsMut<InternalPage>();
  last_page->InsertData(cur_page->KeyAt(cur_page->GetSize() / 2), new_internal_id, comparator_);
  cur_page->CopySecondHalfTo(new_internal_page);
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;
  WritePageGuard root_guard = GetRootGuardWrite();
  ctx.write_set_.emplace_back(std::move(root_guard));
  auto res = RemoveInPage(key, &ctx);
  if (res.first) {
    WritePageGuard cur_guard = GetRootGuardWrite();
    auto cur_page = cur_guard.AsMut<BPlusTreePage>();
    if (!cur_page->IsLeafPage() && cur_page->GetSize() == 1) {
      //   page_id_t old_root_id = GetRootPageId();
      auto internal_page = reinterpret_cast<InternalPage *>(cur_page);
      SetNewRoot(internal_page->ValueAt(0));
      //   bpm_->DeletePage(old_root_id);
    }
    return true;
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::RemoveInPage(const KeyType &key, Context *ctx) -> std::pair<bool, KeyType> {
  auto cur_page = ctx->write_set_.back().AsMut<BPlusTreePage>();
  if (cur_page->IsLeafPage()) {
    return RemoveInLeafPage(key, ctx);
  }
  auto internal_page = reinterpret_cast<InternalPage *>(cur_page);
  int next_remove_index = internal_page->GetLastIndexLE(key, comparator_);
  page_id_t next_page_id = internal_page->ValueAt(next_remove_index);
  auto next_guard = bpm_->FetchPageWrite(next_page_id);
  ctx->write_set_.emplace_back(std::move(next_guard));
  auto res = RemoveInPage(key, ctx);
  WritePageGuard cur_guard = std::move(ctx->write_set_.back());
  ctx->write_set_.pop_back();
  if (res.first) {
    if (next_remove_index > 0 && next_remove_index < internal_page->GetSize() &&
        comparator_(internal_page->KeyAt(next_remove_index), key) == 0) {
      internal_page->SetKeyAt(next_remove_index, res.second);
    }
    if (internal_page->SizeNotEnough() && !ctx->write_set_.empty()) {
      auto last_page = ctx->write_set_.back().AsMut<InternalPage>();
      int index = last_page->GetLastIndexLE(key, comparator_);
      if (!ReplenishInternalPage(internal_page, last_page, index) &&
          !CoalesceInternalPage(internal_page, last_page, index)) {
        if (internal_page->GetSize() == 0) {
          last_page->RemoveData(index);
        }
      }
    }
  }
  return res;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::RemoveInLeafPage(const KeyType &key, Context *ctx) -> std::pair<bool, KeyType> {
  auto leaf_page = ctx->write_set_.back().AsMut<LeafPage>();
  int index = leaf_page->RemoveData(key, comparator_);
  if (index == -1) {
    return {false, KeyType()};
  }
  std::pair<bool, KeyType> res = {true, KeyType()};
  if (index == 0 && leaf_page->GetSize() != 0) {
    res.second = leaf_page->KeyAt(index);
  }
  WritePageGuard cur_guard = std::move(ctx->write_set_.back());
  ctx->write_set_.pop_back();
  if (leaf_page->SizeNotEnough() && !ctx->write_set_.empty()) {
    auto last_page = ctx->write_set_.back().AsMut<InternalPage>();
    int index = last_page->GetLastIndexLE(key, comparator_);
    if (!ReplenishLeafPage(leaf_page, last_page, index) && !CoalesceLeafPage(leaf_page, last_page, index)) {
      if (leaf_page->GetSize() == 0) {
        last_page->RemoveData(index);
      }
    }
  }
  return res;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ReplenishLeafPage(LeafPage *cur_page, InternalPage *last_page, int index) -> bool {
  bool replenished = false;
  if (index != last_page->GetSize() - 1) {
    page_id_t next_leaf_id = last_page->ValueAt(index + 1);
    WritePageGuard next_leaf_guard = bpm_->FetchPageWrite(next_leaf_id);
    auto next_leaf_page = next_leaf_guard.AsMut<LeafPage>();
    int size_diff = next_leaf_page->GetSize() - cur_page->GetSize();
    if (size_diff >= 2) {
      next_leaf_page->CopyFirstNTo(size_diff / 2, cur_page);
      last_page->SetKeyAt(index + 1, next_leaf_page->KeyAt(0));
      replenished = true;
    }
  }
  if (!replenished && index != 0) {
    page_id_t last_leaf_id = last_page->ValueAt(index - 1);
    WritePageGuard last_leaf_guard = bpm_->FetchPageWrite(last_leaf_id);
    auto last_leaf_page = last_leaf_guard.AsMut<LeafPage>();
    int size_diff = last_leaf_page->GetSize() - cur_page->GetSize();
    if (size_diff >= 2) {
      last_leaf_page->CopyLastNTo(size_diff / 2, cur_page);
      last_page->SetKeyAt(index, cur_page->KeyAt(0));
      replenished = true;
    }
  }
  return replenished;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ReplenishInternalPage(InternalPage *cur_page, InternalPage *last_page, int index) -> bool {
  bool replenished = false;
  if (index != last_page->GetSize() - 1) {
    page_id_t next_internal_id = last_page->ValueAt(index + 1);
    WritePageGuard next_internal_guard = bpm_->FetchPageWrite(next_internal_id);
    auto next_internal_page = next_internal_guard.AsMut<InternalPage>();
    int size_diff = next_internal_page->GetSize() - cur_page->GetSize();
    if (size_diff >= 2) {
      next_internal_page->SetKeyAt(0, last_page->KeyAt(index + 1));
      next_internal_page->CopyFirstNTo(size_diff / 2, cur_page);
      last_page->SetKeyAt(index + 1, next_internal_page->KeyAt(0));
      next_internal_page->SetKeyAt(0, KeyType());
      replenished = true;
    }
  }
  if (!replenished && index != 0) {
    page_id_t last_internal_id = last_page->ValueAt(index - 1);
    WritePageGuard last_internal_guard = bpm_->FetchPageWrite(last_internal_id);
    auto last_internal_page = last_internal_guard.AsMut<InternalPage>();
    int size_diff = last_internal_page->GetSize() - cur_page->GetSize();
    if (size_diff >= 2) {
      cur_page->SetKeyAt(0, last_page->KeyAt(index + 1));
      last_internal_page->CopyLastNTo(size_diff / 2, cur_page);
      last_page->SetKeyAt(index, cur_page->KeyAt(0));
      cur_page->SetKeyAt(0, KeyType());
      replenished = true;
    }
  }
  return replenished;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CoalesceLeafPage(LeafPage *cur_page, InternalPage *last_page, int index) -> bool {
  bool coalesced = false;
  if (index != last_page->GetSize() - 1) {
    page_id_t next_leaf_id = last_page->ValueAt(index + 1);
    WritePageGuard next_leaf_guard = bpm_->FetchPageWrite(next_leaf_id);
    auto next_leaf_page = next_leaf_guard.AsMut<LeafPage>();
    int size_sum = next_leaf_page->GetSize() + cur_page->GetSize();
    if (size_sum <= leaf_max_size_) {
      next_leaf_page->CopyFirstNTo(next_leaf_page->GetSize(), cur_page);
      last_page->RemoveData(index + 1);
      //   bpm_->DeletePage(next_leaf_id);
      coalesced = true;
    }
  }
  if (!coalesced && index != 0) {
    page_id_t last_leaf_id = last_page->ValueAt(index - 1);
    WritePageGuard last_leaf_guard = bpm_->FetchPageWrite(last_leaf_id);
    auto last_leaf_page = last_leaf_guard.AsMut<LeafPage>();
    int size_sum = last_leaf_page->GetSize() + cur_page->GetSize();
    if (size_sum <= leaf_max_size_) {
      cur_page->CopyFirstNTo(cur_page->GetSize(), last_leaf_page);
      last_page->RemoveData(index);
      //   bpm_->DeletePage(cur_page->);
      coalesced = true;
    }
  }
  return coalesced;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CoalesceInternalPage(InternalPage *cur_page, InternalPage *last_page, int index) -> bool {
  bool coalesced = false;
  if (index != last_page->GetSize() - 1) {
    page_id_t next_internal_id = last_page->ValueAt(index + 1);
    WritePageGuard next_internal_guard = bpm_->FetchPageWrite(next_internal_id);
    auto next_internal_page = next_internal_guard.AsMut<InternalPage>();
    int size_sum = next_internal_page->GetSize() + cur_page->GetSize();
    if (size_sum <= internal_max_size_) {
      next_internal_page->SetKeyAt(0, last_page->RemoveData(index + 1).first);
      next_internal_page->CopyFirstNTo(next_internal_page->GetSize(), cur_page);
      //   bpm_->DeletePage(next_leaf_id);
      coalesced = true;
    }
  }
  if (!coalesced && index != 0) {
    page_id_t last_internal_id = last_page->ValueAt(index - 1);
    WritePageGuard last_internal_guard = bpm_->FetchPageWrite(last_internal_id);
    auto last_internal_page = last_internal_guard.AsMut<InternalPage>();
    int size_sum = last_internal_page->GetSize() + cur_page->GetSize();
    if (size_sum <= internal_max_size_) {
      cur_page->SetKeyAt(0, last_page->RemoveData(index).first);
      cur_page->CopyFirstNTo(cur_page->GetSize(), last_internal_page);
      //   bpm_->DeletePage(cur_page->);
      coalesced = true;
    }
  }
  return coalesced;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*****************************************************************************
 * BASIC OPERATIONS
 *****************************************************************************/

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = guard.As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
