#include <algorithm>
#include <sstream>
#include <string>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

/**
 * @brief The constructor of BPlusTree (thread-safe)
 *
 * @param name The index name of the BPlusTree, not related to the file name, which is stored in BufferPoolManager
 * @param header_page_id The page id of the existed header page, which will be created by the constructor of
BPlusTreeIndex or other functions.
 * @param buffer_pool_manager The buffer pool manager using in bpt
 * @param comparator The default comparator
 * @param leaf_max_size The maximum leaf page size
 * @param internal_max_size The maximum internal page size
 * @param inherit_file The tag of file inherit that determines whether to reset root page id
 */
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size, bool inherit_file)
    : index_name_(std::move(name)),
      inherit_file_(inherit_file),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
  if (!inherit_file) {  // Reset the root page id
    header_page->root_page_id_ = INVALID_PAGE_ID;
  }
}

/**
 * @brief The destructor of BPlusTree (thread-safe)
 * It resets the root_page_id if inherit_file_ tag is true
 */
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::~BPlusTree() {
  if (inherit_file_) {
    auto header_guard = bpm_->FetchPageBasic(header_page_id_);
    header_guard.AsMut<BPlusTreeHeaderPage>()->root_page_id_ = INVALID_PAGE_ID;
  }
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  ReadPageGuard root_guard = GetRootGuardRead();  // Try to get the root guard
  return !root_guard.Exist() || root_guard.As<BPlusTreePage>()->GetSize() == 0;
}

/**
 * @brief Create a new page with the given page type.
 *
 * @param page_type The type of the new page
 * @return The page id of the new page
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CreateNewPage(IndexPageType page_type) -> page_id_t {
  page_id_t new_page_id;
  bpm_->NewPageGuarded(&new_page_id);
  if (new_page_id == INVALID_PAGE_ID) {
    return new_page_id;
  }
  WritePageGuard page_guard = bpm_->FetchPageWrite(new_page_id);
  // The type and initialization of new page
  if (page_type == IndexPageType::INTERNAL_PAGE) {
    auto page = page_guard.AsMut<InternalPage>();
    page->Init(internal_max_size_);
  } else {
    auto page = page_guard.AsMut<LeafPage>();
    page->Init(leaf_max_size_);
  }
  return new_page_id;
}

/**
 * @brief Create a new root with the given page type by header page.
 *
 * @param page_type The type of the new root
 * @param header_page
 * @return The page id of the new root
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CreateNewRoot(IndexPageType page_type, BPlusTreeHeaderPage *header_page) -> page_id_t {
  header_page->root_page_id_ = CreateNewPage(page_type);
  return header_page->root_page_id_;
}

/**
 * @brief Set root id to new root id by the given header page.
 *
 * @param new_root_id The page id of the new root
 * @param header_page
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SetNewRoot(page_id_t new_root_id, BPlusTreeHeaderPage *header_page) {
  header_page->root_page_id_ = new_root_id;
}

/**
 * @brief Get the root write guard, with the read guard of HeaderPage required.
 *
 * @param ctx The context to write header guard into
 * @param create_new_root Whether to create a new root if there's no root
 * @return The root writeguard
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootGuardWrite(Context *ctx, bool create_new_root) -> WritePageGuard {
  // Get the header guard
  WritePageGuard header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {  // There's no root
    if (!create_new_root) {                             // Return a guard that doesn't exist
      return {nullptr, nullptr};
    }
    CreateNewRoot(IndexPageType::LEAF_PAGE, header_page);
  }
  auto root_guard = bpm_->FetchPageWrite(header_page->root_page_id_);
  // Write the header guard and root page id into ctx
  ctx->root_page_id_ = header_page->root_page_id_;
  ctx->header_page_.emplace(std::move(header_guard));
  return root_guard;
}

/**
 * @brief Fetch the root write guard from the given context.
 *
 * @param ctx The context which stored the header guard
 * @return The root writeguard
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FetchRootGuardWrite(Context *ctx) -> WritePageGuard {
  if (!ctx->header_page_.has_value()) {  // If the header_page isn't stored
    return {nullptr, nullptr};
  }
  auto header_page = ctx->header_page_->As<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    return {nullptr, nullptr};
  }
  auto root_guard = bpm_->FetchPageWrite(header_page->root_page_id_);
  return root_guard;
}

/**
 * @brief Get the root read guard, with the read guard of HeaderPage required.
 *
 * @return The root readguard
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootGuardRead(Context *ctx) const -> ReadPageGuard {
  ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    return {nullptr, nullptr};
  }
  if (ctx != nullptr) {
    ctx->root_page_id_ = header_page->root_page_id_;
  }
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
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, vector<ValueType> *result, Transaction *txn) -> bool {
  return GetValue(key, result, comparator_, txn);  // Compare by default comparator
}

/**
 * @brief Return all the values associated with the given key and comparator.
 *
 * @param key
 * @param result
 * @param comparator
 * @param txn
 * @return true if key exists
 * @return false otherwise
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, vector<ValueType> *result, const KeyComparator &comparator,
                              Transaction *txn) -> bool {
  // std::cout << "Searching for key " << key << std::endl;  // Debug message
  BUSTUB_ENSURE(result->empty(), "The result array should be empty.");
  // Declaration of context instance.
  Context ctx;
  ReadPageGuard root_guard = GetRootGuardRead();  // Only the read guard is required
  if (!root_guard.Exist()) {                      // If there's no root
    return false;
  }
  ctx.read_set_.emplace_back(std::move(root_guard));
  return GetValueInPage(key, result, &ctx, comparator);
}

/**
 * @brief Get all the values associated with the given key in current internal page, stored in back of ctx->read_set.
 *
 * @param key
 * @param result
 * @param ctx
 * @param comparator
 * @return true if key exists
 * @return false otherwise
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValueInPage(const KeyType &key, vector<ValueType> *result, Context *ctx,
                                    const KeyComparator &comparator) -> bool {
  auto cur_page = ctx->read_set_.back().As<BPlusTreePage>();
  if (cur_page->IsLeafPage()) {  // Current page is leaf page
    return GetValueInLeafPage(key, result, ctx, comparator);
  }
  auto internal_page = reinterpret_cast<const InternalPage *>(cur_page);
  int next_search_index = internal_page->GetLastIndexL(
      key,
      comparator);  // Last key that is less than the given key.
                    // If you want to directly access the same key, please refer to Begin(const Key &) -> IndexIterator.
  page_id_t next_page_id = internal_page->ValueAt(next_search_index);
  ctx->read_set_.pop_back();  // Release the read guard immediately
  auto next_guard = bpm_->FetchPageRead(next_page_id);
  ctx->read_set_.emplace_back(std::move(next_guard));
  return GetValueInPage(key, result, ctx, comparator);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValueInLeafPage(const KeyType &key, vector<ValueType> *result, Context *ctx,
                                        const KeyComparator &comparator) -> bool {
  auto leaf_page = ctx->read_set_.back().As<LeafPage>();
  int index = leaf_page->GetLastIndexL(key, comparator) + 1;  // Same as above
  int size = leaf_page->GetSize();
  for (; index < size; index++) {
    if (comparator(leaf_page->KeyAt(index), key) > 0) {
      break;
    }
    result->push_back(leaf_page->ValueAt(index));
  }
  if (index == size) {  // Reach the end of current page
    page_id_t next_leaf_id = leaf_page->GetNextPageId();
    if (next_leaf_id != INVALID_PAGE_ID) {  // Reach the last page
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
  std::cout << "Inserting key " << key << " optimistically\n";  // Debug message
  auto insert_opt_res = InsertOptimistic(key, value);
  std::cout << "Finished.\n";  // Debug message
  if (insert_opt_res.second) {
    return insert_opt_res.first;
  }
  std::cout << "Inserting key " << key << '\n';
  // Declaration of context instance
  Context ctx;
  WritePageGuard root_guard = GetRootGuardWrite(&ctx, true);  // Get root guard and store header guard in ctx
  ctx.write_set_.emplace_back(std::move(root_guard));
  auto insert_res = InsertIntoPage(key, value, &ctx, -1);
  if (insert_res.second) {  // If the ctx has been cleared, which contains the situation that the key already exists
    std::cout << "Finished.\n";
    return insert_res.first;
  }
  WritePageGuard cur_guard = FetchRootGuardWrite(&ctx);  // If the insert isn't safe
  auto cur_page = cur_guard.AsMut<BPlusTreePage>();
  if (cur_page->SizeExceeded()) {  // The root page should be split
    auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
    page_id_t root_id = CreateNewRoot(IndexPageType::INTERNAL_PAGE, header_page);
    WritePageGuard root_guard = bpm_->FetchPageWrite(root_id);
    auto root_page = root_guard.AsMut<InternalPage>();
    root_page->IncreaseSize(1);
    root_page->SetValueAt(0, ctx.root_page_id_);  // Init the new root page
    if (cur_page->IsLeafPage()) {
      SplitLeafPage(reinterpret_cast<LeafPage *>(cur_page), root_page);
    } else {
      SplitInternalPage(reinterpret_cast<InternalPage *>(cur_page), root_page);
    }
  }
  std::cout << "Finished.\n";
  return true;
}

/**
 * @brief Insert data optimistically into leaf, assuming that only leaf page will be edited.
 *
 * @param key
 * @param value
 * @return std::pair<bool, bool> with its first value representing whether the key is valid, second value representing
 * the whether the insertion is optimistic
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertOptimistic(const KeyType &key, const ValueType &value) -> std::pair<bool, bool> {
  Context ctx;
  ReadPageGuard cur_guard = GetRootGuardRead(&ctx);
  if (!cur_guard.Exist()) {
    return {true, false};
  }
  auto cur_pid = ctx.root_page_id_;
  auto cur_page = cur_guard.As<BPlusTreePage>();
  while (!cur_page->IsLeafPage()) {
    auto internal_page = reinterpret_cast<const InternalPage *>(cur_page);
    if (!internal_page->IsInsertSafe()) {
      return {true, false};
    }
    int next_insert_index =
        internal_page->GetLastIndexLE(key, comparator_);  // Find the last key that is less or equal to the given key
    cur_pid = internal_page->ValueAt(next_insert_index);
    cur_guard = bpm_->FetchPageRead(cur_pid);
    cur_page = cur_guard.As<BPlusTreePage>();
  }
  cur_guard.Drop();

  WritePageGuard leaf_guard = bpm_->FetchPageWrite(cur_pid);
  auto leaf_page = leaf_guard.AsMut<LeafPage>();
  if (!leaf_page->IsInsertSafe() || comparator_(leaf_page->KeyAt(0), key) > 0) {
    return {true, false};
  }
  bool insert_res = leaf_page->InsertData(key, value, comparator_) != -1;
  return {insert_res, true};
}

/**
 * @brief Insert data into current internal page, stored in back of ctx->write_set.
 *
 * @param key
 * @param value
 * @param ctx
 * @param index
 * @return std::pair<bool, bool> with its first value representing the final insertion result, second value representing
 * whether ctx has been cleared.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoPage(const KeyType &key, const ValueType &value, Context *ctx, int index)
    -> std::pair<bool, bool> {
  auto cur_page = ctx->write_set_.back().AsMut<BPlusTreePage>();
  if (cur_page->IsLeafPage()) {
    return InsertIntoLeafPage(key, value, ctx, index);
  }
  auto internal_page = reinterpret_cast<InternalPage *>(cur_page);
  int next_insert_index =
      internal_page->GetLastIndexLE(key, comparator_);  // Find the last key that is less or equal to the given key
  page_id_t next_page_id = internal_page->ValueAt(next_insert_index);
  if (internal_page
          ->IsInsertSafe()) {  // If current page can absorb insert, the write_set_ could contain the current guard only
    WritePageGuard cur_guard = std::move(ctx->write_set_.back());
    ctx->header_page_.reset();
    ctx->write_set_.clear();
    ctx->write_set_.emplace_back(std::move(cur_guard));
  }
  auto next_guard = bpm_->FetchPageWrite(next_page_id);
  ctx->write_set_.emplace_back(std::move(next_guard));
  auto insert_res = InsertIntoPage(key, value, ctx, next_insert_index);
  if (!insert_res.first) {  // If insertion failed
    BUSTUB_ENSURE(ctx->write_set_.empty(), "Write set should be cleared.");
    BUSTUB_ENSURE(!ctx->header_page_.has_value(), "Header guard should be released.");
    return {false, true};
  }
  bool insert_safe_tag = true;
  if (!insert_res.second) {  // If the next step of insertion isn't safe, i.e. it inserted data into current page, then
                             // we should check the size
    WritePageGuard cur_guard = std::move(
        ctx->write_set_
            .back());  // If all the insertion below isn't insert safe, the current page must be still in write_set.
    ctx->write_set_.pop_back();
    if (internal_page->SizeExceeded()) {
      if (!ctx->write_set_.empty()) {  // If current page isn't root and its size exceeds
        auto last_page = ctx->write_set_.back().AsMut<InternalPage>();
        if (!ShiftInternalPage(internal_page, last_page, index)) {
          SplitInternalPage(internal_page, last_page);
          insert_safe_tag = false;
        }
      } else {
        insert_safe_tag = false;
      }
    }
    if (insert_safe_tag) {  // If the insert is safe but ctx wasn't cleared
      ctx->write_set_.clear();
      ctx->header_page_.reset();
    }
  }
  return {insert_res.first, insert_safe_tag | insert_res.second};
}

/**
 * @brief Insert data into current leaf page, stored in back of ctx->write_set.
 *
 * @param key
 * @param value
 * @param ctx
 * @param index
 * @return std::pair<bool, bool> with its first value representing the final insertion result, second value representing
 * whether ctx has been cleared.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoLeafPage(const KeyType &key, const ValueType &value, Context *ctx, int index)
    -> std::pair<bool, bool> {
  auto leaf_page = ctx->write_set_.back().AsMut<LeafPage>();
  bool insert_res = leaf_page->InsertData(key, value, comparator_) != -1;
  if (!insert_res) {  // If the insertion failed, clear all the context
    ctx->write_set_.clear();
    ctx->header_page_.reset();
    return {false, true};
  }
  WritePageGuard cur_guard = std::move(ctx->write_set_.back());
  ctx->write_set_.pop_back();
  bool insert_safe_tag = true;
  if (leaf_page->SizeExceeded()) {
    if (!ctx->write_set_.empty()) {
      auto last_page = ctx->write_set_.back().AsMut<InternalPage>();
      if (!ShiftLeafPage(leaf_page, last_page, index)) {
        SplitLeafPage(leaf_page, last_page);
        insert_safe_tag = false;
      }
    } else {
      insert_safe_tag = false;
    }
  }
  if (insert_safe_tag) {
    ctx->write_set_.clear();
    ctx->header_page_.reset();
  }
  return {true, insert_safe_tag};
}

/**
 * @brief Shift data between leaf page, i.e. send some data away from current page.
 *
 * @param cur_page
 * @param last_page
 * @param index
 * @return true if an adjacent page is available
 * @return false otherwise
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ShiftLeafPage(LeafPage *cur_page, InternalPage *last_page, int index) -> bool {
  bool shifted = false;                     // Whether the data is shifted
  if (index != last_page->GetSize() - 1) {  // Try to shift into the next page
    page_id_t next_leaf_id = last_page->ValueAt(index + 1);
    WritePageGuard next_leaf_guard = bpm_->FetchPageWrite(next_leaf_id);
    auto next_leaf_page = next_leaf_guard.AsMut<LeafPage>();
    int size_diff = cur_page->GetSize() - next_leaf_page->GetSize();
    if (size_diff >= 2) {  // Satisfy
      cur_page->CopyLastNTo(size_diff / 2, next_leaf_page);
      last_page->SetKeyAt(index + 1, next_leaf_page->KeyAt(0));  // Change the key
      shifted = true;
    }
  }
  if (!shifted && index != 0) {  // Try to shift into the last page
    page_id_t last_leaf_id = last_page->ValueAt(index - 1);
    WritePageGuard last_leaf_guard = bpm_->FetchPageWrite(last_leaf_id);
    auto last_leaf_page = last_leaf_guard.AsMut<LeafPage>();
    int size_diff = cur_page->GetSize() - last_leaf_page->GetSize();
    if (size_diff >= 2) {  // Satisfy
      cur_page->CopyFirstNTo(size_diff / 2, last_leaf_page);
      last_page->SetKeyAt(index, cur_page->KeyAt(0));  // Change the key
      shifted = true;
    }
  }
  return shifted;
}

/**
 * @brief Shift data between internal page, i.e. send some data away from current page.
 *
 * @param cur_page
 * @param last_page
 * @param index
 * @return true if an adjacent page is available
 * @return false otherwise
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ShiftInternalPage(InternalPage *cur_page, InternalPage *last_page, int index) -> bool {
  bool shifted = false;
  if (index != last_page->GetSize() - 1) {
    page_id_t next_internal_id = last_page->ValueAt(index + 1);
    WritePageGuard next_internal_guard = bpm_->FetchPageWrite(next_internal_id);
    auto next_internal_page = next_internal_guard.AsMut<InternalPage>();
    int size_diff = cur_page->GetSize() - next_internal_page->GetSize();
    if (size_diff >= 2) {
      next_internal_page->SetKeyAt(0, last_page->KeyAt(index + 1));  // Store the key at a temporary place
      cur_page->CopyLastNTo(size_diff / 2, next_internal_page);      // Copy the keys
      last_page->SetKeyAt(index + 1, next_internal_page->KeyAt(0));  // Change the key
      next_internal_page->SetKeyAt(0, KeyType());                    // Clear
      shifted = true;
    }
  }
  if (!shifted && index != 0) {
    page_id_t last_internal_id = last_page->ValueAt(index - 1);
    WritePageGuard last_internal_guard = bpm_->FetchPageWrite(last_internal_id);
    auto last_internal_page = last_internal_guard.AsMut<InternalPage>();
    int size_diff = cur_page->GetSize() - last_internal_page->GetSize();
    if (size_diff >= 2) {
      cur_page->SetKeyAt(0, last_page->KeyAt(index));  // Same as above
      cur_page->CopyFirstNTo(size_diff / 2, last_internal_page);
      last_page->SetKeyAt(index, cur_page->KeyAt(0));
      cur_page->SetKeyAt(0, KeyType());
      shifted = true;
    }
  }
  return shifted;
}

/**
 * @brief Split current leaf page into two pages.
 *
 * @param cur_page
 * @param last_page
 * @return true if split succeeds
 * @return false otherwise
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitLeafPage(LeafPage *cur_page, InternalPage *last_page) -> bool {
  page_id_t new_leaf_id = CreateNewPage(IndexPageType::LEAF_PAGE);  // Create a new leaf page
  if (new_leaf_id == INVALID_PAGE_ID) {
    return false;
  }
  WritePageGuard new_leaf_guard = bpm_->FetchPageWrite(new_leaf_id);
  auto new_leaf_page = new_leaf_guard.AsMut<LeafPage>();
  cur_page->CopySecondHalfTo(new_leaf_page);  // Copy the second half to the new page
  last_page->InsertData(new_leaf_page->KeyAt(0), new_leaf_id, comparator_);
  new_leaf_page->SetNextPageId(cur_page->GetNextPageId());
  cur_page->SetNextPageId(new_leaf_id);
  return true;
}

/**
 * @brief Split current internal page into two pages.
 *
 * @param cur_page
 * @param last_page
 * @return true if split succeeds
 * @return false otherwise
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitInternalPage(InternalPage *cur_page, InternalPage *last_page) -> bool {
  page_id_t new_internal_id = CreateNewPage(IndexPageType::INTERNAL_PAGE);
  if (new_internal_id == INVALID_PAGE_ID) {
    return false;
  }
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
  std::cout << "Removing key " << key << "\n";  // debug message
  auto remove_opt_res = RemoveOptimistic(key);
  if (remove_opt_res.second) {
    return remove_opt_res.first;
  }
  // Declaration of context instance.
  Context ctx;
  WritePageGuard root_guard = GetRootGuardWrite(&ctx);  // Get root guard and store header guard in ctx
  if (!root_guard.Exist()) {                            // If the tree is empty
    return false;
  }
  ctx.write_set_.emplace_back(std::move(root_guard));
  auto remove_res = RemoveInPage(key, &ctx, -1);
  if (!remove_res.first) {  // If the key doesn't exist
    return false;
  }
  WritePageGuard cur_guard = FetchRootGuardWrite(&ctx);
  auto cur_page = cur_guard.AsMut<BPlusTreePage>();
  if (!cur_page->IsLeafPage() && cur_page->GetSize() == 1) {  // The root page has only one child
    auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
    auto internal_page = reinterpret_cast<InternalPage *>(cur_page);
    SetNewRoot(internal_page->ValueAt(0), header_page);  // Set its child as the root
  }
  std::cout << "Finished.\n";
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::RemoveOptimistic(const KeyType &key) -> std::pair<bool, bool> {
  Context ctx;
  ReadPageGuard cur_guard = GetRootGuardRead(&ctx);
  if (!cur_guard.Exist()) {
    return {false, true};
  }
  auto cur_pid = ctx.root_page_id_;
  auto cur_page = cur_guard.As<BPlusTreePage>();
  while (!cur_page->IsLeafPage()) {
    auto internal_page = reinterpret_cast<const InternalPage *>(cur_page);
    if ((cur_pid == ctx.root_page_id_ && internal_page->GetSize() == 1) ||
        (cur_pid != ctx.root_page_id_ && !internal_page->IsRemoveSafe())) {
      return {true, false};
    }
    int next_remove_index =
        internal_page->GetLastIndexLE(key, comparator_);  // Find the last key that is less or equal to the given key
    if (next_remove_index != 0 && comparator_(internal_page->KeyAt(next_remove_index), key) == 0) {
      return {true, false};
    }
    cur_pid = internal_page->ValueAt(next_remove_index);
    cur_guard = bpm_->FetchPageRead(cur_pid);
    cur_page = cur_guard.As<BPlusTreePage>();
  }
  cur_guard.Drop();

  WritePageGuard leaf_guard = bpm_->FetchPageWrite(cur_pid);
  auto leaf_page = leaf_guard.AsMut<LeafPage>();
  if (!leaf_page->IsRemoveSafe() || comparator_(leaf_page->KeyAt(0), key) == 0) {
    return {true, false};
  }
  bool remove_res = leaf_page->RemoveData(key, comparator_) != -1;
  return {remove_res, true};
}

/**
 * @brief Remove the data assoicated with the given key in current internal page, stored in back of ctx->write_set.
 *
 * @param key
 * @param ctx
 * @param index
 * @return std::pair<bool, KeyType> with its first value representing the removing result, second value stored the next
 * key for replacement.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::RemoveInPage(const KeyType &key, Context *ctx, int index) -> std::pair<bool, KeyType> {
  auto cur_page = ctx->write_set_.back().AsMut<BPlusTreePage>();
  if (cur_page->IsLeafPage()) {
    return RemoveInLeafPage(key, ctx, index);
  }
  auto internal_page = reinterpret_cast<InternalPage *>(cur_page);
  int next_remove_index =
      internal_page->GetLastIndexLE(key, comparator_);  // Find the last key that is less or equal to the given key
  page_id_t next_page_id = internal_page->ValueAt(next_remove_index);
  auto next_guard = bpm_->FetchPageWrite(next_page_id);
  ctx->write_set_.emplace_back(std::move(next_guard));
  auto res = RemoveInPage(key, ctx, next_remove_index);
  WritePageGuard cur_guard = std::move(ctx->write_set_.back());
  ctx->write_set_.pop_back();
  if (res.first) {
    if (next_remove_index > 0 && next_remove_index < internal_page->GetSize() &&
        comparator_(internal_page->KeyAt(next_remove_index), key) == 0) {  // If current key should be replaced
      internal_page->SetKeyAt(next_remove_index, res.second);
    }
    if (internal_page->SizeNotEnough() && !ctx->write_set_.empty()) {  // If current page doesn't have enough size
      auto last_page = ctx->write_set_.back().AsMut<InternalPage>();
      if (!ReplenishInternalPage(internal_page, last_page, index)) {
        CoalesceInternalPage(internal_page, last_page, index);
      }
    }
  }
  return res;
}

/**
 * @brief Remove the data assoicated with the given key in current leaf page, stored in back of ctx->write_set.
 *
 * @param key
 * @param ctx
 * @param index
 * @return std::pair<bool, KeyType> with its first value representing the removing result, second value stored the next
 * key for replacement.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::RemoveInLeafPage(const KeyType &key, Context *ctx, int index) -> std::pair<bool, KeyType> {
  auto leaf_page = ctx->write_set_.back().AsMut<LeafPage>();
  int remove_index = leaf_page->RemoveData(key, comparator_);  // The index of the key to remove
  if (remove_index == -1) {                                    // Doesn't exist
    return {false, KeyType()};
  }
  std::pair<bool, KeyType> res = {true, KeyType()};
  if (remove_index == 0 && leaf_page->GetSize() != 0) {  // Find the replacement key
    res.second = leaf_page->KeyAt(remove_index);
  }
  WritePageGuard cur_guard = std::move(ctx->write_set_.back());
  ctx->write_set_.pop_back();
  if (leaf_page->SizeNotEnough() && !ctx->write_set_.empty()) {  // If current page doesn't have enough size
    auto last_page = ctx->write_set_.back().AsMut<InternalPage>();
    if (!ReplenishLeafPage(leaf_page, last_page, index)) {
      CoalesceLeafPage(leaf_page, last_page, index);
    }
  }
  return res;
}

/**
 * @brief Replenish current leaf page, i.e. borrow some data to current page.
 *
 * @param cur_page
 * @param last_page
 * @param index
 * @return true if an adjacent page is available
 * @return false otherwise
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ReplenishLeafPage(LeafPage *cur_page, InternalPage *last_page, int index) -> bool {
  bool replenished = false;                 // Whether the page is replenished
  if (index != last_page->GetSize() - 1) {  // Try to borrow data from the next page
    page_id_t next_leaf_id = last_page->ValueAt(index + 1);
    WritePageGuard next_leaf_guard = bpm_->FetchPageWrite(next_leaf_id);
    auto next_leaf_page = next_leaf_guard.AsMut<LeafPage>();
    int size_diff = next_leaf_page->GetSize() - cur_page->GetSize();
    if (size_diff >= 2) {
      next_leaf_page->CopyFirstNTo(size_diff / 2, cur_page);
      last_page->SetKeyAt(index + 1, next_leaf_page->KeyAt(0));  // Change the key
      replenished = true;
    }
  }
  if (!replenished && index != 0) {  // Try to borrow data from the last page
    page_id_t last_leaf_id = last_page->ValueAt(index - 1);
    WritePageGuard last_leaf_guard = bpm_->FetchPageWrite(last_leaf_id);
    auto last_leaf_page = last_leaf_guard.AsMut<LeafPage>();
    int size_diff = last_leaf_page->GetSize() - cur_page->GetSize();
    if (size_diff >= 2) {
      last_leaf_page->CopyLastNTo(size_diff / 2, cur_page);
      last_page->SetKeyAt(index, cur_page->KeyAt(0));  // Change key
      replenished = true;
    }
  }
  return replenished;
}

/**
 * @brief Replenish current internal page, i.e. borrow some data to current page.
 *
 * @param cur_page
 * @param last_page
 * @param index
 * @return true if an adjacent page is available
 * @return false otherwise
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ReplenishInternalPage(InternalPage *cur_page, InternalPage *last_page, int index) -> bool {
  bool replenished = false;
  if (index != last_page->GetSize() - 1) {
    page_id_t next_internal_id = last_page->ValueAt(index + 1);
    WritePageGuard next_internal_guard = bpm_->FetchPageWrite(next_internal_id);
    auto next_internal_page = next_internal_guard.AsMut<InternalPage>();
    int size_diff = next_internal_page->GetSize() - cur_page->GetSize();
    if (size_diff >= 2) {
      next_internal_page->SetKeyAt(0, last_page->KeyAt(index + 1));  // Store the key at a temporary place
      next_internal_page->CopyFirstNTo(size_diff / 2, cur_page);     // Copy the keys
      last_page->SetKeyAt(index + 1, next_internal_page->KeyAt(0));  // Change the key
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
      cur_page->SetKeyAt(0, last_page->KeyAt(index));  // Same as above
      last_internal_page->CopyLastNTo(size_diff / 2, cur_page);
      last_page->SetKeyAt(index, cur_page->KeyAt(0));
      cur_page->SetKeyAt(0, KeyType());
      replenished = true;
    }
  }
  return replenished;
}

/**
 * @brief Coalesce current leaf page with an adjacent page.
 *
 * @param cur_page
 * @param last_page
 * @param index
 * @return true if split succeeds
 * @return false otherwise
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CoalesceLeafPage(LeafPage *cur_page, InternalPage *last_page, int index) -> bool {
  bool coalesced = false;
  if (index != last_page->GetSize() - 1) {  // Coalescec with the next page
    page_id_t next_leaf_id = last_page->ValueAt(index + 1);
    WritePageGuard next_leaf_guard = bpm_->FetchPageWrite(next_leaf_id);
    auto next_leaf_page = next_leaf_guard.AsMut<LeafPage>();
    int size_sum = next_leaf_page->GetSize() + cur_page->GetSize();
    if (size_sum <= leaf_max_size_) {
      next_leaf_page->CopyFirstNTo(next_leaf_page->GetSize(), cur_page);  // Copy all the data to current page
      last_page->RemoveData(index + 1);
      cur_page->SetNextPageId(next_leaf_page->GetNextPageId());  // Update next_page_id
      bpm_->DeletePage(next_leaf_id);
      coalesced = true;
    }
  }
  if (!coalesced && index != 0) {
    page_id_t last_leaf_id = last_page->ValueAt(index - 1);
    WritePageGuard last_leaf_guard = bpm_->FetchPageWrite(last_leaf_id);
    auto last_leaf_page = last_leaf_guard.AsMut<LeafPage>();
    int size_sum = last_leaf_page->GetSize() + cur_page->GetSize();
    if (size_sum <= leaf_max_size_) {
      cur_page->CopyFirstNTo(cur_page->GetSize(), last_leaf_page);  // Copy all the data to last page
      auto remove_res = last_page->RemoveData(index);
      last_leaf_page->SetNextPageId(cur_page->GetNextPageId());  // Update next_page_id
      bpm_->DeletePage(remove_res.second);
      coalesced = true;
    }
  }
  return coalesced;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CoalesceInternalPage(InternalPage *cur_page, InternalPage *last_page, int index) -> bool {
  bool coalesced = false;  // Same as above except the next_page_id part
  if (index != last_page->GetSize() - 1) {
    page_id_t next_internal_id = last_page->ValueAt(index + 1);
    WritePageGuard next_internal_guard = bpm_->FetchPageWrite(next_internal_id);
    auto next_internal_page = next_internal_guard.AsMut<InternalPage>();
    int size_sum = next_internal_page->GetSize() + cur_page->GetSize();
    if (size_sum <= internal_max_size_) {
      next_internal_page->SetKeyAt(0, last_page->RemoveData(index + 1).first);
      next_internal_page->CopyFirstNTo(next_internal_page->GetSize(), cur_page);
      bpm_->DeletePage(next_internal_id);
      coalesced = true;
    }
  }
  if (!coalesced && index != 0) {
    page_id_t last_internal_id = last_page->ValueAt(index - 1);
    WritePageGuard last_internal_guard = bpm_->FetchPageWrite(last_internal_id);
    auto last_internal_page = last_internal_guard.AsMut<InternalPage>();
    int size_sum = last_internal_page->GetSize() + cur_page->GetSize();
    if (size_sum <= internal_max_size_) {
      auto remove_res = last_page->RemoveData(index);
      cur_page->SetKeyAt(0, remove_res.first);
      cur_page->CopyFirstNTo(cur_page->GetSize(), last_internal_page);
      bpm_->DeletePage(remove_res.second);
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
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  // std::cout << "Using begin.\n"; // debug message
  page_id_t next_page_id = GetRootPageId();
  if (next_page_id == INVALID_PAGE_ID) {
    return End();
  }
  ReadPageGuard cur_guard = bpm_->FetchPageRead(next_page_id);
  auto cur_page = cur_guard.As<BPlusTreePage>();
  while (!cur_page->IsLeafPage()) {                                               // Current page isn't leaf page
    next_page_id = reinterpret_cast<const InternalPage *>(cur_page)->ValueAt(0);  // The first child page
    cur_guard = bpm_->FetchPageRead(next_page_id);
    cur_page = cur_guard.As<BPlusTreePage>();
  }
  if (cur_page->GetSize() == 0) {
    return End();
  }
  return INDEXITERATOR_TYPE(next_page_id, 0, bpm_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  // std::cout << "Using begin " << key << '\n'; // debug message
  page_id_t next_page_id = GetRootPageId();
  if (next_page_id == INVALID_PAGE_ID) {
    return End();
  }
  ReadPageGuard cur_guard = bpm_->FetchPageRead(next_page_id);
  auto cur_page = cur_guard.As<BPlusTreePage>();
  while (!cur_page->IsLeafPage()) {
    next_page_id = reinterpret_cast<const InternalPage *>(cur_page)->GetLastIndexLE(key, comparator_);
    cur_guard = bpm_->FetchPageRead(next_page_id);
    cur_page = cur_guard.As<BPlusTreePage>();
  }
  if (cur_page->GetSize() == 0) {
    return End();
  }
  int index = reinterpret_cast<const LeafPage *>(cur_page)->GetLastIndexLE(key, comparator_);
  if (index == -1) {
    return End();
  }
  return INDEXITERATOR_TYPE(next_page_id, index, bpm_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  // std::cout << "Using end.\n"; // debug message
  return INDEXITERATOR_TYPE(INVALID_PAGE_ID, 0, bpm_);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::First(const KeyType &key, const KeyComparator &comparator) -> INDEXITERATOR_TYPE {
  // std::cout << "Using first " << key << '\n'; // debug message
  page_id_t next_page_id = GetRootPageId();
  if (next_page_id == INVALID_PAGE_ID) {
    return End();
  }
  ReadPageGuard cur_guard = bpm_->FetchPageRead(next_page_id);
  auto cur_page = cur_guard.As<BPlusTreePage>();
  while (!cur_page->IsLeafPage()) {
    next_page_id = reinterpret_cast<const InternalPage *>(cur_page)->GetLastIndexL(key, comparator);
    cur_guard = bpm_->FetchPageRead(next_page_id);
    cur_page = cur_guard.As<BPlusTreePage>();
  }
  if (cur_page->GetSize() == 0) {
    return End();
  }
  auto leaf_page = reinterpret_cast<const LeafPage *>(cur_page);
  int index = leaf_page->GetLastIndexL(key, comparator) + 1;
  if (index < leaf_page->GetSize()) {
    if (comparator(leaf_page->KeyAt(index), key) == 0) {
      return INDEXITERATOR_TYPE(next_page_id, index, bpm_);
    }
    return End();
  }
  next_page_id = leaf_page->GetNextPageId();
  if (next_page_id == INVALID_PAGE_ID) {
    return End();
  }
  cur_guard = bpm_->FetchPageRead(next_page_id);
  leaf_page = cur_guard.As<LeafPage>();
  if (comparator(leaf_page->KeyAt(0), key) == 0) {
    return INDEXITERATOR_TYPE(next_page_id, 0, bpm_);
  }
  return End();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Find(const KeyType &key) -> INDEXITERATOR_TYPE {
  // std::cout << "Using find " << key << '\n'; // debug message
  page_id_t next_page_id = GetRootPageId();
  if (next_page_id == INVALID_PAGE_ID) {
    return End();
  }
  ReadPageGuard cur_guard = bpm_->FetchPageRead(next_page_id);
  auto cur_page = cur_guard.As<BPlusTreePage>();
  while (!cur_page->IsLeafPage()) {
    next_page_id = reinterpret_cast<const InternalPage *>(cur_page)->GetLastIndexLE(key, comparator_);
    cur_guard = bpm_->FetchPageRead(next_page_id);
    cur_page = cur_guard.As<BPlusTreePage>();
  }
  if (cur_page->GetSize() == 0) {
    return End();
  }
  auto leaf_page = reinterpret_cast<const LeafPage *>(cur_page);
  int index = leaf_page->GetLastIndexLE(key, comparator_);
  if (index == -1 || comparator_(leaf_page->KeyAt(index), key) != 0) {
    return End();
  }
  return INDEXITERATOR_TYPE(next_page_id, index, bpm_);
}

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
    Insert(index_key, *reinterpret_cast<ValueType *>(&rid), txn);
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

}  // namespace bustub

#ifdef CUSTOMIZED_BUSTUB
#include "storage/index/custom_key.h"
BUSTUB_DECLARE(BPlusTree)
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
BUSTUB_DECLARE(BPlusTree)
#endif
