#pragma once

#include "storage/index/b_plus_tree.h"

namespace bustub {

#define BPLUSTREENTS_TYPE BPlusTree<KeyType, ValueType, KeyComparator, false>

class BasicContext {
 public:
  // When you insert into / remove from the B+ tree, store the write guard of header page here.
  // Remember to drop the header page guard and set it to nullopt when you want to unlock all.
  std::optional<WritePageGuard> header_page_{std::nullopt};

  // Save the root page id here so that it's easier to know if the current page is the root page.
  page_id_t root_page_id_{INVALID_PAGE_ID};

  // Store the write guards of the pages that you're modifying here.
  std::vector<BasicPageGuard> basic_set_;

  auto IsRootPage(page_id_t page_id) -> bool { return page_id == root_page_id_; }
};

// Main class providing the API for the Interactive B+ Tree.

INDEX_TEMPLATE_ARGUMENTS
class BPlusTree<KeyType, ValueType, KeyComparator, false> {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  explicit BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                     const KeyComparator &comparator, int leaf_max_size = LEAF_PAGE_SIZE,
                     int internal_max_size = INTERNAL_PAGE_SIZE);

  // Returns true if this B+ tree has no keys and values.
  auto IsEmpty() const -> bool;

  // Insert a key-value pair into this B+ tree.
  auto Insert(const KeyType &key, const ValueType &value, Transaction *txn = nullptr) -> bool;

  // Remove a key and its value from this B+ tree.
  auto Remove(const KeyType &key, Transaction *txn = nullptr) -> bool;

  // Return the value associated with a given key
  auto GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn = nullptr) -> bool;

  auto GetValue(const KeyType &key, std::vector<ValueType> *result, const KeyComparator &comparator,
                Transaction *txn = nullptr) -> bool;

  // Return the page id of the root node
  auto GetRootPageId() -> page_id_t;

  // Index iterator
  auto Begin() -> INDEXITERATOR_TYPE;

  auto End() -> INDEXITERATOR_TYPE;

  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;

  // Print the B+ tree
  void Print(BufferPoolManager *bpm);

  // Draw the B+ tree
  void Draw(BufferPoolManager *bpm, const std::string &outf);

  /**
   * @brief draw a B+ tree, below is a printed
   * B+ tree(3 max leaf, 4 max internal) after inserting key:
   *  {1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 18, 19, 20}
   *
   *                               (25)
   *                 (9,17,19)                          (33)
   *  (1,5)    (9,13)    (17,18)    (19,20,21)    (25,29)    (33,37)
   *
   * @return std::string
   */
  auto DrawBPlusTree() -> std::string;

 protected:
  void SetNewRoot(page_id_t new_root_id);
  void SetNewRoot(page_id_t new_root_id, BPlusTreeHeaderPage *header_page);
  auto CreateNewRoot(IndexPageType page_type) -> page_id_t;
  auto CreateNewRoot(IndexPageType page_type, BPlusTreeHeaderPage *header_page) -> page_id_t;
  /**
   * @brief Create a New BPlusTreePage object with return value as its page_id. No latch permission is required.
   * @param page_type INTERNAL_PAGE or LEAF_PAGE
   * @return page_id_t
   */
  auto CreateNewPage(IndexPageType page_type) -> page_id_t;

  /**
   * @brief Get the guard of root page. Latch permission of the header page is needed.
   *
   * @param create_new_root whether to create a new root when there's no root
   * @return BasicPageGuard
   */
  auto GetRootGuard(bool create_new_root = false) -> BasicPageGuard;

  /**
   * @brief Insert the <key, value> pair into current page
   *
   * @param cur_guard
   * @param key
   * @param value
   * @param ctx
   * @return true
   * @return false
   */
  auto InsertIntoPage(const KeyType &key, const ValueType &value, BasicContext *ctx) -> bool;
  auto InsertIntoLeafPage(const KeyType &key, const ValueType &value, BasicContext *ctx) -> bool;
  auto ShiftLeafPage(LeafPage *cur_page, InternalPage *last_page, int index) -> bool;
  auto ShiftInternalPage(InternalPage *cur_page, InternalPage *last_page, int index) -> bool;
  auto SplitLeafPage(LeafPage *cur_page, InternalPage *last_page) -> bool;
  auto SplitInternalPage(InternalPage *cur_page, InternalPage *last_page) -> bool;

  auto GetValueInPage(const KeyType &key, std::vector<ValueType> *result, BasicContext *ctx, const KeyComparator &comparator)
      -> bool;
  auto GetValueInLeafPage(const KeyType &key, std::vector<ValueType> *result, BasicContext *ctx,
                          const KeyComparator &comparator) -> bool;

  auto RemoveInPage(const KeyType &key, BasicContext *ctx) -> std::pair<bool, KeyType>;
  auto RemoveInLeafPage(const KeyType &key, BasicContext *ctx) -> std::pair<bool, KeyType>;
  auto ReplenishLeafPage(LeafPage *cur_page, InternalPage *last_page, int index) -> bool;
  auto ReplenishInternalPage(InternalPage *cur_page, InternalPage *last_page, int index) -> bool;
  auto CoalesceLeafPage(LeafPage *cur_page, InternalPage *last_page, int index) -> bool;
  auto CoalesceInternalPage(InternalPage *cur_page, InternalPage *last_page, int index) -> bool;

 private:
  /* Debug Routines for FREE!! */
  void ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out);

  void PrintTree(page_id_t page_id, const BPlusTreePage *page);

  /**
   * @brief Convert A B+ tree into a Printable B+ tree
   *
   * @param root_id
   * @return PrintableNode
   */
  auto ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree;

  // member variable
  std::string index_name_;
  BufferPoolManager *bpm_;
  KeyComparator comparator_;
  std::vector<std::string> log;  // NOLINT
  int leaf_max_size_;
  int internal_max_size_;
  page_id_t header_page_id_;
};

}  // namespace bustub
