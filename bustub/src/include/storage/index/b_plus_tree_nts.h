#pragma once

#include "storage/index/b_plus_tree.h"

#ifdef REPLACE_STL
#include "container/stl/vector.h"
using sjtu::vector;  // NOLINT
#else
using std::vector;  // NOLINT
#endif

namespace bustub {

/**
 * @brief Definition of the BasicContext class.
 *
 * Hint: This class is designed to help you keep track of the pages
 * that you're modifying or accessing.
 */
class BasicContext {
 public:
  // When you insert into / remove from the B+ tree, store the write guard of header page here.
  // Remember to drop the header page guard and set it to nullopt when you want to unlock all.
  std::optional<WritePageGuard> header_page_{std::nullopt};

  // Save the root page id here so that it's easier to know if the current page is the root page.
  page_id_t root_page_id_{INVALID_PAGE_ID};

  // Store the write guards of the pages that you're modifying here.
  vector<BasicPageGuard> basic_set_;

  auto IsRootPage(page_id_t page_id) -> bool { return page_id == root_page_id_; }
};

#define BPLUSTREE_NTS_TYPE BPlusTree<KeyType, ValueType, KeyComparator, false>

// The B+ tree (non-thread-safe version)
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree<KeyType, ValueType, KeyComparator, false> {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  explicit BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                     const KeyComparator &comparator, int leaf_max_size = LEAF_PAGE_SIZE,
                     int internal_max_size = INTERNAL_PAGE_SIZE, bool inherit_file = true);
  ~BPlusTree();

  // Returns true if this B+ tree has no keys and values.
  auto IsEmpty() const -> bool;

  // Insert a key-value pair into this B+ tree.
  auto Insert(const KeyType &key, const ValueType &value, Transaction *txn = nullptr) -> bool;

  // Remove a key and its value from this B+ tree.
  auto Remove(const KeyType &key, Transaction *txn = nullptr) -> bool;

  // Return the value associated with a given key.
  auto GetValue(const KeyType &key, vector<ValueType> *result, Transaction *txn = nullptr) -> bool;

  // Return all the values associated with the given key and comparator.
  auto GetValue(const KeyType &key, vector<ValueType> *result, const KeyComparator &comparator,
                Transaction *txn = nullptr) -> bool;

  // Return the page id of the root node.
  auto GetRootPageId() -> page_id_t;

  // Begin iterator, containing the first {key, data} pair in B+ tree.
  auto Begin() -> INDEXITERATOR_TYPE;

  // End iterator, containing no data.
  auto End() -> INDEXITERATOR_TYPE;

  // The iterator containing the first (less or equal) data associated with the given key.
  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;

 protected:
  /** Create, reset and get root operations */
  // Create a new page with the given page type.
  auto CreateNewPage(IndexPageType page_type) -> page_id_t;
  // Create a new root with the given page type.
  auto CreateNewRoot(IndexPageType page_type) -> page_id_t;
  // Set root id to new root id.
  void SetNewRoot(page_id_t new_root_id);
  // Get the root guard
  auto GetRootGuard(bool create_new_root = false) -> BasicPageGuard;

  /** Insert operation and utils functions  */
  // Insert data into current internal page, stored in back of ctx->basic_set.
  auto InsertIntoPage(const KeyType &key, const ValueType &value, BasicContext *ctx, int index) -> std::pair<bool, bool>;
  // Insert data into current leaf page, stored in back of ctx->basic_set.
  auto InsertIntoLeafPage(const KeyType &key, const ValueType &value, BasicContext *ctx, int index) -> std::pair<bool, bool>;
  // Shift data between leaf page, i.e. send some data away from current page.
  auto ShiftLeafPage(LeafPage *cur_page, InternalPage *last_page, int index) -> bool;
  // Shift data between internal page, i.e. send some data away from current page.
  auto ShiftInternalPage(InternalPage *cur_page, InternalPage *last_page, int index) -> bool;
  // Split current leaf page into two pages.
  auto SplitLeafPage(LeafPage *cur_page, InternalPage *last_page) -> bool;
  // Split current internal page into two pages.
  auto SplitInternalPage(InternalPage *cur_page, InternalPage *last_page) -> bool;

  /** GetValue operation and utils functions  */
  // Get all the values associated with the given key in current internal page, stored in back of ctx->basic_set.
  auto GetValueInPage(const KeyType &key, vector<ValueType> *result, BasicContext *ctx, const KeyComparator &comparator)
      -> bool;
  // Get all the values associated with the given key in current internal page, stored in back of ctx->basic_set.
  auto GetValueInLeafPage(const KeyType &key, vector<ValueType> *result, BasicContext *ctx,
                          const KeyComparator &comparator) -> bool;

  /** Remove operation and utils functions  */
  // Remove the data assoicated with the given key in current internal page, stored in back of ctx->basic_set.
  auto RemoveInPage(const KeyType &key, BasicContext *ctx, int index) -> std::pair<bool, KeyType>;
  // Remove the data assoicated with the given key in current leaf page, stored in back of ctx->basic_set.
  auto RemoveInLeafPage(const KeyType &key, BasicContext *ctx, int index) -> std::pair<bool, KeyType>;
  // Replenish current leaf page, i.e. borrow some data to current page.
  auto ReplenishLeafPage(LeafPage *cur_page, InternalPage *last_page, int index) -> bool;
  // Replenish current internal page, i.e. borrow some data to current page.
  auto ReplenishInternalPage(InternalPage *cur_page, InternalPage *last_page, int index) -> bool;
  // Coalesce current leaf page with an adjacent page.
  auto CoalesceLeafPage(LeafPage *cur_page, InternalPage *last_page, int index) -> bool;
  // Coalesce current internal page with an adjacent page.
  auto CoalesceInternalPage(InternalPage *cur_page, InternalPage *last_page, int index) -> bool;

 public:
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
  const std::string index_name_;
  const bool inherit_file_;
  BufferPoolManager *bpm_;
  KeyComparator comparator_;
  vector<std::string> log;  // NOLINT
  int leaf_max_size_;
  int internal_max_size_;
  page_id_t header_page_id_;
  page_id_t root_page_id_;
};

}  // namespace bustub
