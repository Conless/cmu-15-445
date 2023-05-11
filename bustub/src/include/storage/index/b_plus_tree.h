/**
 * b_plus_tree.h
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
#pragma once

#include <algorithm>
#include <deque>
#include <iostream>
#include <optional>
#include <queue>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

#ifdef REPLACE_STL
#include "container/stl/vector.h"
using sjtu::vector;  // NOLINT
#else
using std::vector;  // NOLINT
#endif

namespace bustub {

struct PrintableBPlusTree;

/**
 * @brief Definition of the Context class.
 *
 * Hint: This class is designed to help you keep track of the pages
 * that you're modifying or accessing.
 */
class Context {
 public:
  // When you insert into / remove from the B+ tree, store the write guard of header page here.
  // Remember to drop the header page guard and set it to nullopt when you want to unlock all.
  std::optional<WritePageGuard> header_page_{std::nullopt};

  // Save the root page id here so that it's easier to know if the current page is the root page.
  page_id_t root_page_id_{INVALID_PAGE_ID};

  // Store the write guards of the pages that you're modifying here.
  std::deque<WritePageGuard> write_set_;

  // You may want to use this when getting value, but not necessary.
  std::deque<ReadPageGuard> read_set_;

  auto IsRootPage(page_id_t page_id) -> bool { return page_id == root_page_id_; }
};

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator, true>

// Main class providing the API for the Interactive B+ Tree.

template <typename KeyType, typename ValueType, typename KeyComparator, bool isThreadSafe = true>
class BPlusTree {};

// The B+ tree (thread-safe version)
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree<KeyType, ValueType, KeyComparator, true> {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  explicit BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                     const KeyComparator &comparator, int leaf_max_size = LEAF_PAGE_SIZE,
                     int internal_max_size = INTERNAL_PAGE_SIZE, bool inherit_file = false);

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
  // Create a new root with the given page type by header page.
  auto CreateNewRoot(IndexPageType page_type, BPlusTreeHeaderPage *header_page) -> page_id_t;
  // Set root id to new root id by the given header page.
  void SetNewRoot(page_id_t new_root_id, BPlusTreeHeaderPage *header_page);
  // Get the root write guard, with the read guard of HeaderPage required.
  auto GetRootGuardWrite(Context *ctx, bool create_new_root = false) -> WritePageGuard;
  // Fetch the root write guard from the given context.
  auto FetchRootGuardWrite(Context *ctx) -> WritePageGuard;
  // Get the root read guard, with the read guard of HeaderPage required.
  auto GetRootGuardRead() const -> ReadPageGuard;

  /** Insert operation and utils functions  */
  // Insert data optimistically into leaf, assuming that only leaf page will be edited
  auto InsertOptimistic(const KeyType &key, const ValueType &value) -> std::pair<bool, bool>;
  // Insert data into current internal page, stored in back of ctx->write_set.
  auto InsertIntoPage(const KeyType &key, const ValueType &value, Context *ctx, int index) -> std::pair<bool, bool>;
  // Insert data into current leaf page, stored in back of ctx->write_set.
  auto InsertIntoLeafPage(const KeyType &key, const ValueType &value, Context *ctx, int index) -> std::pair<bool, bool>;
  // Shift data between leaf page, i.e. send some data away from current page.
  auto ShiftLeafPage(LeafPage *cur_page, InternalPage *last_page, int index) -> bool;
  // Shift data between internal page, i.e. send some data away from current page.
  auto ShiftInternalPage(InternalPage *cur_page, InternalPage *last_page, int index) -> bool;
  // Split current leaf page into two pages.
  auto SplitLeafPage(LeafPage *cur_page, InternalPage *last_page) -> bool;
  // Split current internal page into two pages.
  auto SplitInternalPage(InternalPage *cur_page, InternalPage *last_page) -> bool;

  /** GetValue operation and utils functions  */
  // Get all the values associated with the given key in current internal page, stored in back of ctx->read_set.
  auto GetValueInPage(const KeyType &key, vector<ValueType> *result, Context *ctx, const KeyComparator &comparator)
      -> bool;
  // Get all the values associated with the given key in current internal page, stored in back of ctx->read_set.
  auto GetValueInLeafPage(const KeyType &key, vector<ValueType> *result, Context *ctx, const KeyComparator &comparator)
      -> bool;

  /** Remove operation and utils functions  */
  // Remove the data assoicated with the given key in current internal page, stored in back of ctx->write_set.
  auto RemoveInPage(const KeyType &key, Context *ctx, int index) -> std::pair<bool, KeyType>;
  // Remove the data assoicated with the given key in current leaf page, stored in back of ctx->write_set.
  auto RemoveInLeafPage(const KeyType &key, Context *ctx, int index) -> std::pair<bool, KeyType>;
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

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name, Transaction *txn = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name, Transaction *txn = nullptr);

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
};

/**
 * @brief for test only. PrintableBPlusTree is a printalbe B+ tree.
 * We first convert B+ tree into a printable B+ tree and the print it.
 */
struct PrintableBPlusTree {
  int size_;
  std::string keys_;
  std::vector<PrintableBPlusTree> children_;

  /**
   * @brief BFS traverse a printable B+ tree and print it into
   * into out_buf
   *
   * @param out_buf
   */
  void Print(std::ostream &out_buf) {
    std::vector<PrintableBPlusTree *> que = {this};
    while (!que.empty()) {
      std::vector<PrintableBPlusTree *> new_que;

      for (auto &t : que) {
        int padding = (t->size_ - t->keys_.size()) / 2;
        out_buf << std::string(padding, ' ');
        out_buf << t->keys_;
        out_buf << std::string(padding, ' ');

        for (auto &c : t->children_) {
          new_que.push_back(&c);
        }
      }
      out_buf << "\n";
      que = new_que;
    }
  }
};

}  // namespace bustub
