//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_leaf_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <string>
#include <utility>
#include <vector>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_LEAF_PAGE_TYPE BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>
#define LEAF_PAGE_HEADER_SIZE 16
#define LEAF_PAGE_SIZE ((BUSTUB_PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / sizeof(MappingType) - 1)

/**
 * Store indexed key and record id(record id = page id combined with slot id,
 * see include/common/rid.h for detailed implementation) together within leaf
 * page. Only support unique key.
 *
 * Leaf page format (keys are stored in order):
 *  ----------------------------------------------------------------------
 * | HEADER | KEY(1) + RID(1) | KEY(2) + RID(2) | ... | KEY(n) + RID(n)
 *  ----------------------------------------------------------------------
 *
 *  Header format (size in byte, 16 bytes in total):
 *  ---------------------------------------------------------------------
 * | PageType (4) | CurrentSize (4) | MaxSize (4) |
 *  ---------------------------------------------------------------------
 *  -----------------------------------------------
 * |  NextPageId (4)
 *  -----------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeLeafPage : public BPlusTreePage {
 public:
  // Delete all constructor / destructor to ensure memory safety
  BPlusTreeLeafPage() = delete;
  BPlusTreeLeafPage(const BPlusTreeLeafPage &other) = delete;

  /**
   * After creating a new leaf page from buffer pool, must call initialize
   * method to set default values
   * @param max_size Max size of the leaf node
   */
  void Init(int max_size = LEAF_PAGE_SIZE);

  // helper methods
  auto GetNextPageId() const -> page_id_t;
  void SetNextPageId(page_id_t next_page_id);

  /**
   *
   * @param key the key to search for
   */
  auto GetLastIndexLE(const KeyType &key, const KeyComparator &comparator) const -> int;

  auto GetLastIndexL(const KeyType &key, const KeyComparator &comparator) const -> int;

  auto GetIndexE(const KeyType &key, const KeyComparator &comparator) const -> int;

  /**
   * @param index The index of the key to get. Index must be non-zero.
   * @return Key at index
   */
  auto KeyAt(int index) const -> const KeyType &;

  /**
   *
   * @param index The index of the key to set. Index must be non-zero.
   * @param key The new value for key
   */
  void SetKeyAt(int index, const KeyType &key);

  /**
   *
   * @param index the index
   * @return the value at the index
   */
  auto ValueAt(int index) const -> const ValueType &;

  /**
   *
   * @param index The index of the key to set. Index must be non-zero.
   * @param value The new value
   */
  void SetValueAt(int index, const ValueType &value);

  auto DataAt(int index) const -> const MappingType &;

  /**
   * @brief Set the Data At object
   *
   * @param index
   * @param key
   * @param value
   */
  void SetDataAt(int index, const KeyType &key, const ValueType &value);

  auto InsertData(const KeyType &key, const ValueType &value, const KeyComparator &comparator) -> int;

  auto RemoveData(const KeyType &key, const KeyComparator &comparator) -> int;

  auto RemoveData(int index) -> MappingType;

  void CopyBackward(int index);

  void CopyForward(int index);

  void CopySecondHalfTo(BPlusTreeLeafPage *other);

  void CopyFirstNTo(int n, BPlusTreeLeafPage *other);

  void CopyLastNTo(int n, BPlusTreeLeafPage *other);

  /**
   * @brief for test only return a string representing all keys in
   * this leaf page formatted as "(key1,key2,key3,...)"
   *
   * @return std::string
   */
  auto ToString() const -> std::string {
    std::string kstr = "(";
    bool first = true;

    for (int i = 0; i < GetSize(); i++) {
      KeyType key = KeyAt(i);
      if (first) {
        first = false;
      } else {
        kstr.append(",");
      }
#ifndef CUSTOMIZED_BUSTUB
      kstr.append(std::to_string(key.ToString()));
#else
      kstr.append(key.ToString());
#endif
    }
    kstr.append(")");

    return kstr;
  }

 private:
  page_id_t next_page_id_;
  // Flexible array member for page data.
  MappingType array_[0];
};
}  // namespace bustub
