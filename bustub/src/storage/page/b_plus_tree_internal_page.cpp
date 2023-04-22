//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
}

/*
 * Helper method to get the last key that is less than the given key
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetLastIndexL(const KeyType &key, const KeyComparator &comparator) const -> int {
  int l = 1;
  int r = GetSize() - 1;
  int res = 0;
  while (l <= r) {
    int mid = (l + r) >> 1;
    if (comparator(KeyAt(mid), key) < 0) {
      l = mid + 1;
      res = mid;
    } else {
      r = mid - 1;
    }
  }
  return res;
}

/*
 * Helper method to get the index associated with key
 * The result is the first key that is greater or equal than the given key, from 1 to size()
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetFirstIndexGE(const KeyType &key, const KeyComparator &comparator) const -> int {
  int l = 1;
  int r = GetSize() - 1;
  int res = GetSize();
  while (l <= r) {
    int mid = (l + r) >> 1;
    if (comparator(KeyAt(mid), key) >= 0) {
      r = mid - 1;
      res = mid;
    } else {
      l = mid + 1;
    }
  }
  return res;
}

/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  if (index < 1 || index >= GetSize()) {
    return KeyType{};
  }
  return (array_ + index)->first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  if (index < 1 || index >= GetSize()) {
    return;
  }
  (array_ + index)->first = key;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  if (index < 0 || index >= GetSize()) {
    return ValueType{};
  }
  return (array_ + index)->second;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  if (index < 0 || index >= GetSize()) {
    return;
  }
  (array_ + index)->second = value;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetDataAt(int index, const KeyType &key, const ValueType &value) {
  if (index < 1 || index >= GetSize()) {
    return;
  }
  (array_ + index)->first = key;
  (array_ + index)->second = value;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyBackward(int index) {
  for (int i = GetSize(); i > index; i--) {
    *(array_ + i) = *(array_ + i - 1);
  }
}

/**
 * @brief
 *  For example, we have the old internal page like
 *      array_[0] array_[1] ... array_[size() - 1]
 *  So the first size() / 2 elements is from array_[0] to array_[size() / 2 - 1]
 * @param other 
 * @return INDEX_TEMPLATE_ARGUMENTS 
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopySecondHalfTo(BPlusTreeInternalPage *other) {
  int size = GetSize();
  int start = size / 2 + 1;
  int end = size;
  other->IncreaseSize(end - start);
  this->SetSize(size / 2);
  other->SetSize(size - size / 2);
  other->array_[0].second = array_[start - 1].second;
  std::copy(array_ + start, array_ + end, other->array_ + 1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertData(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  int index = GetFirstIndexGE(key, comparator); // recall index corresponds to the first greater or equal key
  CopyBackward(index);
  IncreaseSize(1);
  SetKeyAt(index, key);
  SetValueAt(index, value);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
