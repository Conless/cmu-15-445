//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to get the index associated with key
 * The result is the last key that is less or equal than the given key, from -1 to size() - 1
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetLastIndexLE(const KeyType &key, const KeyComparator &comparator) const -> int {
  int l = 0;
  int r = GetSize() - 1;
  int res = -1;
  while (l <= r) {
    int mid = (l + r) >> 1;
    if (comparator(KeyAt(mid), key) <= 0) {
      l = mid + 1;
      res = mid;
    } else {
      r = mid - 1;
    }
  }
  return res;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetLastIndexL(const KeyType &key, const KeyComparator &comparator) const -> int {
  int l = 0;
  int r = GetSize() - 1;
  int res = -1;
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

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetIndexE(const KeyType &key, const KeyComparator &comparator) const -> int {
  int l = 0;
  int r = GetSize() - 1;
  while (l <= r) {
    int mid = (l + r) >> 1;
    int comp_res = comparator(KeyAt(mid), key);
    if (comp_res == 0) {
      return mid;
    }
    if (comp_res < 0) {
      l = mid + 1;
    } else {
      r = mid - 1;
    }
  }
  return -1;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> const KeyType & {
  if (index < 0 || index >= GetSize()) {
    throw Exception(ExceptionType::OUT_OF_RANGE, "index out of range in leaf");
  }
  return (array_ + index)->first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  if (index < 0 || index >= GetSize()) {
    throw Exception(ExceptionType::OUT_OF_RANGE, "index out of range in leaf");
  }
  (array_ + index)->first = key;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> const ValueType & {
  if (index < 0 || index >= GetSize()) {
    throw Exception(ExceptionType::OUT_OF_RANGE, "index out of range in leaf");
  }
  return (array_ + index)->second;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  if (index < 0 || index >= GetSize()) {
    throw Exception(ExceptionType::OUT_OF_RANGE, "index out of range in leaf");
  }
  (array_ + index)->second = value;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::DataAt(int index) const -> const MappingType & {
  if (index < 0 || index >= GetSize()) {
    std::cout << "size: " << GetSize() << '\n';
    throw Exception(ExceptionType::OUT_OF_RANGE, "index out of range in leaf");
  }
  return *(array_ + index);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetDataAt(int index, const KeyType &key, const ValueType &value) {
  if (index < 0 || index >= GetSize()) {
    throw Exception(ExceptionType::OUT_OF_RANGE, "index out of range in leaf");
  }
  (array_ + index)->first = key;
  (array_ + index)->second = value;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyBackward(int index) {
  for (int i = GetSize(); i > index; i--) {
    *(array_ + i) = *(array_ + i - 1);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyForward(int index) {
  for (int i = index + 1; i < GetSize(); i++) {
    *(array_ + i - 1) = *(array_ + i);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopySecondHalfTo(BPlusTreeLeafPage *other) {
  int size = GetSize();
  int start = size / 2;
  int end = size;
  this->SetSize(size / 2);
  other->SetSize(size - size / 2);
  std::copy(array_ + start, array_ + end, other->array_);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstNTo(int n, BPlusTreeLeafPage *other) {
  BUSTUB_ASSERT(n <= GetSize(), "Error");
  for (int i = other->GetSize(), j = 0; j < n; i++, j++) {
    *(other->array_ + i) = *(array_ + j);
  }
  other->IncreaseSize(n);
  this->IncreaseSize(-n);
  int cur_size = GetSize();
  for (int i = 0; i < cur_size; i++) {
    *(array_ + i) = *(array_ + i + n);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastNTo(int n, BPlusTreeLeafPage *other) {
  BUSTUB_ASSERT(n <= GetSize(), "Error");
  other->IncreaseSize(n);
  for (int i = other->GetSize() - 1; i >= n; i--) {
    *(other->array_ + i) = *(other->array_ + i - n);
  }
  this->IncreaseSize(-n);
  for (int i = 0, j = GetSize(); i < n; i++, j++) {
    *(other->array_ + i) = *(array_ + j);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::InsertData(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> int {
  int index = GetLastIndexLE(key, comparator);  // recall index corresponds to the last less or equal key
  if (index != -1 && comparator(KeyAt(index), key) == 0) {
    return -1;
  }
  CopyBackward(index + 1);
  IncreaseSize(1);
  SetKeyAt(index + 1, key);
  SetValueAt(index + 1, value);
  return index + 1;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveData(const KeyType &key, const KeyComparator &comparator) -> int {
  int index = GetIndexE(key, comparator);
  if (index == -1) {
    return -1;
  }
  RemoveData(index);
  return index;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveData(int index) -> MappingType {
  MappingType data = *(array_ + index);
  CopyForward(index);
  IncreaseSize(-1);
  return data;
}

}  // namespace bustub

#ifdef CUSTOMIZED_BUSTUB
#include "storage/index/custom_key.h"
BUSTUB_DECLARE(BPlusTreeLeafPage)
#else
#ifndef BUSTUB_DECLARE
#define BUSTUB_DECLARE(TypeName)                                                    \
  namespace bustub {                                                                \
  template class TypeName<GenericKey<4>, RID, GenericComparator<4>>;   /* NOLINT */ \
  template class TypeName<GenericKey<8>, RID, GenericComparator<8>>;   /* NOLINT */ \
  template class TypeName<GenericKey<16>, RID, GenericComparator<16>>; /* NOLINT */ \
  template class TypeName<GenericKey<32>, RID, GenericComparator<32>>; /* NOLINT */ \
  template class TypeName<GenericKey<64>, RID, GenericComparator<64>>; /* NOLINT */ \
  }
#endif
BUSTUB_DECLARE(BPlusTreeLeafPage)
#endif
