/**
 * index_iterator.cpp
 */
#include <cassert>

#include "common/config.h"
#include "common/exception.h"
#include "storage/index/index_iterator.h"
#include "storage/page/page_guard.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t page_id, int index_in_page, BufferPoolManager *bpm)
    : page_id_(page_id), index_in_page_(index_in_page), bpm_(bpm) {
  if (page_id_ != INVALID_PAGE_ID) {
    page_guard_ = bpm_->FetchPageBasic(page_id_);
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() const -> bool { return page_id_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  if (IsEnd()) {
    throw Exception(ExceptionType::OUT_OF_RANGE, "invalid iterator");
  }
  auto cur_page = page_guard_.As<LeafPage>();
  return cur_page->DataAt(index_in_page_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (IsEnd()) {
    return *this;
  }
  auto cur_page = page_guard_.As<LeafPage>();
  index_in_page_++;
  if (index_in_page_ == cur_page->GetSize()) {
    if (cur_page->GetNextPageId() != INVALID_PAGE_ID) {
      page_guard_ = bpm_->FetchPageBasic(cur_page->GetNextPageId());
      cur_page = page_guard_.As<LeafPage>();
      index_in_page_ = 0;
    } else {
      page_id_ = INVALID_PAGE_ID;
    }
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
