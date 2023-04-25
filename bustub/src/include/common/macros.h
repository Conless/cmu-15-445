//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// macros.h
//
// Identification: src/include/common/macros.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cassert>
#include <stdexcept>

namespace bustub {

#define BUSTUB_ASSERT(expr, message) assert((expr) && (message))

#define UNIMPLEMENTED(message) throw std::logic_error(message)

#define BUSTUB_ENSURE(expr, message) \
  if (!(expr)) {                     \
    throw std::logic_error(message); \
  }

#define UNREACHABLE(message) throw std::logic_error(message)

// Macros to disable copying and moving
#define DISALLOW_COPY(cname)                                    \
  cname(const cname &) = delete;                   /* NOLINT */ \
  auto operator=(const cname &)->cname & = delete; /* NOLINT */

#define DISALLOW_MOVE(cname)                               \
  cname(cname &&) = delete;                   /* NOLINT */ \
  auto operator=(cname &&)->cname & = delete; /* NOLINT */

#define DISALLOW_COPY_AND_MOVE(cname) \
  DISALLOW_COPY(cname);               \
  DISALLOW_MOVE(cname);

#define BUSTUB_DECLARE(TypeName)                                                        \
  namespace bustub {                                                                    \
  template class TypeName<GenericKey<4>, RID, GenericComparator<4>>;       /* NOLINT */ \
  template class TypeName<GenericKey<8>, RID, GenericComparator<8>>;       /* NOLINT */ \
  template class TypeName<GenericKey<16>, RID, GenericComparator<16>>;     /* NOLINT */ \
  template class TypeName<GenericKey<32>, RID, GenericComparator<32>>;     /* NOLINT */ \
  template class TypeName<GenericKey<64>, RID, GenericComparator<64>>;     /* NOLINT */ \
  template class TypeName<StandardKey<int>, int, StandardComparator<int>>; /* NOLINT */ \
  }

#define BUSTUB_NTS_DECLARE(TypeName)                                                       \
  namespace bustub {                                                                       \
  template class TypeName<GenericKey<4>, RID, GenericComparator<4>, false>;   /* NOLINT */ \
  template class TypeName<GenericKey<8>, RID, GenericComparator<8>, false>;   /* NOLINT */ \
  template class TypeName<GenericKey<16>, RID, GenericComparator<16>, false>; /* NOLINT */ \
  template class TypeName<GenericKey<32>, RID, GenericComparator<32>, false>; /* NOLINT */ \
  template class TypeName<GenericKey<64>, RID, GenericComparator<64>, false>; /* NOLINT */ \
  template class TypeName<StandardKey<int>, int, StandardComparator<int>, false>;    /* NOLINT */ \
  }

#define BUSTUB_INTERNAL_DECLARE(TypeName)                                                     \
  namespace bustub {                                                                          \
  template class TypeName<GenericKey<4>, page_id_t, GenericComparator<4>>;       /* NOLINT */ \
  template class TypeName<GenericKey<8>, page_id_t, GenericComparator<8>>;       /* NOLINT */ \
  template class TypeName<GenericKey<16>, page_id_t, GenericComparator<16>>;     /* NOLINT */ \
  template class TypeName<GenericKey<32>, page_id_t, GenericComparator<32>>;     /* NOLINT */ \
  template class TypeName<GenericKey<64>, page_id_t, GenericComparator<64>>;     /* NOLINT */ \
  template class TypeName<StandardKey<int>, page_id_t, StandardComparator<int>>; /* NOLINT */ \
  }

}  // namespace bustub
