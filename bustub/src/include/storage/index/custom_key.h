#pragma once

#include <cstdint>
#include "common/macros.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

template <typename KeyType>
class StandardComparator;

template <typename KeyType>
class StandardKey {
  friend class StandardComparator<KeyType>;
 public:
  StandardKey() = default;
StandardKey(const KeyType &data) : data_(data) {} // NOLINT
//   inline void SetFromKey(const Tuple &tuple) { UNIMPLEMENTED("Standard key cannot be set from tuple."); }
  inline void SetFromInteger(int64_t key) { data_ = static_cast<KeyType>(key); }
//   inline auto ToValue(Schema *schema, uint32_t column_idx) { UNIMPLEMENTED("Standard key cannot be converted to value."); }
  inline auto ToString() const -> int64_t { return static_cast<int64_t>(data_); }
  friend auto operator<<(std::ostream &os, const StandardKey &key) -> std::ostream & {
    os << key.data_;
    return os;
  }
  
 private:
  KeyType data_;
};

template <typename KeyType>
class StandardComparator {
 public:
  StandardComparator() = default;
  inline auto operator()(const StandardKey<KeyType> &lhs, const StandardKey<KeyType> &rhs) const -> int {
    if (lhs.data_ < rhs.data_) {
      return 1;
    }
    if (rhs.data_ < lhs.data_) {
      return -1;
    }
    return 0;
  }
//   explicit StandardComparator(Schema *key_schema)  { UNIMPLEMENTED("Standard comparator cannot be constructed from schema."); }
};

}  // namespace bustub