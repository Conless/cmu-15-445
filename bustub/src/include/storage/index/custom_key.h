#pragma once

#include <cstdint>
#include <istream>
#include <ostream>
#include <string>
#include "common/macros.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

class Key {
 public:
  Key() = default;
  Key(const Key &other) = default;
  //   inline virtual void SetFromKey(const Tuple &tuple) { UNIMPLEMENTED("Key cannot be set from tuple."); }
  inline virtual void SetFromInteger(int64_t key) { UNIMPLEMENTED("Key cannot be set from integer."); }
  //   inline virtual auto ToValue(Schema *schema, uint32_t column_idx) -> Value { UNIMPLEMENTED("Key cannot be
  //   converted to value."); }
  inline virtual auto ToString() const -> std::string { UNIMPLEMENTED("Key cannot be converted to string."); }
};

class Comparator {
 public:
  Comparator() = default;
  //   explicit Comparator(Schema *key_schema)  { UNIMPLEMENTED("Standard comparator cannot be constructed from
  //   schema."); }
};

template <typename KeyType>
class StandardKey : public Key {
 public:
  StandardKey() = default;
  StandardKey(const KeyType &data) : data_(data) {}  // NOLINT
  StandardKey(const StandardKey &other) = default;
  auto operator=(const StandardKey &other) -> StandardKey & = default;
  inline void SetFromInteger(int64_t key) override { data_ = static_cast<KeyType>(key); }
  inline auto ToString() const -> std::string override { return std::to_string(data_); }
  friend auto operator<<(std::ostream &os, const StandardKey &key) -> std::ostream & {
    os << key.data_;
    return os;
  }
  KeyType data_;
};

template <typename KeyType>
class StandardComparator : public Comparator {
 public:
  StandardComparator() = delete;
  StandardComparator(const StandardComparator &) = default;
  StandardComparator(StandardComparator &&) noexcept = default;
  inline auto operator()(const StandardKey<KeyType> &lhs, const StandardKey<KeyType> &rhs) const -> int {
    if (lhs.data_ < rhs.data_) {
      return -1;
    }
    if (rhs.data_ < lhs.data_) {
      return 1;
    }
    return 0;
  }
};

/**
 * @brief Class String
 * @details Package the char array at a size of kMaxKeyLen, enable assignment
 * and comparison.
 */
template <size_t Length>
class StringKey : public Key {
 public:
  StringKey() { str_[0] = '\0'; }
  explicit StringKey(const char *str) {
    for (int i = 0; i < Length; i++) {
      str_[i] = str[i];
      if (str[i] == '\0') {
        return;
      }
    }
  }
  explicit StringKey(const std::string &str) : StringKey(str.c_str()) { }
  StringKey(const StringKey &x) : StringKey(x.str_) {}
  inline auto ToString() const -> std::string override { return std::string(str_); }
  auto Empty() const -> bool { return str_[0] == '\0'; }
  auto operator<(const StringKey<Length> &x) const -> bool {
    for (int i = 0; i < Length; i++) {
      if (str_[i] != x.str_[i] || str_[i] == '\0') {
        return str_[i] < x.str_[i];
      }
    }
    return false;
  }
  auto operator==(const StringKey &x) const -> bool {
    for (int i = 0; i < Length; i++) {
      if (str_[i] != x.str_[i]) {
        return false;
      }
      if (str_[i] == '\0') {
        return true;
      }
    }
    return true;
  }
  explicit operator std::string() const { return std::string(str_); }
  friend auto operator<<(std::ostream &os, const StringKey &rhs) -> std::ostream & { return (os << rhs.str_); }
  friend auto operator>>(std::istream &is, const StringKey &rhs) -> std::istream & { return (is >> rhs.str_); }

 public:
  char str_[Length];
};

enum ComparatorType { CompareData, CompareKey };

/**
 * @brief Class DataType
 * @details Package the pair of key and value, enable assignment and comparison.
 */
template <size_t Length>
class StringIntKey : public Key {
 public:
  StringKey<Length> key_{};
  int value_{};
  StringIntKey() = default;
  StringIntKey(const StringKey<Length> &key, int value) : key_(key), value_(value) {}
  StringIntKey(const char *key, int value) : key_(key), value_(value) {}
  StringIntKey(const std::string &key, int value) : key_(key), value_(value) {}
  inline auto ToString() const -> std::string override {
    return ("{" + std::string(key_) + "," + std::to_string(value_) + "}");
  }
  auto operator<(const StringIntKey &x) const -> bool { return key_ == x.key_ ? value_ < x.value_ : key_ < x.key_; }
  auto operator>(const StringIntKey &x) const -> bool { return key_ == x.key_ ? value_ > x.value_ : key_ > x.key_; }
  auto operator==(const StringIntKey &x) const -> bool { return key_ == x.key_ && value_ == x.value_; }
  friend auto operator<<(std::ostream &os, const StringIntKey &rhs) -> std::ostream & {
    return (os << '{' << rhs.key_ << ',' << rhs.value_ << '}');
  }
  friend auto operator>>(std::istream &is, const StringIntKey &rhs) -> std::istream & {
    return (is >> rhs.key_ >> rhs.value_);
  }
};

template <size_t Length>
class StringIntComparator : public Comparator {
 public:
  StringIntComparator() = delete;
  explicit StringIntComparator(ComparatorType type) : type_(type) {}
  StringIntComparator(const StringIntComparator &) = default;
  StringIntComparator(StringIntComparator &&other) noexcept { type_ = other.type_; }
  inline auto operator()(const StringIntKey<Length> &lhs, const StringIntKey<Length> &rhs) const -> int {
    if (type_ == CompareKey) {
      if (lhs.key_ < rhs.key_) {
        return -1;
      }
      if (rhs.key_ < lhs.key_) {
        return 1;
      }
      return 0;
    }
    if (lhs < rhs) {
      return -1;
    }
    if (rhs < lhs) {
      return 1;
    }
    return 0;
  }

 private:
  ComparatorType type_;
};

}  // namespace bustub