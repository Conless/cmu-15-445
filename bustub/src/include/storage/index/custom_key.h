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
  //   inline virtual void SetFromKey(const Tuple &tuple) { UNIMPLEMENTED("Key cannot be set from tuple."); }
  inline virtual void SetFromInteger(int64_t key) { UNIMPLEMENTED("Key cannot be set from integer."); }
  //   inline virtual auto ToValue(Schema *schema, uint32_t column_idx) -> Value { UNIMPLEMENTED("Key cannot be
  //   converted to value."); }
  inline virtual auto ToString() const -> std::string { UNIMPLEMENTED("Key cannot be converted to string."); }
};

template <typename KeyType>
class StandardKey : public Key {
 public:
  StandardKey() = default;
  explicit StandardKey(const KeyType &data) : data_(data) {}

  StandardKey(const StandardKey<KeyType> &other) { data_ = other.data_; };
  auto operator=(const StandardKey<KeyType> &other) -> StandardKey & {
    data_ = other.data_;
    return *this;
  }

 public:
  class Comparator {
   public:
    Comparator() = delete;
    Comparator(const Comparator &) = default;
    Comparator(Comparator &&) noexcept = default;
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

 public:
  inline void SetFromInteger(int64_t key) override { data_ = static_cast<KeyType>(key); }
  inline auto ToString() const -> std::string override { return std::to_string(data_); }
  friend auto operator<<(std::ostream &os, const StandardKey &key) -> std::ostream & {
    os << key.data_;
    return os;
  }
  KeyType data_;
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
  StringKey(const StringKey &other) : StringKey(other.str_) {}
  auto operator=(const StringKey &other) -> StringKey & {
    for (size_t i = 0; i < Length; i++) {
      str_[i] = other.str_[i];
      if (other.str_[i] == '\0') {
        break;
      }
    }
    return *this;
  }

 public:
  StringKey(const char *str) { // NOLINT
    for (size_t i = 0; i < Length; i++) {
      str_[i] = str[i];
      if (str[i] == '\0') {
        return;
      }
    }
  }
  explicit StringKey(const std::string &str) : StringKey(str.c_str()) {}

 public:
  inline auto ToString() const -> std::string override { return std::string(str_); }
  auto Empty() const -> bool { return str_[0] == '\0'; }
  friend auto operator<(const StringKey<Length> &lhs, const StringKey<Length> &rhs) -> bool {
    for (size_t i = 0; i < Length; i++) {
      if (lhs.str_[i] != rhs.str_[i] || lhs.str_[i] == '\0') {
        return lhs.str_[i] < rhs.str_[i];
      }
    }
    return false;
  }
  friend auto operator>(const StringKey<Length> &lhs, const StringKey<Length> &rhs) -> bool {
    for (size_t i = 0; i < Length; i++) {
      if (lhs.str_[i] != rhs.str_[i] || lhs.str_[i] == '\0') {
        return lhs.str_[i] > rhs.str_[i];
      }
    }
    return false;
  }
  friend auto operator==(const StringKey &lhs, const StringKey &rhs) -> bool {
    for (size_t i = 0; i < Length; i++) {
      if (lhs.str_[i] != rhs.str_[i]) {
        return false;
      }
      if (lhs.str_[i] == '\0') {
        return true;
      }
    }
    return true;
  }
  friend auto operator!=(const StringKey<Length> &lhs, const StringKey<Length> &rhs) -> bool { return !(lhs == rhs); }
  explicit operator std::string() const { return std::string(str_); }
  friend auto operator<<(std::ostream &os, const StringKey &rhs) -> std::ostream & { return (os << rhs.str_); }
  friend auto operator>>(std::istream &is, const StringKey &rhs) -> std::istream & { return (is >> rhs.str_); }

 public:
  char str_[Length];
};

enum ComparatorType { CompareFirst, CompareBoth };
/**
 * @brief Class PairKey
 * @details Package the pair of key and value, enable assignment and comparison.
 */ 
template <typename T1, typename T2>
class PairKey : public Key {
 public:
  PairKey() = default;
  PairKey(const PairKey &other) : first_(other.first_), second_(other.second_) {}
  PairKey(const T1 &first, const T2 &second) : first_(first), second_(second) {}
  auto operator=(const PairKey<T1, T2> &other) -> PairKey & {
    first_ = other.first_;
    second_ = other.second_;
    return *this;
  }

 public:
  inline auto ToString() const -> std::string override {
    return ("{" + static_cast<std::string>(first_) + "," + std::to_string(second_) + "}");
  }
  friend auto operator==(const PairKey<T1, T2> &lhs, const PairKey<T1, T2> &rhs) -> bool { return lhs.first_ == rhs.first_ && lhs.second_ == rhs.second_; }
  friend auto operator<<(std::ostream &os, const PairKey<T1, T2> &rhs) -> std::ostream & {
    return (os << '{' << rhs.first_ << ',' << rhs.second_ << '}');
  }

 public:
  class Comparator {
   public:
    explicit Comparator(ComparatorType type = CompareBoth) : type_(type) {}
    Comparator(const Comparator &) = default;
    Comparator(Comparator &&other) noexcept { type_ = other.type_; }
    inline auto operator()(const PairKey<T1, T2> &lhs, const PairKey<T1, T2> &rhs) const -> int {
      if (type_ == CompareFirst || lhs.first_ != rhs.first_) {
        if (lhs.first_ < rhs.first_) {
          return -1;
        }
        if (rhs.first_ < lhs.first_) {
          return 1;
        }
        return 0;
      }
      if (lhs.second_ < rhs.second_) {
        return -1;
      }
      if (rhs.second_ < lhs.second_) {
        return 1;
      }
      return 0;
    }

   private:
    ComparatorType type_;
  };

 public:
  T1 first_{};
  T2 second_{};
};

template class PairKey<StringKey<65>, int>;

}  // namespace bustub