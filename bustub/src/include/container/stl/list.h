#pragma once

#ifndef SJTU_LIST_H
#define SJTU_LIST_H

#include <algorithm>
#include <cstddef>
#include <iterator>

#include "common/exception.h"

namespace sjtu {
template <class T>
class list {  // NOLINT
 private:
  struct Lnode {
    T data_;
    Lnode *prev_;
    Lnode *next_;
    explicit Lnode(const T &data) : data_(std::move(data)), prev_(nullptr), next_(nullptr) {}
  };

 public:
  template <bool const_tag>
  class base_iterator {  // NOLINT
    friend class list;
    friend class base_iterator<true>;
    friend class base_iterator<false>;

   protected:
    /**
     * TODO add data members
     *   just add whatever you want.
     */
    const list *iter_;
    Lnode *ptr_;

   public:
    // The following code is written for the C++ type_traits library.
    // Type traits is a C++ feature for describing certain properties of a type.
    // For instance, for an iterator, iterator::value_type is the type that the
    // iterator points to.
    // STL algorithms and containers may use these type_traits (e.g. the following
    // typedef) to work properly.
    // See these websites for more information:
    // https://en.cppreference.com/w/cpp/header/type_traits
    // About value_type: https://blog.csdn.net/u014299153/article/details/72419713
    // About iterator_category: https://en.cppreference.com/w/cpp/iterator
    using difference_type = std::ptrdiff_t;
    using value_type = T;
    using iterator_category = std::output_iterator_tag;
    using pointer = typename std::conditional<const_tag, const value_type *, value_type *>::type;
    using reference = typename std::conditional<const_tag, const value_type &, value_type &>::type;

    base_iterator() : iter_(nullptr), ptr_(nullptr) {}
    template <bool _const_tag>
    explicit base_iterator(const base_iterator<_const_tag> &other) : iter_(other.iter_), ptr_(other.ptr_) {}
    base_iterator(const list *iter, Lnode *ptr) : iter_(iter), ptr_(ptr) {}

    /**
     * TODO iter++
     */
    auto operator++(int) -> base_iterator {
      if (ptr_ == nullptr || ptr_ == iter_->tail_) {
        throw bustub::Exception(bustub::ExceptionType::OUT_OF_RANGE, "index out of range");
      }
      base_iterator cp = *this;
      ptr_ = ptr_->next_;
      return cp;
    }
    /**
     * TODO ++iter
     */
    auto operator++() -> base_iterator & {
      if (ptr_ == nullptr || ptr_ == iter_->tail_) {
        throw bustub::Exception(bustub::ExceptionType::OUT_OF_RANGE, "index out of range");
      }
      ptr_ = ptr_->next_;
      return *this;
    }
    /**
     * TODO iter--
     */
    auto operator--(int) -> base_iterator {
      if (ptr_ == nullptr || ptr_->prev_ == iter_->head_) {
        throw bustub::Exception(bustub::ExceptionType::OUT_OF_RANGE, "index out of range");
      }
      base_iterator cp = *this;
      ptr_ = ptr_->prev_;
      return cp;
    }
    /**
     * TODO --iter
     */
    auto operator--() -> base_iterator & {
      if (ptr_ == nullptr || ptr_->prev_ == iter_->head_) {
        throw bustub::Exception(bustub::ExceptionType::OUT_OF_RANGE, "index out of range");
      }
      ptr_ = ptr_->prev_;
      return *this;
    }

    auto operator+(int x) -> base_iterator {
      base_iterator cp = *this;
      while ((x--) != 0) {
        cp++;
      }
    }
    auto operator-(int x) -> base_iterator {
      base_iterator cp = *this;
      while ((x--) != 0) {
        cp--;
      }
    }

    /**
     * a operator to check whether two iterators are same (pointing to the same memory).
     */
    template <bool _const_tag>
    auto operator==(const base_iterator<_const_tag> &rhs) const -> bool {
      return iter_ == rhs.iter_ && ptr_ == rhs.ptr_;
    }
    template <bool _const_tag>
    auto operator!=(const base_iterator<_const_tag> &rhs) const -> bool {
      return iter_ != rhs.iter_ || ptr_ != rhs.ptr_;
    }
    /**
     * some other operator for iterator.
     */
    auto operator*() const -> reference { return this->ptr_->data_; }
    auto operator->() const -> pointer { return &this->ptr_->data_; }
  };

  using iterator = base_iterator<false>;
  using const_iterator = base_iterator<true>;

  list() {
    head_ = new Lnode(T());
    tail_ = new Lnode(T());
    head_->next_ = tail_;
    tail_->prev_ = head_;
  }
  list(const list &other) {
    head_ = new Lnode(T());
    tail_ = new Lnode(T());
    Lnode *cur = head_;
    Lnode *tmp = other.head_->next_;
    while (tmp != other.tail_) {
      cur->next_ = new Lnode(tmp->data_);
      cur->next_->prev_ = cur;
      cur = cur->next_;
      tmp = tmp->next_;
    }
    cur->next_ = tail_;
    tail_->prev_ = cur;
  }
  ~list() {
    Lnode *tmp = head_;
    Lnode *nex;
    while (tmp != nullptr) {
      nex = tmp->next_;
      delete tmp;
      tmp = nex;
    }
    head_ = tail_ = nullptr;
  }

  auto empty() const -> bool { return head_->next_ == tail_; }  // NOLINT

  auto begin() -> iterator { return iterator{this, head_->next_}; }               // NOLINT
  auto cbegin() -> const_iterator { return const_iterator{this, head_->next_}; }  // NOLINT
  auto end() -> iterator { return iterator{this, tail_}; }                        // NOLINT
  auto cend() -> const_iterator { return const_iterator{this, tail_}; }           // NOLINT

  auto front() const -> const T & {  // NOLINT
    if (empty()) {
      throw bustub::Exception(bustub::ExceptionType::OUT_OF_RANGE, "index out of range");
    }
    return head_->next_->data_;
  }
  auto tail() const -> const T & {  // NOLINT
    if (empty()) {
      throw bustub::Exception(bustub::ExceptionType::OUT_OF_RANGE, "index out of range");
    }
    return tail_->prev_->data_;
  }

  auto insert(iterator pos, const T &data) -> iterator {  // NOLINT
    auto *tmp = new Lnode(data);
    tmp->prev_ = pos.ptr_->prev_;
    pos.ptr_->prev_->next_ = tmp;
    tmp->next_ = pos.ptr_;
    pos.ptr_->prev_ = tmp;
    return {this, tmp};
  }

  void push_front(const T &data) { insert({this, head_->next_}, data); }  // NOLINT

  void push_back(const T &data) { insert({this, tail_}, data); }  // NOLINT

  void erase(iterator pos) {  // NOLINT
    if (pos.iter_ != this) {
      throw bustub::Exception(bustub::ExceptionType::INVALID, "pointing to another list");
    }
    pos.ptr_->prev_->next_ = pos.ptr_->next_;
    pos.ptr_->next_->prev_ = pos.ptr_->prev_;
    delete pos.ptr_;
    pos.ptr_ = nullptr;
  }

  void pop_front() {  // NOLINT
    if (empty()) {
      throw bustub::Exception(bustub::ExceptionType::OUT_OF_RANGE, "index out of range");
    }
    erase({this, head_->next_});
  }
  void pop_back() {  // NOLINT
    if (empty()) {
      throw bustub::Exception(bustub::ExceptionType::OUT_OF_RANGE, "index out of range");
    }
    erase({this, tail_->prev_});
  }

 private:
  Lnode *head_;
  Lnode *tail_;
};

template class list<int>;

}  // namespace sjtu

#endif
