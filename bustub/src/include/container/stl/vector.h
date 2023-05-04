#ifndef SJTU_VECTOR_H
#define SJTU_VECTOR_H

#include "common/exception.h"
#include "storage/page/page_guard.h"

#include <cmath>
#include <iostream>

namespace sjtu {
/**
 * a data container like std::vector
 * store data in a successive memory and support random access.
 */
template <typename T, class Allocator = std::allocator<T>>
class vector {  // NOLINT
 public:
  /**
   * TODO
   * a type for actions of the elements of a vector, and you should write
   *   a class named const_iterator with same interfaces.
   */
  using value_type = T;
  using allocator_type = Allocator;
  using size_type = size_t;
  using reference = value_type &;
  using const_reference = const value_type &;
  /**
   * you can see RandomAccessIterator at CppReference for help.
   */
  class const_iterator;  // NOLINT
  class iterator {       // NOLINT
                         // The following code is written for the C++ type_traits library.
                         // Type traits is a C++ feature for describing certain properties of a type.
                         // For instance, for an iterator, iterator::value_type is the type that the
                         // iterator points to.
                         // STL algorithms and containers may use these type_traits (e.g. the following
                         // typedef) to work properly. In particular, without the following code,
                         // @code{std::sort(iter, iter1);} would not compile.
                         // See these websites for more information:
                         // https://en.cppreference.com/w/cpp/header/type_traits
                         // About value_type: https://blog.csdn.net/u014299153/article/details/72419713
                         // About iterator_category: https://en.cppreference.com/w/cpp/iterator
   public:
    using difference_type = std::ptrdiff_t;
    using value_type = T;
    using pointer = T *;
    using reference = T &;
    using iterator_category = std::output_iterator_tag;

   private:
    /**
     * TODO add data members
     *   just add whatever you want.
     */
    const vector<T> *iter_;
    pointer ptr_;

    friend class vector;
    friend class const_iterator;

   public:
    /**
     * return a new iterator which pointer n-next elements
     * as well as operator-
     */
    iterator() : ptr_(nullptr) {}
    iterator(T *ptr, const vector<T> *iter) : iter_(iter), ptr_(ptr) {}
    iterator(const iterator &rhs) : iter_(rhs.iter_), ptr_(rhs.ptr_) {}
    auto operator=(const iterator &rhs) -> iterator & {
      ptr_ = rhs.ptr_, iter_ = rhs.iter_;
      return *this;
    }

    auto operator+(const int &n) const -> iterator { return iterator(ptr_ + n, iter_); }
    auto operator-(const int &n) const -> iterator { return iterator(ptr_ - n, iter_); }

    // return the distance between two iterators,
    // if these two iterators point to different vectors, throw invaild_iterator.
    auto operator-(const iterator &rhs) const -> int {
      if (iter_ != rhs.iter_) {
        throw bustub::Exception(bustub::ExceptionType::MISMATCH_TYPE, "pointing to different iter");
      }
      return ptr_ - rhs.ptr_;
    }
    auto operator+=(const int &n) -> iterator & {
      ptr_ += n;
      return *this;
    }
    auto operator-=(const int &n) -> iterator & {
      ptr_ -= n;
      return *this;
    }
    /**
     * TODO iter++
     */
    auto operator++(int) -> iterator {
      ptr_++;
      return iterator(ptr_ - 1, iter_);
    }
    /**
     * TODO ++iter
     */
    auto operator++() -> iterator & {
      ptr_++;
      return *this;
    }
    /**
     * TODO iter--
     */
    auto operator--(int) -> iterator {
      ptr_--;
      return iterator(ptr_ + 1, iter_);
    }
    /**
     * TODO --iter
     */
    auto operator--() -> iterator & {
      ptr_--;
      return *this;
    }
    /**
     * TODO *it
     */
    auto operator*() const -> reference { return *ptr_; }
    /**
     * a operator to check whether two iterators are same (pointing to the same memory address).
     */
    auto operator==(const iterator &rhs) const -> bool { return ptr_ == rhs.ptr_; }
    auto operator==(const const_iterator &rhs) const -> bool { return ptr_ == rhs.ptr_; }
    /**
     * some other operator for iterator.
     */
    auto operator!=(const iterator &rhs) const -> bool { return ptr_ != rhs.ptr_; }
    auto operator!=(const const_iterator &rhs) const -> bool { return ptr_ != rhs.ptr_; }
  };
  /**
   * TODO
   * has same function as iterator, just for a const object.
   */
  class const_iterator {
   public:
    using difference_type = std::ptrdiff_t;
    using value_type = T;
    using pointer = T *;
    using reference = T &;
    using iterator_category = std::output_iterator_tag;

   private:
    /**
     * TODO add data members
     *   just add whatever you want.
     */
    const vector<T> *iter_;
    pointer ptr_;

    friend class vector;
    friend class iterator;

   public:
    /**
     * return a new iterator which pointer n-next elements
     * as well as operator-
     */
    const_iterator(T *ptr, const vector<T> *iter) : iter_(iter), ptr_(ptr) {}
    explicit const_iterator(const iterator &rhs) : iter_(rhs.iter_), ptr_(rhs.ptr_) {}
    const_iterator(const const_iterator &rhs) : iter_(rhs.iter_), ptr_(rhs.ptr_) {}

    auto operator+(const int &n) const -> const_iterator { return const_iterator(ptr_ + n, iter_); }
    auto operator-(const int &n) const -> const_iterator { return const_iterator(ptr_ - n, iter_); }

    // return the distance between two iterators,
    // if these two iterators point to different vectors, throw invaild_iterator.
    auto operator-(const const_iterator &rhs) const -> int {
      if (iter_ != rhs.iter_) {
        throw bustub::Exception(bustub::ExceptionType::MISMATCH_TYPE, "pointing to different iter");
      }
      return ptr_ - rhs.ptr_;
    }
    auto operator+=(const int &n) -> const_iterator & {
      ptr_ += n;
      return *this;
    }
    auto operator-=(const int &n) -> const_iterator & {
      ptr_ -= n;
      return *this;
    }
    /**
     * TODO iter++
     */
    auto operator++(int) -> const_iterator {
      ptr_++;
      return const_iterator(ptr_ - 1, iter_);
    }
    /**
     * TODO ++iter
     */
    auto operator++() -> const_iterator & {
      ptr_++;
      return *this;
    }
    /**
     * TODO iter--
     */
    auto operator--(int) -> const_iterator {
      ptr_--;
      return const_iterator(ptr_ + 1, iter_);
    }
    /**
     * TODO --iter
     */
    auto operator--() -> const_iterator & {
      ptr_--;
      return *this;
    }
    /**
     * TODO *it
     */
    auto operator*() const -> const_reference { return *ptr_; }
    /**
     * a operator to check whether two iterators are same (pointing to the same memory address).
     */
    auto operator==(const iterator &rhs) const -> bool { return ptr_ == rhs.ptr_; }
    auto operator==(const const_iterator &rhs) const -> bool { return ptr_ == rhs.ptr_; }
    /**
     * some other operator for iterator.
     */
    auto operator!=(const iterator &rhs) const -> bool { return ptr_ != rhs.ptr_; }
    auto operator!=(const const_iterator &rhs) const -> bool { return ptr_ != rhs.ptr_; }
  };
  /**
   * TODO Constructs
   * At least two: default constructor, copy constructor
   */
  vector() noexcept {
    start_ = iterator(alloc_.allocate(1), this);
    finish_ = start_;
    end_of_storage_ = start_ + 1;
  }
  vector(const vector &other) noexcept {
    start_ = iterator(alloc_.allocate(other.capacity()), this);
    finish_ = start_ + other.size();
    end_of_storage_ = start_ + other.capacity();
    for (iterator it1 = start_, it2 = other.start_; it2 != other.finish_; it1++, it2++) {
      alloc_.construct(it1.ptr_, *it2);
    }
  }
  vector(vector &&other) noexcept = default;
  explicit vector(size_type count, const T &value = T()) noexcept {
    int new_size = pow(2, ceil(log2(count)));
    start_ = iterator(alloc_.allocate(new_size), this);
    finish_ = start_ + count;
    end_of_storage_ = start_ + new_size;
    for (iterator it = start_; it != finish_; it++) {
      alloc_.construct(it.ptr_, value);
    }
  }
  /**
   * TODO Destructor
   */
  ~vector() {
    clear();
    alloc_.deallocate(start_.ptr_, capacity());
    start_ = finish_ = end_of_storage_ = iterator(nullptr, this);
  }
  /**
   * TODO Assignment operator
   */
  auto operator=(const vector &other) -> vector & {
    if (this == &other) {
      return *this;
    }
    if (start_.ptr_ != nullptr) {
      alloc_.deallocate(start_.ptr_, capacity());
    }
    start_.ptr_ = alloc_.allocate(other.capacity());
    finish_ = start_ + other.size();
    end_of_storage_ = start_ + other.capacity();
    std::copy(other.start_, other.finish_, start_);
    return *this;
  }
  /**
   * assigns specified element with bounds checking
   * throw index_out_of_bound if pos is not in [0, size)
   */
  auto at(const size_type &pos) -> reference {  // NOLINT
    if (pos >= size()) {
      throw bustub::Exception(bustub::ExceptionType::OUT_OF_RANGE, "index out of range");
    }
    return *(begin() + pos);
  }
  auto at(const size_type &pos) const -> const_reference {  // NOLINT
    if (pos >= size()) {
      throw bustub::Exception(bustub::ExceptionType::OUT_OF_RANGE, "index out of range");
    }
    return *(cbegin() + pos);
  }
  /**
   * assigns specified element with bounds checking
   * throw index_out_of_bound if pos is not in [0, size)
   * !!! Pay attentions
   *   In STL this operator does not check the boundary but I want you to do.
   */
  auto operator[](const size_type &pos) -> reference {
    if (pos >= size()) {
      throw bustub::Exception(bustub::ExceptionType::OUT_OF_RANGE, "index out of range");
    }
    return *(begin() + pos);
  }
  auto operator[](const size_type &pos) const -> const_reference {
    if (pos >= size()) {
      throw bustub::Exception(bustub::ExceptionType::OUT_OF_RANGE, "index out of range");
    }
    return *(cbegin() + pos);
  }
  /**
   * access the first element.
   * throw container_is_empty if size == 0
   */
  auto front() -> reference {  // NOLINT
    if (!size()) {
      throw bustub::Exception(bustub::ExceptionType::OUT_OF_RANGE, "container is empty");
    }
    return *begin();
  };
  /**
   * access the last element.
   * throw container_is_empty if size == 0
   */
  auto back() -> reference {  // NOLINT
    if (!size()) {
      throw bustub::Exception(bustub::ExceptionType::OUT_OF_RANGE, "container is empty");
    }
    return *std::prev(end().ptr_);
  }
  /**
   * returns an iterator to the beginning.
   */
  auto begin() noexcept -> iterator { return start_; }                               // NOLINT
  auto cbegin() const noexcept -> const_iterator { return const_iterator(start_); }  // NOLINT
  /**
   * returns an iterator to the end.
   */
  auto end() -> iterator { return finish_; }                                        // NOLINT
  auto cend() const noexcept -> const_iterator { return const_iterator(finish_); }  // NOLINT
  /**
   * checks whether the container is empty
   */
  auto empty() const noexcept -> bool { return start_ == finish_; }  // NOLINT
  /**
   * returns the number of elements
   */
  auto size() const noexcept -> size_t { return finish_ - start_; }              // NOLINT
  auto capacity() const noexcept -> size_t { return end_of_storage_ - start_; }  // NOLINT
  /**
   * clears the contents
   */
  void clear() {  // NOLINT
    if (empty()) {
      return;
    }
    while (--finish_ != start_) {
      alloc_.destroy(finish_.ptr_);
    }
    alloc_.destroy(start_.ptr_);
  }
  /**
   * reserve the space
   */
  void reserve(const size_type &new_cap) {  // NOLINT
    if (new_cap <= capacity()) {
      return;
    }
    int new_size = pow(2, ceil(log2(new_cap)));
    iterator new_start = iterator(alloc_.allocate(new_size), this);
    iterator new_finish = new_start + size();
    iterator new_end_of_storage = new_start + new_size;
    try {
      for (iterator it1 = new_start, it2 = start_; it2 != finish_; it1++, it2++) {
        alloc_.construct(it1.ptr_, *it2);
      }
    } catch (...) {
      alloc_.deallocate(new_start.ptr_, new_size);
      throw;
    }
    clear();
    alloc_.deallocate(start_.ptr_, capacity());
    start_ = new_start;
    finish_ = new_finish;
    end_of_storage_ = new_end_of_storage;
  }
  /**
   * inserts value before pos
   * returns an iterator pointing to the inserted value.
   */
  auto insert(iterator pos, const value_type &value) -> iterator {  // NOLINT
    if (pos.ptr_ < start_.ptr_ || pos.ptr_ > finish_.ptr_) {
      throw bustub::Exception(bustub::ExceptionType::OUT_OF_RANGE, "index out of range");
    }
    int index = pos - start_;
    if (finish_ == end_of_storage_) {
      reserve(capacity() << 1);
    }
    pos = start_ + index;
    alloc_.construct(finish_.ptr_, *(finish_ - 1));
    finish_++;
    std::copy_backward(pos, finish_ - 2, finish_ - 1);
    *pos = value;
    return pos;
  }
  /**
   * inserts value at index ind.
   * after inserting, this->at(ind) == value
   * returns an iterator pointing to the inserted value.
   * throw index_out_of_bound if ind > size (in this situation ind can be size because after inserting the size will
   * increase 1.)
   */
  auto insert(const size_t &pos, const T &data) -> iterator {  // NOLINT
    if (pos > size()) {
      throw bustub::Exception(bustub::ExceptionType::OUT_OF_RANGE, "index out of range");
    }
    return insert(begin() + pos, data);
  }
  /**
   * removes the element at pos.
   * return an iterator pointing to the following element.
   * If the iterator pos refers the last element, the end() iterator is returned.
   */
  auto erase(iterator pos) -> iterator {  // NOLINT
    if (pos.ptr_ < start_.ptr_ || pos.ptr_ >= finish_.ptr_) {
      throw bustub::Exception(bustub::ExceptionType::OUT_OF_RANGE, "index out of range");
    }
    if (pos + 1 != finish_) {
      std::copy(pos + 1, finish_, pos);
    }
    --finish_;
    alloc_.destroy(finish_.ptr_);
    return pos;
  }
  /**
   * removes the element with index ind.
   * return an iterator pointing to the following element.
   * throw index_out_of_bound if ind >= size
   */
  auto erase(const size_t &pos) -> iterator { return erase(begin() + pos); }  // NOLINT
  /**
   * adds an element to the end.
   */
  void push_back(const value_type &value) {  // NOLINT
    if (finish_ == end_of_storage_) {
      reserve(capacity() << 1);
    }
    alloc_.construct(finish_.ptr_, value);
    finish_++;
  }
  void push_back(value_type &&value) {  // NOLINT
    if (finish_ == end_of_storage_) {
      reserve(capacity() << 1);
    }
    alloc_.construct(finish_.ptr_, value);
    finish_++;
  }
  /**
   * remove the last element from the end.
   * throw container_is_empty if size() == 0
   */
  void pop_back() {  // NOLINT
    finish_--;
    alloc_.destroy(finish_.ptr_);
  }

 private:
  iterator start_;
  iterator finish_;
  iterator end_of_storage_;
  allocator_type alloc_;
};

template class vector<int>;

template <>
class vector<bustub::BasicPageGuard> {
 public:
  using T = bustub::BasicPageGuard;
  void push_back(T &&value) { data_[++tail_] = new T(std::move(value)); }  // NOLINT
  void pop_back() { delete data_[tail_--]; }                               // NOLINT
  auto back() -> T & { return *data_[tail_]; }                             // NOLINT
  auto empty() -> bool { return tail_ == 0; }                              // NOLINT

 private:
  T *data_[1000];
  int tail_{0};
};

}  // namespace sjtu

#endif