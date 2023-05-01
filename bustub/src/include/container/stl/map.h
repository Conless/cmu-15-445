/**
 * implement a container like std::map
 */
#ifndef SJTU_MAP_H
#define SJTU_MAP_H

// only for std::less<T>
#include <cstddef>
#include <functional>
#include "common/exception.h"

namespace sjtu {

template <class Key, class T, class Compare = std::less<Key>>
class RBTree {
 public:
  /**
   * the internal type of data.
   * it should have a default constructor, a copy constructor.
   * You can use sjtu::map as value_type by typedef.
   */
  using value_type = std::pair<const Key, T>;

  /**
   * the main data of red-black tree
   *
   */
  enum Color { BLACK, RED };
  struct Tnode {
    value_type data_;
    Tnode *left_, *right_, *parent_;
    Color col_;
    int siz_;

    Tnode(value_type data, Tnode *parent, Color col, int siz = 0)
        : data_(std::move(data)), left_(nullptr), right_(nullptr), parent_(parent), col_(col), siz_(siz) {}
  } * rt_;

 public:
  RBTree() { rt_ = nullptr; }
  RBTree(const RBTree<Key, T, Compare> &other) { rt_ = node_copy(other.rt_); }

  auto operator=(const RBTree &other) -> RBTree & {
    if (this == &other) {
      return *this;
    }
    rt_ = node_copy(other.rt_);
    return *this;
  }

  ~RBTree() { node_destruct(rt_); }

 public:
  /**
   * @brief checks whether the container is empty
   *
   * @return true if empty
   * @return false otherwise
   */
  auto empty() -> bool { return rt_ == nullptr; }  // NOLINT
  /**
   * @brief returns the number of elements.
   *
   * @return size_t
   */
  auto size() const -> size_t { return rt_ ? rt_->siz_ : 0; }  // NOLINT
  /**
   * @brief clear the contents
   *
   */
  void clear() { node_destruct(rt_); }  // NOLINT

 public:
  auto find(const Key &key) const -> Tnode * {  // NOLINT
    Tnode *cur = rt_;
    while (cur != nullptr) {
      /**
       * if key < cur->key, comp = 0 - 1 = -1
       * if key = cur->key, comp = 0 - 0 = 0
       * if key > cur->key, comp = 1 - 0 = 1
       * (same below)
       */
      int comp = Compare()(cur->data_.first, key) - Compare()(key, cur->data_.first);
      if (comp == 0) {
        break;
      }
      if (comp < 0) {
        cur = cur->left_;
      } else {
        cur = cur->right_;
      }
    }
    return cur;
  }

  auto first() const -> Tnode * {  // NOLINT
    Tnode *u = rt_;
    if (u == nullptr) {
      return nullptr;
    }
    while (u->left_ != nullptr) {
      u = u->left_;
    }
    return u;
  }

  auto last() const -> Tnode * {  // NOLINT
    Tnode *u = rt_;
    if (u == nullptr) {
      return nullptr;
    }
    while (u->right_ != nullptr) {
      u = u->right_;
    }
    return u;
  }

  auto prev(Tnode *ptr) const -> Tnode * {  // NOLINT
    if (ptr->left_) {
      ptr = ptr->left_;
      while (ptr->right_ != nullptr) {
        ptr = ptr->right_;
      }
    } else {
      while (ptr != rt_ && is_left(ptr)) {
        ptr = ptr->parent_;
      }
      if (ptr->parent_ == nullptr) {
        throw bustub::Exception(bustub::ExceptionType::OUT_OF_RANGE, "index out of range");
      }
      ptr = ptr->parent_;
    }
    return ptr;
  }

  auto next(Tnode *ptr) const -> Tnode * {  // NOLINT
    if (ptr == nullptr) {
      throw bustub::Exception(bustub::ExceptionType::OUT_OF_RANGE, "index out of range");
    }
    if (ptr->right_) {
      ptr = ptr->right_;
      while (ptr->left_ != nullptr) {
        ptr = ptr->left_;
      }
    } else {
      while (ptr != rt_ && !is_left(ptr)) {
        ptr = ptr->parent_;
      }
      ptr = ptr->parent_;
    }
    return ptr;
  }

 public:
  auto insert(const value_type &value) -> std::pair<Tnode *, bool> {  // NOLINT
    Tnode *cur = rt_;
    Tnode *next;
    if (cur == nullptr) {  // If the tree is empty
      // Create a new root node, with col = RED, size = 1 and no links to other node
      rt_ = cur = new Tnode(value, nullptr, BLACK, 1);
      return {cur, true};
    }
    // Here we try to ensure the node we found cannot have a red sibling,
    // which requires that every node on the path doesn't have two red descendants.
    while (true) {
      int comp = Compare()(cur->data_.first, value.first) - Compare()(value.first, cur->data_.first);
      if (comp == 0) {  // Find the same element
        return {cur, false};
      }
      // If the current node has two red descendeants, we should change them to black.
      if ((cur->left_ && cur->left_->col_ == RED) && (cur->right_ && cur->right_->col_ == RED)) {
        cur->col_ = RED;
        cur->left_->col_ = cur->right_->col_ = BLACK;
        // fix the situation if there's a red-red link to its parent.
        insert_adjust(cur);
      }
      if (comp < 0) {
        if (cur->left_ == nullptr) {
          cur = cur->left_ = new Tnode(value, cur, RED);
          break;
        }
        cur = cur->left_;
      } else {
        if (cur->right_ == nullptr) {
          cur = cur->right_ = new Tnode(value, cur, RED);
          break;
        }
        cur = cur->right_;
      }
    }
    // Change the size backward
    size_adjust_upward(cur, 1);
    // After inserted, fix the red-red link again
    insert_adjust(cur);
    return {cur, true};
  }

  void erase(const Key &key) {  // NOLINT
    if (!Compare()(rt_->data_.first, key) && !Compare()(rt_->data_.first, key) && rt_->left_ == nullptr &&
        rt_->right_ == nullptr) {
      delete rt_;
      rt_ = nullptr;
      return;
    }
    Tnode *cur = rt_;
    // Here we try to ensure every node we met is red, which then conducts the node we want to delete is red
    while (true) {
      if (cur == nullptr) {
        return;
      }
      // Change the current node to red
      erase_adjust(cur, key);
      int comp = Compare()(cur->data_.first, key) - Compare()(key, cur->data_.first);
      // If we find the node with two descendents,
      // swap the data with its 'next' node and delete that node then
      if ((comp == 0) && cur->left_ != nullptr && cur->right_ != nullptr) {
        Tnode *next = cur->right_;
        while (next->left_) {
          next = next->left_;
        }
        node_swap(cur, next);
        cur = next->right_;
        continue;
      }
      // If we find the node
      if (comp == 0) {
        Tnode *replacement = cur->left_ == nullptr ? cur->right_ : cur->left_;
        if (is_left(cur)) {
          cur->parent_->left_ = replacement;
        } else {
          cur->parent_->right_ = replacement;
        }
        size_adjust_upward(cur, -1);
        delete cur;
        return;
      }
      // Go to the next node
      cur = comp > 0 ? cur->right_ : cur->left_;
    }
  }

 private:
  /**
   * @brief custom exceptions for the protected and private functions of map
   *
   */
  void size_adjust(Tnode *cur) {  // NOLINT
    cur->siz_ = 1;
    if (cur->left_ != nullptr) {
      cur->siz_ += cur->left_->siz_;
    }
    if (cur->right_ != nullptr) {
      cur->siz_ += cur->right_->siz_;
    }
  }
  /**
   * @brief Adjust the size of the nodes upward
   *
   * @param cur
   * @param delta
   */
  void size_adjust_upward(Tnode *cur, int delta) {  // NOLINT
    while (cur != nullptr) {
      cur->siz_ += delta;
      cur = cur->parent_;
    }
  }

  /**
   * @brief Fix the situation when there's a red-red link between the selected node and its parent
   *
   * @param cur
   */
  void insert_adjust(Tnode *cur) {  // NOLINT
    if (cur->parent_ == nullptr || cur->parent_->col_ == BLACK) {
      return;
    }
    if (cur->parent_ == rt_) {
      cur->parent_->col_ = BLACK;
      return;
    }
    if (is_left(cur->parent_)) {
      if (is_left(cur)) {
        /** Change the tree by
         *      B1                       B2
         *     / \                      / \
         *    R2  B3     ------->  (cur)R  R1
         *   /                              \
         *  R(cur)                           B3
         */
        right_rotate(cur->parent_->parent_);
        std::swap(cur->parent_->col_, sibling(cur)->col_);
      } else {
        /** Change the tree by
         *      B1                     B1            R(cur)         B(cur)
         *     / \                    / \           / \            /  \
         *    R2  B3     -----> (cur)R   B3 -----> R2  B1  -----> R2  R1
         *     \                   /                    \               \
         *      R(cur)            R2                     B3             B3
         */
        left_rotate(cur->parent_);
        right_rotate(cur->parent_);
        std::swap(cur->col_, cur->right_->col_);
      }
    } else {
      if (is_left(cur)) {
        /** Change the tree by
         *      B1               B1                 R(cur)          B(cur)
         *     / \              / \                / \             / \
         *    B2  R3    -----> B2  R(cur) ----->  B1  R3   -----> R1  R3
         *       /                  \            /               /
         *      R(cur)               R3         B2              B2
         */
        right_rotate(cur->parent_);
        left_rotate(cur->parent_);
        std::swap(cur->col_, cur->left_->col_);
      } else {
        /** Change the tree by
         *       B1                R3                B3
         *      / \               / \               /  \
         *     B2  R3  ------->  B1  R(cur) -----> R1  R(cur)
         *          \           /                 /
         *          R(cur)     B2                B2
         */
        left_rotate(cur->parent_->parent_);
        std::swap(cur->parent_->col_, sibling(cur)->col_);
      }
    }
  }

  /**
   * @brief Adjust every node on the path to red node
   *
   * @param cur
   */
  void erase_adjust(Tnode *cur, Key del) {  // NOLINT
    // If the current node is red, we don't need to change it.
    // Notice: it only happens when current node is root.
    if (cur->col_ == RED) {
      return;
    }
    /**
     * if del < cur->key, comp = 0 - 1 = -1
     * if del = cur->key, comp = 0 - 0 = 0
     * if del > cur->key, comp = 1 - 0 = 1
     */
    int comp = Compare()(cur->data_.first, del) - Compare()(del, cur->data_.first);
    if (has_black_descendants(cur)) {
      // note that sib == nullptr suggest it has no siblings or it's the root
      Tnode *sib = sibling(cur);
      // If the (black) sibling node has two black descendants or it doesn't have a sibling
      /** Case 1-1: cur (black, black, black), sib null or (black, black, black)
       *  That would not change the structure of the tree
       */
      if (sib == nullptr || has_black_descendants(sib)) {
        if (cur->parent_) {
          cur->parent_->col_ = BLACK;
        }
        if (sib) {
          sib->col_ = RED;
        }
        cur->col_ = RED;
        return;
      }
      // If the current node has a sibling which has red descendent
      /** Case 1-2: cur (black, black, black) at left, sib with outer red at right
       *      R(par)                    B(sib)                 R(sib)
       *     /   \                     /  \                   /   \
       *    B(cur)B(sib) -------->  R(par) R2 -------->     B(par) B2
       *           \               /                       /
       *            R2            B(cur)                R(cur)
       *  That is a left_rotate on par and change color then
       */
      if (is_left(cur) && sib->right_ && sib->right_->col_ == RED) {
        left_rotate(cur->parent_);
        sib->col_ = RED;
        cur->parent_->col_ = BLACK;
        sib->right_->col_ = BLACK;
        cur->col_ = RED;
        return;
      }
      /** Case 1-3: cur (black, black, black) at right, sib with outer red at left
       *     R(par)                   B(sib)                 R(sib)
       *    /    \                   /  \                   /  \
       *   B(sib) B(cur) -------->  R1   R(par)  ------->  B1  B(par)
       *  /                                \                     \
       * R1                                 B(cur)                R(cur)
       *  That is a right_rotate on par and change color then
       */
      if (!is_left(cur) && sib->left_ && sib->left_->col_ == RED) {
        right_rotate(cur->parent_);
        sib->col_ = RED;
        sib->left_->col_ = BLACK;
        cur->parent_->col_ = BLACK;
        cur->col_ = RED;
      }
      /** Case 1-4: cur (black, black, black) at left, sib with inner red at left
       *      R(par)                    R(par)                  R1                   R1
       *     /    \                     /  \                   /  \                 /  \
       *    B(cur) B(sib) -------->  B(cur) R1 -------->    R(par) B(sib) -----> B(par) B(sib)
       *          /                          \             /                    /
       *         R1                          B(sib)       B(cur)              R(cur)
       *  That is a right_rotate on sib, left_rotate on par and change color then
       */
      if (is_left(cur) && sib->left_ && sib->left_->col_ == RED) {
        right_rotate(sib);
        left_rotate(cur->parent_);
        std::swap(cur->col_, cur->parent_->col_);
      }
      /** Case 1-5: cur (black, black, black) at right, sib with inner red at right
       *      R(par)                      R(par)                 R1                   B1
       *     /    \                      /  \                   /  \                 /  \
       *    B(sib) B(cur) -------->    R1   B(cur) --------> B(sib) R(par) -----> B(sib) B(sib)
       *     \                        /                               \                   \
       *      R1                    B(sib)                           B(cur)                R(cur)
       *  That is a left_rotate on sib, right_rotate on par and change color then
       */
      if (!is_left(cur) && sib->right_ && sib->right_->col_ == RED) {
        left_rotate(sib);
        right_rotate(cur->parent_);
        std::swap(cur->col_, cur->parent_->col_);
      }
    } else {
      // If the current node is the node to be deleted
      if (comp == 0) {
        // If the current node has two descendents
        if (cur->left_ && cur->right_) {
          // If the right descendent is black
          /** Case 2-1: cur (black, red, black), which implies that left node has two black descendents
           *     B(cur)      R1         B1
           *    /     ----->  \  ----->  \
           *   R1              B(cur)    R(cur)
           */
          if (cur->right_->col_ == BLACK) {
            right_rotate(cur);
            std::swap(cur->col_, cur->parent_->col_);
          }
          /** Case 2-2: cur (black, black, red), we'll reach its right node then, so don't need to change anything
           */
          else {
          }
          return;
        }
        /** Case 2-2: cur (black, red, null)
         *    B(cur)     R1           B1
         *   /    ----->  \   ------>  \
         *  R1             B(cur)      R(cur)
         */
        if (cur->left_) {
          right_rotate(cur);
          std::swap(cur->col_, cur->parent_->col_);
        }
        /** Case 2-3: cur (black, null, red)
         *   B(cur)        R1          B1
         *    \    -----> /    -----> /
         *    R1         B(cur)      R(cur)
         */
        if (cur->right_) {
          left_rotate(cur);
          std::swap(cur->col_, cur->parent_->col_);
          return;
        }
      }
      // If the current node isn't the node to be deleted
      else {
        /** Case 2-3: we'll reach a red node or nullptr then, so don't need to change anything
         */
        if ((comp < 0 && (!cur->left_ || cur->left_->col_ == RED)) ||
            (comp > 0 && (!cur->right_ || cur->right_->col_ == RED))) {
          return;
        }
        /** Case 2-4: cur (black, black, red) and we'll reach its left node then
         *      B(cur)          R2              B2
         *     / \              /              /
         *    B1 R2    -----> B(cur) ----->   R(cur)
         *                   /               /
         *                  B1              B1
         * That is a left_rotate on cur and change color then
         */
        if (comp < 0 && cur->left_->col_ == BLACK) {
          left_rotate(cur);
          std::swap(cur->col_, cur->parent_->col_);
          return;
        }
        /** Case 2-5: cur (black, black, red) and we'll reach its left node then
         *      B(cur)       R1              B1
         *     / \            \               \
         *    R1 B2    ----->  B(cur) ----->   R(cur)
         *                      \               \
         *                      B2               B2
         * That is a left_rotate on cur and change color then
         */
        if (comp > 0 && cur->right_->col_ == BLACK) {
          right_rotate(cur);
          std::swap(cur->col_, cur->parent_->col_);
          return;
        }
      }
    }
  }

  /**
   * @brief Rotate the selected node to its left child, with its right child replacing the current position
   *   cur              r0
   *  /  \             /  \
   * l0   r0  ---->  cur  r1
   *     /  \       /  \
   *    l1  r1     l0  l1
   * @throw custom_exception when the selected node doesn't have a right child
   *
   * @param cur
   */
  void left_rotate(Tnode *cur) {  // NOLINT
    if (cur->right_ == nullptr) {
      throw bustub::Exception(bustub::ExceptionType::INVALID, "The node to rotate (left) doesn't have a right child.");
    }
    if (cur->parent_) {
      if (is_left(cur)) {
        cur->parent_->left_ = cur->right_;
      } else {
        cur->parent_->right_ = cur->right_;
      }
    } else {
      rt_ = cur->right_;
    }
    Tnode *tmp = cur->right_;
    cur->right_->parent_ = cur->parent_;
    cur->right_ = tmp->left_;
    if (tmp->left_) {
      tmp->left_->parent_ = cur;
    }
    tmp->left_ = cur;
    cur->parent_ = tmp;
    size_adjust(cur);
    size_adjust(tmp);
  }

  /**
   * @brief Rotate the selected node to its right child, with its left child replacing the current position
   *      cur            l0
   *     /  \           /  \
   *    l0  r0  ---->  l1  cur
   *   /  \               /  \
   *  l1  r1             r1  r0
   * @throw custom_exception when the selected node doesn't have a left child
   *
   * @param cur
   */
  void right_rotate(Tnode *cur) {  // NOLINT
    if (cur->left_ == nullptr) {
      throw bustub::Exception(bustub::ExceptionType::INVALID, "The node to rotate (right) doesn't have a left child.");
    }
    if (cur->parent_) {
      if (is_left(cur)) {
        cur->parent_->left_ = cur->left_;
      } else {
        cur->parent_->right_ = cur->left_;
      }
    } else {
      rt_ = cur->left_;
    }
    Tnode *tmp = cur->left_;
    tmp->parent_ = cur->parent_;
    cur->left_ = tmp->right_;
    if (tmp->right_) {
      tmp->right_->parent_ = cur;
    }
    tmp->right_ = cur;
    cur->parent_ = tmp;
    size_adjust(cur);
    size_adjust(tmp);
  }

 private:
  /**
   * @brief copy a tree node
   *
   * @param target
   * @param _parent
   * @return return the copy of selected tree node
   */
  auto node_copy(Tnode *target, Tnode *_parent = nullptr) -> Tnode * {  // NOLINT
    if (target == nullptr) {
      return nullptr;
    }
    auto *tmp = new Tnode(target->data_, _parent, target->col_, target->siz_);
    tmp->left_ = node_copy(target->left_, tmp);
    tmp->right_ = node_copy(target->right_, tmp);
    return tmp;
  }

  /**
   * @brief swap the node with another tree node, since the Key type doesn't even support the f**king assignment
   * operation
   *
   * @param cur
   * @param target
   */
  void node_swap(Tnode *cur, Tnode *target) {  // NOLINT
    // parent
    int cur_is_left = target->parent_ ? is_left(target) : 0;
    int tar_is_left = cur->parent_ ? is_left(cur) : 0;
    std::swap(cur->parent_, target->parent_);
    if (cur->parent_) {
      if (cur_is_left != 0) {
        cur->parent_->left_ = cur;
      } else {
        cur->parent_->right_ = cur;
      }
    } else {
      rt_ = cur;
    }
    if (target->parent_) {
      if (tar_is_left != 0) {
        target->parent_->left_ = target;
      } else {
        target->parent_->right_ = target;
      }
    } else {
      rt_ = target;
    }
    // left
    std::swap(cur->left_, target->left_);
    if (cur->left_) {
      cur->left_->parent_ = cur;
    }
    if (target->left_) {
      target->left_->parent_ = target;
    }
    // right
    std::swap(cur->right_, target->right_);
    if (cur->right_) {
      cur->right_->parent_ = cur;
    }
    if (target->right_) {
      target->right_->parent_ = target;
    }
    // color and size
    std::swap(cur->col_, target->col_);
    std::swap(cur->siz_, target->siz_);
  }

  /**
   * @brief destruct a tree node and all its progenies by recursion
   *
   * @param target
   */
  void node_destruct(Tnode *&target) {  // NOLINT
    if (target == nullptr) {
      return;
    }
    node_destruct(target->left_);
    node_destruct(target->right_);
    delete target;
    target = nullptr;
  }

  /**
   * @brief Judge if a node is the left child of its parent
   * @throw custom_exception when passing the root node
   *
   * @param cur
   * @return true when is the left child
   * @return false when is the right child
   */
  auto is_left(Tnode *cur) const -> bool {  // NOLINT
    if (cur->parent_ == nullptr) {
      throw bustub::Exception(bustub::ExceptionType::INVALID, "Unexpected operations on the root node.");
    }
    return cur->parent_->left_ == cur;
  }

  auto has_black_descendants(Tnode *cur) -> bool {  // NOLINT
    return ((cur->left_ && cur->left_->col_ == BLACK) || cur->left_ == nullptr) &&
           ((cur->right_ && cur->right_->col_ == BLACK) || cur->right_ == nullptr);
  }

  /**
   * @brief Get the sibling of the selected node
   * @throw custom_exception by is_left() when passing the root node
   *
   * @param cur
   * @return Tnode*
   */
  auto sibling(Tnode *cur) -> Tnode * {  // NOLINT
    if (cur == rt_) {
      return nullptr;
    }
    return is_left(cur) ? cur->parent_->right_ : cur->parent_->left_;
  }
};

struct my_true_type {  // NOLINT
  static constexpr bool VALUE = true;
};
struct my_false_type {  // NOLINT
  static constexpr bool VALUE = false;
};
template <typename T>
struct my_type_traits {  // NOLINT
  using iterator_assignable = typename T::iterator_assignable;
};

template <class Key, class T, class Compare = std::less<Key>>
class map : public RBTree<Key, T, Compare> {  // NOLINT
 public:
  using Tnode = typename RBTree<Key, T, Compare>::Tnode;
  using value_type = typename RBTree<Key, T, Compare>::value_type;

  /**
   * see BidirectionalIterator at CppReference for help.
   *
   * if there is anything wrong throw invalid_iterator.
   *     like it = map.begin(); --it;
   *       or it = map.end(); ++end();
   */
  template <bool const_tag>
  class base_iterator {  // NOLINT
    friend class map;
    friend class base_iterator<true>;
    friend class base_iterator<false>;

   protected:
    /**
     * TODO add data members
     *   just add whatever you want.
     */
    const map *iter_;
    Tnode *ptr_;

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
    using value_type = typename RBTree<Key, T, Compare>::value_type;
    using iterator_category = std::output_iterator_tag;
    using pointer = typename std::conditional<const_tag, const value_type *, value_type *>::type;
    using reference = typename std::conditional<const_tag, const value_type &, value_type &>::type;
    using iterator_assignable = typename std::conditional<const_tag, my_false_type, my_true_type>::type;

    base_iterator() : iter_(nullptr), ptr_(nullptr) {}
    template <bool _const_tag>
    explicit base_iterator(const base_iterator<_const_tag> &other) : iter_(other.iter_), ptr_(other.ptr_) {}
    base_iterator(const map *iter, Tnode *ptr) : iter_(iter), ptr_(ptr) {}
    /**
     * TODO iter++
     */
    auto operator++(int) -> base_iterator {
      if (ptr_ == nullptr) {
        throw bustub::Exception(bustub::ExceptionType::OUT_OF_RANGE, "index out of range");
      }
      base_iterator cp = *this;
      ptr_ = iter_->next(ptr_);
      return cp;
    }
    /**
     * TODO ++iter
     */
    auto operator++() -> base_iterator & {
      if (ptr_ == nullptr) {
        throw bustub::Exception(bustub::ExceptionType::OUT_OF_RANGE, "index out of range");
      }
      ptr_ = iter_->next(ptr_);
      return *this;
    }
    /**
     * TODO iter--
     */
    auto operator--(int) -> base_iterator {
      base_iterator cp = *this;
      if (ptr_ == nullptr) {
        ptr_ = iter_->last();
      } else {
        ptr_ = iter_->prev(ptr_);
      }
      if (ptr_ == nullptr) {
        throw bustub::Exception(bustub::ExceptionType::OUT_OF_RANGE, "index out of range");
      }
      return cp;
    }
    /**
     * TODO --iter
     */
    auto operator--() -> base_iterator & {
      if (ptr_ == nullptr) {
        ptr_ = iter_->last();
      } else {
        ptr_ = iter_->prev(ptr_);
      }
      if (ptr_ == nullptr) {
        throw bustub::Exception(bustub::ExceptionType::OUT_OF_RANGE, "index out of range");
      }
      return *this;
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

  /**
   * TODO two constructors
   */
  map() : RBTree<Key, T, Compare>() {}
  map(const map &other) : RBTree<Key, T, Compare>(other) {}
  /**
   * TODO assignment operator
   */
  auto operator=(const map &other) -> map & {
    RBTree<Key, T, Compare>::operator=(other);
    return *this;
  }
  /**
   * TODO Destructors
   */
  ~map() = default;
  /**
   * TODO
   * access specified element with bounds checking
   * Returns a reference to the mapped value of the element with key equivalent to key.
   * If no such element exists, an exception of type `index_out_of_bound'
   */
  auto at(const Key &key) -> T & {  // NOLINT
    Tnode *res = RBTree<Key, T, Compare>::find(key);
    if (res == nullptr) {
      throw bustub::Exception(bustub::ExceptionType::OUT_OF_RANGE, "index out of range");
    }
    return RBTree<Key, T, Compare>::find(key)->data_.second;
  }
  auto at(const Key &key) const -> const T & {  // NOLINT
    Tnode *res = RBTree<Key, T, Compare>::find(key);
    if (res == nullptr) {
      throw bustub::Exception(bustub::ExceptionType::OUT_OF_RANGE, "index out of range");
    }
    return RBTree<Key, T, Compare>::find(key)->data_.second;
  }
  /**
   * TODO
   * access specified element
   * Returns a reference to the value that is mapped to a key equivalent to key,
   *   performing an insertion if such key does not already exist.
   */
  auto operator[](const Key &key) -> T & { return (RBTree<Key, T, Compare>::insert({key, T()}).first->data_).second; }
  /**
   * behave like at() throw index_out_of_bound if such key does not exist.
   */
  auto operator[](const Key &key) const -> const T & { return at(key); }
  /**
   * return a iterator to the beginning
   */
  auto begin() -> iterator { return iterator(this, RBTree<Key, T, Compare>::first()); }                     // NOLINT
  auto cbegin() const -> const_iterator { return const_iterator(this, RBTree<Key, T, Compare>::first()); }  // NOLINT
  /**
   * return a iterator to the end
   * in fact, it returns past-the-end.
   */
  auto end() -> iterator { return iterator{this, nullptr}; }                     // NOLINT
  auto cend() const -> const_iterator { return const_iterator(this, nullptr); }  // NOLINT

 public:
  /**
   * insert an element.
   * return a pair, the first of the pair is
   *   the iterator to the new element (or the element that prevented the insertion),
   *   the second one is true if insert successfully, or false.
   */
  auto insert(const value_type &value) -> std::pair<iterator, bool> {  // NOLINT
    auto res = RBTree<Key, T, Compare>::insert(value);
    return {iterator(this, res.first), res.second};
  }
  /**
   * erase the element at pos.
   *
   * throw if pos pointed to a bad element (pos == this->end() || pos points an element out of this)
   */
  void erase(iterator pos) {  // NOLINT
    if (pos.iter_ != this || pos.ptr_ == nullptr) {
      throw bustub::Exception(bustub::ExceptionType::OUT_OF_RANGE, "index out of range");
    }
    RBTree<Key, T, Compare>::erase(pos.ptr_->data_.first);
  }
  void erase(const Key &key) { // NOLINT
    RBTree<Key, T, Compare>::erase(key);
  }

 public:
  /**
   * Finds an element with key equivalent to key.
   * key value of the element to search for.
   * Iterator to an element with key equivalent to key.
   *   If no such element is found, past-the-end (see end()) iterator is returned.
   */
  auto find(const Key &key) -> iterator { return iterator(this, RBTree<Key, T, Compare>::find(key)); }  // NOLINT
  auto find(const Key &key) const -> const_iterator {                                                   // NOLINT
    return const_iterator(this, RBTree<Key, T, Compare>::find(key));
  }

  /**
   * Returns the number of elements with key
   *   that compares equivalent to the specified argument,
   *   which is either 1 or 0
   *     since this container does not allow duplicates.
   * The default method of check the equivalence is !(a < b || b > a)
   */
  auto count(const Key &key) const -> size_t { return find(key) == cend() ? 0 : 1; }  // NOLINT
};

template class map<std::string, int>;

}  // namespace sjtu

#endif