/**
 * @file b_plus_tree_map.h
 * @author Conless Pan (conlesspan@outlook.com)
 * @brief The header file of b-plus tree based map
 * @version 0.1
 * @date 2023-04-13
 *
 * @copyright Copyright (c) 2023
 *
 */
#pragma once

#include "storage/index/b_plus_tree.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator, bool isThreadSafe = false>
class Map {};

INDEX_TEMPLATE_ARGUMENTS class Map<KeyType, ValueType, KeyComparator, true> {
 public:
  explicit Map(const std::string &index_name);

 private:
  char index_name_[32];
  DiskManager *disk_manager_;
  int buffer_pool_size_;

  std::shared_ptr<BPlusTree<KeyType, ValueType, KeyComparator, true>> container_;
};

}  // namespace bustub