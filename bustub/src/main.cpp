#include <algorithm>
#include <random>

#include "storage/index/b_plus_tree_index_nts.h"
#include "storage/index/custom_key.h"
// #include "test_util.h"  // NOLINT

using namespace bustub;  // NOLINT

auto main() -> int {
  StringIntComparator<65> comp(ComparatorType::CompareData);
  StringIntComparator<65> comp_key(ComparatorType::CompareKey);
  BPlusTreeIndex<StringIntKey<65>, int, StringIntComparator<65>, false> tree("haha", comp, 3, 3);
  StringIntKey<65> key_value;
  int t;
  std::cin >> t;
  while (t-- != 0) {
    std::string opt;
    std::string key;
    int value;
    std::cin >> opt;
    if (opt == "insert") {
      std::cin >> key >> value;
      key_value = {key, value};
      tree.Insert(key_value, value);
    } else if (opt == "find") {
      std::cin >> key;
      key_value = {key, 0};
      std::vector<int> res;
      tree.Search(key_value, &res, comp_key);
      if (res.empty()) {
        std::cout << "null";
      } else {
        for (auto num : res) {
          std::cout << num << ' ';
        }
      }
      std::cout << '\n';
    } else if (opt == "delete") {
      std::cin >> key >> value;
      key_value = {key, value};
      tree.Delete(key_value);
    }
    std::cout << tree.container_->DrawBPlusTree();
  }
  return 0;
}