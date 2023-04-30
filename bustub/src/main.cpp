#include <algorithm>
#include <cstdio>
#include <fstream>
#include <random>

#include "storage/index/b_plus_tree_index_nts.h"
#include "storage/index/custom_key.h"
// #include "test_util.h"  // NOLINT

using namespace bustub;  // NOLINT

void write(int num) {  // NOLINT
  if (num > 9) {
    write(num / 10);
  }
  putchar(num % 10 + '0');
}

auto main() -> int {
  // std::ofstream out("test.log");
  std::ios::sync_with_stdio(false);
  StringIntComparator<65> comp(ComparatorType::CompareData);
  StringIntComparator<65> comp_key(ComparatorType::CompareKey);
  BPlusTreeIndex<StringIntKey<65>, int, StringIntComparator<65>, false> tree("haha", comp, 150, 150, 100, 10);
  StringIntKey<65> key_value;
  int t;
  std::cin >> t;
  while (t-- != 0) {
    std::string opt;
    std::string key;
    int value;
    std::cin >> opt;
    if (opt[0] == 'i') {
      std::cin >> key >> value;
      key_value = {key, value};
      tree.Insert(key_value, value);
    } else if (opt[0] == 'f') {
      std::cin >> key;
      key_value = {key, 0};
      std::vector<int> res;
      tree.Search(key_value, &res, comp_key);
      if (res.empty()) {
        puts("null");
      } else {
        for (auto num : res) {
          write(num);
          putchar(' ');
        }
        putchar('\n');
      }
    } else if (opt == "delete") {
      std::cin >> key >> value;
      key_value = {key, value};
      tree.Delete(key_value);
    }
    // out << t << '\n';
    // out << tree.container_->DrawBPlusTree();
  }
  return 0;
}
