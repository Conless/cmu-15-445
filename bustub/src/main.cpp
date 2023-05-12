#include <algorithm>
#include <cstdio>
#include <fstream>
#include <random>

#include "storage/index/b_plus_tree_index_nts.h"
#include "storage/index/custom_key.h"
// #include "test_util.h"  // NOLINT

using namespace bustub;  // NOLINT

void read(char *str) {  // NOLINT
  do {
    *str = getchar();
  } while (*str > 126 || *str < 33);
  do {
    *++str = getchar();
  } while (*str > 32 && *str < 127);
  *str = '\0';
}

void read(int &x) {  // NOLINT
  x = 0;
  char c = getchar();
  while (c < '0' || c > '9') {
    c = getchar();
  }
  while (c >= '0' && c <= '9') {
    x = (x << 3) + (x << 1) + (c ^ 48);
    c = getchar();
  }
}

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
  BPlusTreeIndex<StringIntKey<65>, int, StringIntComparator<65>, false> tree("haha", comp);
  StringIntKey<65> key_value;
  int t;
  read(t);
  while (t-- != 0) {
    char opt[10];
    char key[65];
    int value;
    read(opt);
    if (opt[0] == 'i') {
      read(key);
      read(value);
      key_value = {key, value};
      tree.Insert(key_value, value);
    } else if (opt[0] == 'f') {
      read(key);
      key_value = {key, 0};
      vector<int> res;
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
    } else if (opt[0] == 'd') {
      read(key);
      read(value);
      key_value = {key, value};
      tree.Delete(key_value);
    }
    // out << t << '\n';
    // out << tree.container_->DrawBPlusTree();
  }
  return 0;
}
