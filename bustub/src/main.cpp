#include <algorithm>
#include <random>
#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "storage/disk/disk_manager_nts.h"
#include "storage/index/b_plus_tree_nts.h"
#include "storage/index/custom_key.h"
// #include "test_util.h"  // NOLINT

using namespace bustub;  // NOLINT

auto main() -> int {
  auto disk_manager = std::make_unique<DiskManagerNTS>("haha.db");
  auto *bpm = new BufferPoolManager(50, disk_manager.get(), 10, nullptr, false);
  // create and fetch header_page
  Page *header_page;
  int header_page_id;
//   header_page = bpm->NewPage(&header_page_id);
  header_page = bpm->FetchPage(HEADER_PAGE_ID);
  StringIntComparator<65> comp(ComparatorType::CompareData);
  StringIntComparator<65> comp_key(ComparatorType::CompareKey);
  BPlusTree<StringIntKey<65>, int, StringIntComparator<65>, false> tree("haha", header_page->GetPageId(), bpm, comp, 3, 3);
  // create transaction
  auto *transaction = new Transaction(0);
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
      tree.GetValue(key_value, &res, comp_key);
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
      tree.Remove(key_value);
    }
    std::cout << tree.DrawBPlusTree();
  }
  bpm->UnpinPage(HEADER_PAGE_ID, true);
  bpm->FlushAllPages();
  delete transaction;
  delete bpm;
}