#include <algorithm>
#include <random>
#include "buffer/buffer_pool_manager.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/index/b_plus_tree_nts.h"
#include "test_util.h"  // NOLINT

using namespace bustub;  // NOLINT

auto main() -> int {
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get(), 10, nullptr, false);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  // create b+ tree
//   int leaf_max_size = 3;
//   int internal_max_size = 2;
//   std::cout << "Type in the maximum leaf page size: ";
//   std::cin >> leaf_max_size;
//   std::cout << "Type in the maximum internal page size: ";
//   std::cin >> internal_max_size;
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>, false> tree("foo_pk", header_page->GetPageId(), bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  std::vector<int> vec;
  int opt = 1;
  int n = 1e5;
  for (int i = 1; i <= n; i++) {
    vec.push_back(i);
  }
//   std::shuffle(vec.begin(), vec.end(), std::mt19937(std::random_device()()));
  for (auto key : vec) {
    if (opt == 2) {
      std::cin >> key;
    } else {
    //   std::cout << key << " ";
    }
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }
  std::cout << "Finish insert\n";
//   std::shuffle(vec.begin(), vec.end(), std::mt19937(std::random_device()()));
//   for (auto key : vec) {
//     if (opt == 2) {
//       std::cin >> key;
//     } else {
//     //   std::cout << key << " ";
//     }
//     index_key.SetFromInteger(key);
//     tree.Remove(index_key, transaction);
//   }
//   std::cout << "Finish remove\n";
//   std::shuffle(vec.begin(), vec.end(), std::mt19937(std::random_device()()));
//   for (auto key : vec) {
//     if (opt == 2) {
//       std::cin >> key;
//     } else {
//     //   std::cout << key << " ";
//     }
//     index_key.SetFromInteger(key);
//     tree.Insert(index_key, rid, transaction);
//   }
//   std::cout << "Finish insert\n";
//   std::shuffle(vec.begin(), vec.end(), std::mt19937(std::random_device()()));
//   for (auto key : vec) {
//     if (opt == 2) {
//       std::cin >> key;
//     } else {
//     //   std::cout << key << " ";
//     }
//     index_key.SetFromInteger(key);
//     tree.Remove(index_key, transaction);
//   }
//   std::cout << "Finish remove\n";
  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete bpm;
}