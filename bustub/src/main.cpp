#include "buffer/buffer_pool_manager.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h"  // NOLINT

using namespace bustub; // NOLINT

auto main() -> int {
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  // create b+ tree
  int leaf_max_size = 0;
  int internal_max_size = 0;
  std::cout << "Type in the maximum leaf page size: ";
  std::cin >> leaf_max_size;
  std::cout << "Type in the maximum internal page size: ";
  std::cin >> internal_max_size;
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", header_page->GetPageId(), bpm, comparator, leaf_max_size, internal_max_size);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  while (true) {
    std::string opt;
    int num;
    std::cin >> opt;
    if (opt == "insert") {
      std::cin >> num;
      index_key.SetFromInteger(num);
      rid.Set(0, num);
      tree.Insert(index_key, rid, transaction);
    } else if (opt == "delete") {
      std::cin >> num;
      index_key.SetFromInteger(num);
      tree.Remove(index_key, transaction);
    } else if (opt == "end") {
      break;
    }
    std::cout << tree.DrawBPlusTree() << std::endl;
  }

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete bpm;
}