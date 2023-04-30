cmake_minimum_required(VERSION 3.16)

set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS True)

project(Bustub)

set(CMAKE_CXX_FLAGS "-pthread -Ofast")

set(SOURCE_CPPS
    bustub/src/storage/disk/disk_manager.cpp
    bustub/src/buffer/lru_k_replacer.cpp
    bustub/src/buffer/buffer_pool_manager.cpp
    bustub/src/storage/page/page_guard.cpp
    bustub/src/storage/page/b_plus_tree_page.cpp
    bustub/src/storage/page/b_plus_tree_internal_page.cpp
    bustub/src/storage/page/b_plus_tree_leaf_page.cpp
    bustub/src/storage/index/index_iterator.cpp
    bustub/src/storage/index/b_plus_tree_nts.cpp
    bustub/src/storage/index/b_plus_tree.cpp
    bustub/src/storage/index/b_plus_tree_index_nts.cpp
    bustub/src/storage/index/b_plus_tree_index.cpp
)

set(BUSTUB_SRC_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/bustub/src/include)
set(BUSTUB_TEST_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/bustub/test/include)
set(BUSTUB_THIRD_PARTY_INCLUDE_DIR
        ${PROJECT_SOURCE_DIR}/bustub/third_party
        ${PROJECT_SOURCE_DIR}/bustub/third_party/fmt/include
        ${PROJECT_SOURCE_DIR}/bustub/third_party/libpg_query/include
        ${PROJECT_SOURCE_DIR}/bustub/third_party/argparse/include
        ${PROJECT_SOURCE_DIR}/bustub/third_party/cpp_random_distributions
)

include_directories(${BUSTUB_SRC_INCLUDE_DIR} ${BUSTUB_TEST_INCLUDE_DIR} ${BUSTUB_THIRD_PARTY_INCLUDE_DIR})

add_executable(code bustub/src/main.cpp ${SOURCE_CPPS})
