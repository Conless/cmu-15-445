cmake_minimum_required(VERSION 3.16)

set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS True)

project(Bustub)

set(CMAKE_CXX_FLAGS "-pthread -O3")

set(SOURCE_CPPS
    src/storage/disk/disk_manager.cpp

    src/buffer/lru_k_replacer.cpp
    src/buffer/buffer_pool_manager.cpp

    src/storage/page/page_guard.cpp
    src/storage/page/b_plus_tree_page.cpp
    src/storage/page/b_plus_tree_internal_page.cpp
    src/storage/page/b_plus_tree_leaf_page.cpp
    src/storage/index/index_iterator.cpp
    src/storage/index/b_plus_tree_nts.cpp
    src/storage/index/b_plus_tree.cpp
)

set(BUSTUB_SRC_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/src/include)
set(BUSTUB_TEST_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/test/include)
set(BUSTUB_THIRD_PARTY_INCLUDE_DIR
        ${PROJECT_SOURCE_DIR}/third_party
        ${PROJECT_SOURCE_DIR}/third_party/fmt/include
        ${PROJECT_SOURCE_DIR}/third_party/libpg_query/include
        ${PROJECT_SOURCE_DIR}/third_party/argparse/include
        ${PROJECT_SOURCE_DIR}/third_party/cpp_random_distributions
)

include_directories(${BUSTUB_SRC_INCLUDE_DIR} ${BUSTUB_TEST_INCLUDE_DIR} ${BUSTUB_THIRD_PARTY_INCLUDE_DIR})

add_executable(code src/main.cpp ${SOURCE_CPPS})
