#pragma once

#include <array>
#include <cstring>
#include <fstream>
#include <future>  // NOLINT
#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <string>
#include <thread>  // NOLINT
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

/**
 * DiskManagerMemory replicates the utility of DiskManager on memory. It is primarily used for
 * data structure performance testing.
 */
class DiskManagerNTS : public DiskManager {
 public:
  explicit DiskManagerNTS(const std::string &db_file) : DiskManager(db_file) {}

  /**
   * Write a page to the database file.
   * @param page_id id of the page
   * @param page_data raw page data
   */
  void WritePage(page_id_t page_id, const char *page_data) override {
    size_t offset = static_cast<size_t>(page_id) * BUSTUB_PAGE_SIZE;
    // set write cursor to offset
    num_writes_ += 1;
    db_io_.seekp(offset);
    db_io_.write(page_data, BUSTUB_PAGE_SIZE);
    // check for I/O error
    if (db_io_.bad()) {
      LOG_DEBUG("I/O error while writing");
      return;
    }
    // needs to flush to keep disk file in sync
    db_io_.flush();
  }

  /**
   * Read a page from the database file.
   * @param page_id id of the page
   * @param[out] page_data output buffer
   */
  void ReadPage(page_id_t page_id, char *page_data) override {
    int offset = page_id * BUSTUB_PAGE_SIZE;
    // check if read beyond file length
    if (offset > GetFileSize(file_name_)) {
      LOG_DEBUG("I/O error reading past end of file");
      // std::cerr << "I/O error while reading" << std::endl;
    } else {
      // set read cursor to offset
      db_io_.seekp(offset);
      db_io_.read(page_data, BUSTUB_PAGE_SIZE);
      if (db_io_.bad()) {
        LOG_DEBUG("I/O error while reading");
        return;
      }
      // if file ends before reading BUSTUB_PAGE_SIZE
      int read_count = db_io_.gcount();
      if (read_count < BUSTUB_PAGE_SIZE) {
        LOG_DEBUG("Read less than a page");
        db_io_.clear();
        // std::cerr << "Read less than a page" << std::endl;
        memset(page_data + read_count, 0, BUSTUB_PAGE_SIZE - read_count);
      }
    }
  }
};

}  // namespace bustub
