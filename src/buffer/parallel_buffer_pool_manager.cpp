//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager): num_instances_(num_instances), pool_size_(pool_size){
  // Allocate and create individual BufferPoolManagerInstances
  for(int i = 0; i < static_cast<int>(num_instances); i++){
    buffer_vec_.push_back(new BufferPoolManagerInstance(pool_size, 
    num_instances, static_cast<uint32_t>(i), disk_manager, log_manager));
  }
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() = default;

size_t ParallelBufferPoolManager::GetPoolSize() {
  // Get size of all BufferPoolManagerInstances
  return num_instances_;
}

BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  u_int32_t buffer_id = page_id %  num_instances_;
  return buffer_vec_[buffer_id];
}

Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  BufferPoolManagerInstance *buffer_pool_manager =
             dynamic_cast<BufferPoolManagerInstance *>(GetBufferPoolManager(page_id));
  if (buffer_pool_manager == nullptr) {
    return nullptr;
  }
  return buffer_pool_manager->FetchPage(page_id);

}

bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  // Unpin page_id from responsible BufferPoolManagerInstance
  BufferPoolManagerInstance *buffer_pool_manager =
             dynamic_cast<BufferPoolManagerInstance *>(GetBufferPoolManager(page_id));
  if (buffer_pool_manager == nullptr) {
    return false;
  }
  return buffer_pool_manager->UnpinPage(page_id, is_dirty);
}

bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {
  // Flush page_id from responsible BufferPoolManagerInstance
  BufferPoolManagerInstance *buffer_pool_manager =
             dynamic_cast<BufferPoolManagerInstance *>(GetBufferPoolManager(page_id));
  if (buffer_pool_manager == nullptr) {
    return false;
  }
  return buffer_pool_manager->FlushPage(page_id);
}


Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
  std::lock_guard<std::mutex> guard(latch_);
  Page *page = nullptr;
  for(size_t i = 0; i < num_instances_; i++) {
      size_t index = start_index_ % num_instances_;
      page = buffer_vec_[index]->NewPage(page_id);
      start_index_ = (start_index_ + 1) % num_instances_;
      if(page != nullptr) {
        LOG_DEBUG("Buffer ID = %zu, page id = %d", index, *page_id);
        break;
      }
  }
  if (page == nullptr) {
    LOG_DEBUG("No new page created");
    return nullptr;
  }
  return page;
}

bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
  // Delete page_id from responsible BufferPoolManagerInstance
  BufferPoolManagerInstance *buffer_pool_manager =
             dynamic_cast<BufferPoolManagerInstance *>(GetBufferPoolManager(page_id));
  if (buffer_pool_manager == nullptr) {
    return true;  // 易错点：page_id不存在 == 已经被 delete了
  }
  return buffer_pool_manager->DeletePage(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  for(BufferPoolManagerInstance* BPI: buffer_vec_) {
    BPI->FlushAllPages();
  }
}

}  // namespace bustub
