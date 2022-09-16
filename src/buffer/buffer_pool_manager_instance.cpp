//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

auto BufferPoolManagerInstance::FindReplacePolicy(frame_id_t *frame_id) -> bool{
  // not empty , return
  //2 call Victim
  //3 if the page pointed by the frame_uid is dirty , then should write back to disk
  //4 delete the item in page_table
  
  // if free_list not empty then we don't need replace page
  // return directly
  if (!free_list_.empty()) {
    *frame_id = free_list_.front(); 
    free_list_.pop_front();
    return true;
  }
  // else we need to find a replace page
  if (replacer_->Victim(frame_id)) {
    int replace_frame_id = -1;
    for (auto &p: page_table_) {  // circle throuth to find page_id
      page_id_t pid = p.first;
      frame_id_t fid = p.second;
      if (fid == *frame_id){
        replace_frame_id = pid;
        break;
      }
    }
    if(replace_frame_id != -1) {  //success to find pid
      Page *replace_page = &pages_[*frame_id];
      if (replace_page->IsDirty()) {  // then write
        char *data = replace_page->GetData();
        disk_manager_->WritePage(replace_page->GetPageId(), data);
        replace_page->pin_count_ = 0;
      }
      page_table_.erase(replace_page->GetPageId());
      return true;
    }
  }

  return false;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::lock_guard<std::mutex> guard(latch_);
  auto iter = page_table_.find(page_id);
  if(iter == page_table_.end() || page_id == INVALID_PAGE_ID) {
    return false;
  }
  frame_id_t flush_fid = iter->second;
  disk_manager_->WritePage(page_id, pages_[flush_fid].data_);
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  for(int i = 0; i < static_cast<int>(pool_size_); i++){
    Page * page = &pages_[i];
    FlushPgImp(page->GetPageId());
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::lock_guard<std::mutex> guard(latch_);
  page_id_t new_page_id = AllocatePage();
   // 1.
  bool is_all = true;
  for (int i = 0; i < static_cast<int>(pool_size_); i++) {
    if (pages_[i].pin_count_ == 0) {
      is_all = false;
      break;
    }
  }
  if (is_all) {
    return nullptr;
  }
  // 2.
  frame_id_t victim_fid;
  if (!FindReplacePolicy(&victim_fid)) {
    return nullptr;
  }
  // 3.
  Page *victim_page = &pages_[victim_fid];
  victim_page->page_id_ = new_page_id;
  victim_page->pin_count_++;
  replacer_->Pin(victim_fid);
  page_table_[new_page_id] = victim_fid;
  victim_page->is_dirty_ = false;
  *page_id = new_page_id;

  disk_manager_->WritePage(victim_page->GetPageId(), victim_page->GetData());
  return victim_page;
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  latch_.lock();
  std::unordered_map<page_id_t, frame_id_t>::iterator it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    frame_id_t frame_id = it->second;
    Page *page = &pages_[frame_id];  // from frame_id get page
     
     // pin page
     page->pin_count_++;
     replacer_->Pin(frame_id); // into lru

     latch_.unlock();
     return page;
  }
  int replace_frame_id;
  if (!FindReplacePolicy(&replace_frame_id)) {
    latch_.unlock();
    return nullptr;
  }
  Page *replacePage = &pages_[replace_frame_id];
  if (replacePage->IsDirty()) {
    disk_manager_->WritePage(replacePage->GetPageId(), replacePage->GetData());
  }
  page_table_.erase(replacePage->page_id_);

  page_table_[page_id] = replace_frame_id;
  
  Page *newPage = replacePage;  // already allocated in Iint, just do some meta_data change
  disk_manager_->ReadPage(page_id, newPage->data_);
  newPage->page_id_ = page_id;
  newPage->pin_count_ ++;  
  newPage->is_dirty_ = false;
  replacer_->Pin(replace_frame_id);
  latch_.unlock();

  return newPage;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::lock_guard<std::mutex> guard(latch_);
  if(page_table_.find(page_id) == page_table_.end()) {
    return true;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];
  if(page->pin_count_ > 0) {
    return false;
  }
  if(page -> is_dirty_) {
    FlushPgImp(page_id);
  }
  // delete in disk in here
  DeallocatePage(page_id);
  
  page_table_.erase(page_id);
  // reset metadata
  page->is_dirty_ = false;
  page->pin_count_ = 0;
  page->page_id_ = INVALID_PAGE_ID;
  // return it to the free list
  
  free_list_.push_back(frame_id);
  return true;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> guard(latch_);
  auto iter = page_table_.find(page_id);
  if(iter == page_table_.end()) {
    return false;
  }
  frame_id_t unpinned_fid = iter->second;
  Page *unpinned_page = &pages_[unpinned_fid];
  if (is_dirty) {
    unpinned_page->is_dirty_ = true;
  }
  if(unpinned_page->pin_count_ == 0) {
    replacer_->Unpin(unpinned_fid);
    return false;
  }
  unpinned_page->pin_count_--;
  if(unpinned_page->pin_count_ == 0) {
    replacer_->Unpin(unpinned_fid);
  }
  return true;

}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
