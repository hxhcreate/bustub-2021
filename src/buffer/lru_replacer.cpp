//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
    capacity = num_pages;
}

LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool { 
    latch_.lock();
    if (lru.empty()) {
        latch_.unlock();
        return false;
    } 

    *frame_id = lru.back();  // delete from back
    lru.pop_back();
    hashMap.erase(*frame_id);
    latch_.unlock();
    return true;
}
// pined frame are using , and must delete from lrulist and map
void LRUReplacer::Pin(frame_id_t frame_id) {
    latch_.lock();
    auto iter = hashMap.find(frame_id);
    if (iter != hashMap.end()) {  // check if exists
        lru.erase(hashMap[frame_id]);
        hashMap.erase(frame_id);
    }
    latch_.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    latch_.lock();
    auto iter = hashMap.find(frame_id);
    if (iter == hashMap.end()) {
        while(lru.size() >= capacity - 1){
            frame_id_t lru_element;
            Victim(&lru_element);
        } 
        lru.push_front(frame_id);  //insert from front
        hashMap.insert({frame_id, lru.begin()});
    }
    latch_.unlock();
}

auto LRUReplacer::Size() -> size_t { return lru.size(); }

}  // namespace bustub
