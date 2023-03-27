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

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t slot = -1;
  *page_id = INVALID_PAGE_ID;
  if (free_list_.empty() && !replacer_->Evict(&slot)) return nullptr;

  if (!free_list_.empty()) {
    slot = free_list_.front();
    free_list_.pop_front();
  }

  if (pages_[slot].IsDirty()) {
    latch_.unlock();
    FlushPgImp(pages_[slot].GetPageId());
    latch_.lock();
  }

  if (pages_[slot].GetPageId() != INVALID_PAGE_ID) page_table_->Remove(pages_[slot].GetPageId());

  *page_id = AllocatePage();
  page_table_->Insert(*page_id, slot);
  replacer_->RecordAccess(slot);
  replacer_->SetEvictable(slot, false);

  pages_[slot].ResetMemory();
  pages_[slot].pin_count_ = 1;
  pages_[slot].page_id_ = *page_id;

  return &pages_[slot];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t slot = -1;

  if (!page_table_->Find(page_id, slot) && free_list_.empty() && !replacer_->Evict(&slot)) return nullptr;

  if (slot == -1) {
    slot = free_list_.front();
    free_list_.pop_front();
  }

  if (!page_table_->Find(page_id, slot)) {
    if (pages_[slot].IsDirty()) {
      latch_.unlock();
      FlushPgImp(pages_[slot].GetPageId());
      latch_.lock();
    }

    if (pages_[slot].GetPageId() != INVALID_PAGE_ID) page_table_->Remove(pages_[slot].GetPageId());

    page_table_->Insert(page_id, slot);

    pages_[slot].ResetMemory();
    pages_[slot].pin_count_ = 0;
    pages_[slot].page_id_ = page_id;
  }

  replacer_->RecordAccess(slot);
  replacer_->SetEvictable(slot, false);

  pages_[slot].pin_count_++;

  disk_manager_->ReadPage(page_id, pages_[slot].GetData());

  return &pages_[slot];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t slot;
  if (!page_table_->Find(page_id, slot) || pages_[slot].pin_count_ == 0) return false;

  pages_[slot].pin_count_--;

  if (pages_[slot].pin_count_ == 0) replacer_->SetEvictable(slot, true);
  pages_[slot].is_dirty_ = is_dirty;

  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t slot;

  if (!page_table_->Find(page_id, slot)) return false;

  disk_manager_->WritePage(page_id, pages_[slot].GetData());

  pages_[slot].is_dirty_ = false;

  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  for (size_t i = 0; i < pool_size_; ++i)
    if (pages_->GetPageId() != INVALID_PAGE_ID) FlushPgImp(pages_[i].GetPageId());
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t slot;
  if (!page_table_->Find(page_id, slot)) return true;

  if (pages_[slot].pin_count_ > 0) return false;

  page_table_->Remove(page_id);
  replacer_->Remove(slot);

  pages_[slot].ResetMemory();
  pages_[slot].page_id_ = INVALID_PAGE_ID;
  free_list_.emplace_back(slot);
  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
