//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  evictableFrames = std::make_unique<std::set<frame_id_t, CMP>>(std::set<frame_id_t, CMP>(CMP(frames)));
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  if (evictableFrames->empty()) return false;

  *frame_id = *evictableFrames->begin();

  evictableFrames->erase(*frame_id);
  frames.erase(*frame_id);
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  if (!frames.count(frame_id)) frames.insert(std::make_pair(frame_id, std::list<long>(k_, INF)));
  size_t haveEvicted = evictableFrames->count(frame_id);

  if (haveEvicted) {
    evictableFrames->erase(frame_id);
  }

  frames[frame_id].pop_back();

  frames[frame_id].push_front(now());

  if (haveEvicted) {
    evictableFrames->insert(frame_id);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  if (set_evictable)
    evictableFrames->insert(frame_id);
  else if (evictableFrames->count(frame_id))
    evictableFrames->erase(frame_id);
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  if (frames.count(frame_id)) frames.erase(frame_id);
  if (evictableFrames->count(frame_id)) evictableFrames->erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t { return evictableFrames->size(); }

}  // namespace bustub
