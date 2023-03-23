//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.resize(num_buckets_);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  int idx = IndexOf(key);

  std::scoped_lock<std::mutex> lock(latch_);

  auto &bucket = dir_[idx];

  return bucket != nullptr && bucket->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  int idx = IndexOf(key);

  if (dir_[idx] == nullptr) return false;

  std::shared_ptr<Bucket> &bucket = dir_[idx];

  return bucket->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  _Insert(key, value);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::_Insert(const K &key, const V &value) {
  int idx = IndexOf(key);

  if (dir_[idx] == nullptr) dir_[idx] = std::make_shared<Bucket>(Bucket(bucket_size_));

  auto bucket = dir_[idx];
  int localDepth = bucket->GetDepth();

  if (bucket->Insert(key, value)) return;

  if (global_depth_ == localDepth) {
    int oldSize = num_buckets_;
    num_buckets_ *= 2;
    dir_.resize(num_buckets_);

    for (int i = 0; i < oldSize; i++) dir_[i | (1 << global_depth_)] = dir_[i];
    global_depth_++;
  }

  std::shared_ptr<Bucket> b1(new Bucket(bucket_size_, localDepth + 1));  // ...0idx
  std::shared_ptr<Bucket> b2(new Bucket(bucket_size_, localDepth + 1));  // ...1idx

  for (auto &item : bucket->GetItems()) {
    K &oldKey = item.first;
    int oldIdx = IndexOf(oldKey);

    if (oldIdx & (1 << localDepth))
      b2->Insert(item.first, item.second);
    else
      b1->Insert(item.first, item.second);
  }

  int size = (int)dir_.size();
  for (int i = idx; i < size; i += (1 << localDepth)) {
    dir_[i] = (i & (1 << localDepth)) ? b2 : b1;
  }

  _Insert(key, value);
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (std::pair<K, V> &item : list_)
    if (item.first == key) {
      value = item.second;
      return true;
    }

  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (std::pair<K, V> &item : list_) {
    if (item.first == key) {
      list_.remove(item);
      return true;
    }
  }

  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  for (std::pair<K, V> &item : list_)
    if (item.first == key) {
      item.second = value;
      return true;
    }

  if (IsFull()) return false;

  list_.push_back(std::make_pair(key, value));
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
