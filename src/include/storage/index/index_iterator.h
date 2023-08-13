//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include <iterator>
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  using iterator_category = std::input_iterator_tag;
  using value_type = MappingType;

  // you may define your own constructor based on your member variables
  IndexIterator(page_id_t leaf_page_id, int page_index, BufferPoolManager *bpm);
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    return cur_page_id_ == itr.cur_page_id_ && page_index_ == itr.page_index_;
  }

  auto operator!=(const IndexIterator &itr) const -> bool {
    return !(cur_page_id_ == itr.cur_page_id_ && page_index_ == itr.page_index_);
  }

 private:
  // add your own private member variables here
  int page_index_;
  page_id_t cur_page_id_;
  LeafPage *cur_page_;
  BufferPoolManager *buffer_pool_manager_;
};

}  // namespace bustub
