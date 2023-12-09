/**
 * index_iterator.cpp
 */
#include <cassert>
#include <utility>

#include "common/config.h"
#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t leaf_page_id, int page_index, BufferPoolManager *bpm)
    : page_index_(page_index), cur_page_id_(leaf_page_id), buffer_pool_manager_(bpm) {
  if (leaf_page_id != INVALID_PAGE_ID) {
    cur_page_ = reinterpret_cast<LeafPage *>(this->buffer_pool_manager_->FetchPage(leaf_page_id)->GetData());
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() { this->buffer_pool_manager_->UnpinPage(cur_page_id_, true); };  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  if (cur_page_id_ == INVALID_PAGE_ID) {
    return true;
  }
  return page_index_ == cur_page_->GetSize() && cur_page_->GetNextPageId() == -1;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return cur_page_->MappingAt(page_index_); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  page_index_++;
  if (page_index_ == cur_page_->GetSize()) {
    auto next_page_id = cur_page_->GetNextPageId();

    if (next_page_id != -1) {
      buffer_pool_manager_->UnpinPage(cur_page_id_, false);

      cur_page_id_ = next_page_id;
      cur_page_ = reinterpret_cast<LeafPage *>(this->buffer_pool_manager_->FetchPage(cur_page_id_)->GetData());

      page_index_ = 0;
    }
  }

  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
