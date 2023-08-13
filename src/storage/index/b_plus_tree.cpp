#include <cstdlib>
#include <stack>
#include <string>
#include <thread>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return this->root_page_id_ == INVALID_PAGE_ID; }

/*
 * Helper function for finding leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeaf(const KeyType &key) -> LeafPage * {
  Page *root_page = nullptr;
  BPlusTreePage *cur_node = nullptr;
  LeafPage *leaf = nullptr;

  root_page = this->buffer_pool_manager_->FetchPage(this->root_page_id_);
  cur_node = reinterpret_cast<BPlusTreePage *>(root_page->GetData());

  while (!cur_node->IsLeafPage()) {
    page_id_t tmp_page_id = reinterpret_cast<InternalPage *>(cur_node)->Search(key, this->comparator_);
    this->buffer_pool_manager_->UnpinPage(cur_node->GetPageId(), false);

    cur_node = reinterpret_cast<BPlusTreePage *>(this->buffer_pool_manager_->FetchPage(tmp_page_id)->GetData());
  }

  leaf = reinterpret_cast<LeafPage *>(cur_node);

  return leaf;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  bool exists;
  ValueType value;

  Page *cur_page = buffer_pool_manager_->FetchPage(root_page_id_);
  cur_page->RLatch();

  BPlusTreePage *cur_node = nullptr;
  LeafPage *leaf = nullptr;

  while (true) {
    cur_node = reinterpret_cast<BPlusTreePage *>(cur_page->GetData());

    if (cur_node->IsLeafPage()) {
      break;
    }

    auto next_page_id = reinterpret_cast<InternalPage *>(cur_node)->Search(key, this->comparator_);
    auto *tmp_page = buffer_pool_manager_->FetchPage(next_page_id);
    tmp_page->RLatch();

    cur_page->RUnlatch();
    this->buffer_pool_manager_->UnpinPage(cur_node->GetPageId(), false);

    cur_page = tmp_page;
  }

  leaf = reinterpret_cast<LeafPage *>(cur_node);

  exists = leaf->Search(key, &value, this->comparator_);

  if (exists) {
    result->push_back(value);
  }

  cur_page->RUnlatch();
  this->buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);

  return exists;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  LeafPage *leaf = nullptr;
  std::stack<Page *> page_with_locks;

  insert_latch_.lock();

  if (this->IsEmpty()) {
    page_id_t new_root_page_id;
    Page *new_root_page = this->buffer_pool_manager_->NewPage(&new_root_page_id);
    new_root_page->WLatch();
    page_with_locks.push(new_root_page);

    leaf = reinterpret_cast<LeafPage *>(new_root_page->GetData());

    leaf->Init(new_root_page_id, new_root_page_id, this->leaf_max_size_);
    this->root_page_id_ = new_root_page_id;
    this->UpdateRootPageId(1);

    insert_latch_.unlock();
  } else {
    insert_latch_.unlock();

    Page *cur_page = nullptr;

    do {
      if (cur_page != nullptr) {
        cur_page->WUnlatch();
      }
      cur_page = buffer_pool_manager_->FetchPage(root_page_id_);
      cur_page->WLatch();
    } while (cur_page->GetPageId() != root_page_id_);

    page_with_locks.push(cur_page);

    BPlusTreePage *cur_node = nullptr;

    while (true) {
      cur_node = reinterpret_cast<BPlusTreePage *>(cur_page->GetData());

      if (cur_node->IsLeafPage()) {
        break;
      }

      auto next_page_id = reinterpret_cast<InternalPage *>(cur_node)->Search(key, this->comparator_);
      auto *tmp_page = buffer_pool_manager_->FetchPage(next_page_id);
      tmp_page->WLatch();
      auto *tmp_node = reinterpret_cast<BPlusTreePage *>(tmp_page->GetData());
      if (tmp_node->GetSize() < tmp_node->GetMaxSize()) {
        while (!page_with_locks.empty()) {
          Page *page_item = page_with_locks.top();
          page_with_locks.pop();

          this->buffer_pool_manager_->UnpinPage(page_item->GetPageId(), false);
          page_item->WUnlatch();
        }
      }
      page_with_locks.push(tmp_page);

      cur_page = tmp_page;
    }

    leaf = reinterpret_cast<LeafPage *>(cur_node);

    // check duplicate key
    ValueType value;
    if (leaf == nullptr || leaf->Search(key, &value, this->comparator_)) {
      while (!page_with_locks.empty()) {
        Page *page_item = page_with_locks.top();
        page_with_locks.pop();

        this->buffer_pool_manager_->UnpinPage(page_item->GetPageId(), false);
        page_item->WUnlatch();
      }
      return false;
    }
  }

  if (leaf->GetSize() < leaf->GetMaxSize()) {
    this->InsertInLeaf(leaf, key, value);

    while (!page_with_locks.empty()) {
      Page *page_item = page_with_locks.top();
      page_with_locks.pop();

      this->buffer_pool_manager_->UnpinPage(page_item->GetPageId(), false);
      page_item->WUnlatch();
    }

    return true;
  }

  // leaf is full
  // create tmp node to hold all key-values
  auto *tmp = reinterpret_cast<LeafPage *>(malloc(BUSTUB_PAGE_SIZE));
  tmp->Init(INVALID_PAGE_ID, INVALID_PAGE_ID, this->leaf_max_size_ + 1);
  for (int i = 0; i < this->leaf_max_size_; i++) {
    tmp->Insert(leaf->KeyAt(i), leaf->ValueAt(i), this->comparator_);
  }
  this->InsertInLeaf(tmp, key, value);

  // create new leaf node
  page_id_t new_leaf_id;
  auto *new_leaf = reinterpret_cast<LeafPage *>(this->buffer_pool_manager_->NewPage(&new_leaf_id)->GetData());
  new_leaf->Init(new_leaf_id, new_leaf_id, this->leaf_max_size_);
  new_leaf->SetNextPageId(leaf->GetNextPageId());
  leaf->SetNextPageId(new_leaf_id);

  // clear leaf
  leaf->SetSize(0);

  for (int i = 0; i < leaf->GetMinSize(); i++) {
    leaf->Insert(tmp->KeyAt(i), tmp->ValueAt(i), this->comparator_);
  }
  for (int i = leaf->GetMinSize(); i < tmp->GetSize(); i++) {
    new_leaf->Insert(tmp->KeyAt(i), tmp->ValueAt(i), this->comparator_);
  }

  free(tmp);

  // update parent
  this->InsertInParent(leaf, new_leaf->KeyAt(0), new_leaf);

  this->buffer_pool_manager_->UnpinPage(new_leaf->GetPageId(), true);

  while (!page_with_locks.empty()) {
    Page *page_item = page_with_locks.top();
    page_with_locks.pop();

    this->buffer_pool_manager_->UnpinPage(page_item->GetPageId(), true);
    page_item->WUnlatch();
  }

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertInLeaf(LeafPage *node, const KeyType &key, const ValueType &value) -> bool {
  node->Insert(key, value, this->comparator_);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertInParent(BPlusTreePage *left_node, const KeyType &key, BPlusTreePage *right_node) -> bool {
  if (left_node->IsRootPage()) {
    page_id_t new_root_page_id;
    Page *new_root_page = this->buffer_pool_manager_->NewPage(&new_root_page_id);
    new_root_page->WLatch();
    auto *new_root = reinterpret_cast<InternalPage *>(new_root_page->GetData());
    new_root->Init(new_root_page_id, new_root_page_id, this->internal_max_size_);

    new_root->SetValueAt(0, left_node->GetPageId());
    new_root->SetKeyAt(1, key);
    new_root->SetValueAt(1, right_node->GetPageId());
    new_root->IncreaseSize(2);

    left_node->SetParentPageId(new_root_page_id);
    right_node->SetParentPageId(new_root_page_id);

    this->root_page_id_ = new_root_page_id;
    this->UpdateRootPageId(0);

    new_root_page->WUnlatch();
    this->buffer_pool_manager_->UnpinPage(new_root_page_id, true);

    return true;
  }

  auto *parent =
      reinterpret_cast<InternalPage *>(this->buffer_pool_manager_->FetchPage(left_node->GetParentPageId())->GetData());

  if (parent->GetSize() < parent->GetMaxSize()) {
    parent->Insert(key, right_node->GetPageId(), this->comparator_);
    right_node->SetParentPageId(parent->GetPageId());

    this->buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    return true;
  }

  // split parent
  auto *tmp = reinterpret_cast<InternalPage *>(malloc(sizeof(char) * BUSTUB_PAGE_SIZE));
  tmp->Init(INVALID_PAGE_ID, INVALID_PAGE_ID, this->internal_max_size_ + 1);
  for (int i = 0; i < this->internal_max_size_; i++) {
    tmp->SetKeyAt(i, parent->KeyAt(i));
    tmp->SetValueAt(i, parent->ValueAt(i));
  }
  tmp->SetSize(this->internal_max_size_);
  tmp->Insert(key, right_node->GetPageId(), this->comparator_);

  parent->SetSize(0);

  page_id_t new_parent_id;
  auto *new_parent = reinterpret_cast<InternalPage *>(this->buffer_pool_manager_->NewPage(&new_parent_id)->GetData());
  new_parent->Init(new_parent_id, new_parent_id, this->internal_max_size_);

  for (int i = 0; i < parent->GetMinSize(); i++) {
    parent->Insert(tmp->KeyAt(i), tmp->ValueAt(i), this->comparator_);
  }

  new_parent->SetValueAt(0, tmp->ValueAt(parent->GetMinSize()));
  new_parent->IncreaseSize(1);
  for (int i = parent->GetMinSize() + 1; i < this->internal_max_size_ + 1; i++) {
    new_parent->Insert(tmp->KeyAt(i), tmp->ValueAt(i), this->comparator_);
  }

  for (int i = 0; i < new_parent->GetSize(); i++) {
    auto tmp_page = buffer_pool_manager_->FetchPage(new_parent->ValueAt(i));
    auto tmp_node = reinterpret_cast<BPlusTreePage *>(tmp_page->GetData());
    tmp_node->SetParentPageId(new_parent_id);

    buffer_pool_manager_->UnpinPage(tmp_node->GetPageId(), true);
  }

  KeyType key_insert = tmp->KeyAt(parent->GetMinSize());
  free(tmp);
  this->InsertInParent(parent, key_insert, new_parent);

  this->buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  this->buffer_pool_manager_->UnpinPage(new_parent->GetPageId(), true);

  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (this->IsEmpty()) {
    return;
  }

  std::stack<Page *> page_with_locks;
  LeafPage *leaf = nullptr;

  Page *cur_page = buffer_pool_manager_->FetchPage(root_page_id_);
  cur_page->WLatch();
  page_with_locks.push(cur_page);

  BPlusTreePage *cur_node = nullptr;

  while (true) {
    cur_node = reinterpret_cast<BPlusTreePage *>(cur_page->GetData());

    if (cur_node->IsLeafPage()) {
      break;
    }

    auto next_page_id = reinterpret_cast<InternalPage *>(cur_node)->Search(key, this->comparator_);
    auto *tmp_page = buffer_pool_manager_->FetchPage(next_page_id);
    tmp_page->WLatch();
    auto *tmp_node = reinterpret_cast<BPlusTreePage *>(tmp_page->GetData());
    if (tmp_node->GetSize() > tmp_node->GetMinSize()) {
      while (!page_with_locks.empty()) {
        Page *page_item = page_with_locks.top();
        page_with_locks.pop();

        page_item->WUnlatch();
      }
    }
    page_with_locks.push(tmp_page);

    this->buffer_pool_manager_->UnpinPage(cur_node->GetPageId(), false);

    cur_page = tmp_page;
  }

  leaf = static_cast<LeafPage *>(cur_node);

  if (leaf != nullptr) {
    ValueType value;

    if (leaf->Search(key, &value, this->comparator_)) {
      this->RemoveEntry(leaf, key);
    }

    while (!page_with_locks.empty()) {
      Page *page_item = page_with_locks.top();
      page_with_locks.pop();

      page_item->WUnlatch();
    }

    this->buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveEntry(BPlusTreePage *node, const KeyType &key) {
  if (node->IsLeafPage()) {
    (static_cast<LeafPage *>(node))->Remove(key, this->comparator_);
  } else {
    (static_cast<InternalPage *>(node))->Remove(key, this->comparator_);
  }

  Page *page_need_lock;

  if (node->IsRootPage() && node->GetSize() == 1) {
    if (!node->IsLeafPage()) {
      page_id_t new_root_page_id = (static_cast<InternalPage *>(node))->ValueAt(0);
      auto *new_root =
          reinterpret_cast<BPlusTreePage *>(this->buffer_pool_manager_->FetchPage(new_root_page_id)->GetData());

      new_root->SetParentPageId(new_root_page_id);
      this->root_page_id_ = new_root_page_id;
      this->UpdateRootPageId(0);
      this->buffer_pool_manager_->DeletePage(node->GetPageId());
    }

    return;
  }

  if (node->GetSize() >= node->GetMinSize() || node->IsRootPage()) {
    return;
  }

  // too few datas
  page_id_t parent_page_id = node->GetParentPageId();
  auto *parent = reinterpret_cast<InternalPage *>(this->buffer_pool_manager_->FetchPage(parent_page_id));

  page_id_t tmp_page_id = INVALID_PAGE_ID;
  BPlusTreePage *prev_node = nullptr;
  BPlusTreePage *node_need_unpin = nullptr;
  KeyType middle_key;
  int middle_key_index;
  for (int i = 0; i < parent->GetSize(); i++) {
    if (parent->ValueAt(i) == node->GetPageId()) {
      if (i > 0) {
        tmp_page_id = parent->ValueAt(i - 1);
        middle_key = parent->KeyAt(i);
        middle_key_index = i;

        page_need_lock = this->buffer_pool_manager_->FetchPage(tmp_page_id);
        page_need_lock->WLatch();
        prev_node = reinterpret_cast<BPlusTreePage *>(page_need_lock->GetData());
        node_need_unpin = prev_node;
      } else {
        middle_key = parent->KeyAt(i + 1);
        middle_key_index = i + 1;
        prev_node = node;
        tmp_page_id = parent->ValueAt(i + 1);
        page_need_lock = this->buffer_pool_manager_->FetchPage(tmp_page_id);
        page_need_lock->WLatch();
        node = reinterpret_cast<BPlusTreePage *>(page_need_lock->GetData());
        node_need_unpin = node;
      }
      break;
    }
  }

  if (node->GetSize() + prev_node->GetSize() <= node->GetMaxSize()) {
    if (!node->IsLeafPage()) {
      auto real_node = static_cast<InternalPage *>(node);
      auto real_prev_node = static_cast<InternalPage *>(prev_node);
      real_node->SetKeyAt(0, middle_key);
      for (int i = 0; i < real_node->GetSize(); i++) {
        real_prev_node->Insert(real_node->KeyAt(i), real_node->ValueAt(i), this->comparator_);
      }
    } else {
      auto real_node = static_cast<LeafPage *>(node);
      auto real_prev_node = static_cast<LeafPage *>(prev_node);
      real_prev_node->SetNextPageId(real_node->GetNextPageId());
      for (int i = 0; i < real_node->GetSize(); i++) {
        real_prev_node->Insert(real_node->KeyAt(i), real_node->ValueAt(i), this->comparator_);
      }
    }

    this->RemoveEntry(parent, middle_key);
    this->buffer_pool_manager_->DeletePage(node->GetPageId());

    page_need_lock->WUnlatch();

    this->buffer_pool_manager_->UnpinPage(node_need_unpin->GetPageId(), true);
    this->buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  } else {
    if (node->GetSize() < prev_node->GetSize()) {
      // remove one pair from prev_node to node
      if (!node->IsLeafPage()) {
        auto real_node = static_cast<InternalPage *>(node);
        auto real_prev_node = static_cast<InternalPage *>(prev_node);

        real_node->SetKeyAt(0, middle_key);
        for (int i = 0; i < real_node->GetSize(); i++) {
          real_node->SetKeyAt(i + 1, real_node->KeyAt(i));
          real_node->SetValueAt(i + 1, real_node->ValueAt(i));
        }
        real_node->SetValueAt(0, real_prev_node->ValueAt(real_prev_node->GetSize() - 1));
        parent->SetKeyAt(middle_key_index, real_prev_node->KeyAt(real_prev_node->GetSize() - 1));
        real_prev_node->IncreaseSize(-1);
      } else {
        auto real_node = static_cast<LeafPage *>(node);
        auto real_prev_node = static_cast<LeafPage *>(prev_node);

        for (int i = 0; i < real_node->GetSize(); i++) {
          real_node->SetKeyValueAt(i + 1, real_node->KeyAt(i), real_node->ValueAt(i));
        }
        real_node->Insert(real_prev_node->KeyAt(real_prev_node->GetSize() - 1),
                          real_prev_node->ValueAt(real_prev_node->GetSize() - 1), this->comparator_);
        parent->SetKeyAt(middle_key_index, real_prev_node->KeyAt(real_prev_node->GetSize() - 1));
        real_prev_node->IncreaseSize(-1);
      }
    } else {
      // remove one pair from node to prev_node
      if (!node->IsLeafPage()) {
        auto real_node = static_cast<InternalPage *>(node);
        auto real_prev_node = static_cast<InternalPage *>(prev_node);

        real_prev_node->Insert(middle_key, real_node->ValueAt(0), this->comparator_);
        parent->SetKeyAt(middle_key_index, real_node->KeyAt(1));
        for (int i = 1; i < real_node->GetSize(); i++) {
          real_node->SetKeyAt(i - 1, real_node->KeyAt(i));
          real_node->SetValueAt(i - 1, real_node->ValueAt(i));
        }
        real_node->IncreaseSize(-1);
      } else {
        auto real_node = static_cast<LeafPage *>(node);
        auto real_prev_node = static_cast<LeafPage *>(prev_node);

        real_prev_node->Insert(real_node->KeyAt(0), real_node->ValueAt(0), this->comparator_);
        parent->SetKeyAt(middle_key_index, real_node->KeyAt(1));
        for (int i = 1; i < real_node->GetSize(); i++) {
          real_node->SetKeyValueAt(i - 1, real_node->KeyAt(i), real_node->ValueAt(i));
        }
        real_node->IncreaseSize(-1);
      }
    }

    page_need_lock->WUnlatch();
    this->buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    this->buffer_pool_manager_->UnpinPage(node_need_unpin->GetPageId(), true);
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  auto *node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());

  while (!node->IsLeafPage()) {
    auto real_node = static_cast<InternalPage *>(node);
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
    node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(real_node->ValueAt(0))->GetData());
  }

  buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
  return INDEXITERATOR_TYPE(node->GetPageId(), 0, this->buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  LeafPage *leaf = FindLeaf(key);
  int index = 0;

  for (int i = 0; i < leaf->GetSize(); i++) {
    if (comparator_(leaf->KeyAt(i), key) == 0) {
      index = i;
      break;
    }
  }

  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
  return INDEXITERATOR_TYPE(leaf->GetPageId(), index, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  auto *node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());

  while (!node->IsLeafPage()) {
    auto real_node = static_cast<InternalPage *>(node);
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
    node = reinterpret_cast<BPlusTreePage *>(
        buffer_pool_manager_->FetchPage(real_node->ValueAt(real_node->GetSize() - 1))->GetData());
  }

  buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
  return INDEXITERATOR_TYPE(node->GetPageId(), node->GetSize(), this->buffer_pool_manager_);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return this->root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetParentPageId(BPlusTreePage *page) -> page_id_t {
  page_id_t parent = page->GetParentPageId();
  if (page->IsRootPage()) {
    Page *new_root_page = this->buffer_pool_manager_->NewPage(&parent);
    auto *new_root = reinterpret_cast<InternalPage *>(new_root_page->GetData());

    page->SetParentPageId(parent);
    new_root->Init(parent, parent, this->internal_max_size_);
    this->root_page_id_ = new_root->GetPageId();
    this->UpdateRootPageId(0);

    if (page->IsLeafPage()) {
      auto leaf = static_cast<LeafPage *>(page);
      new_root->SetKeyAt(1, leaf->KeyAt(leaf->GetMinSize() + 1));
    } else {
      auto internal = static_cast<InternalPage *>(page);
      new_root->SetKeyAt(1, internal->KeyAt(internal->GetMinSize() + 1));
    }
    new_root->SetValueAt(0, page->GetPageId());

    this->buffer_pool_manager_->UnpinPage(parent, true);
  }

  return parent;
}
/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
