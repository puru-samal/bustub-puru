//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree.cpp
//
// Identification: src/storage/index/b_plus_tree.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/index/b_plus_tree.h"
#include "common/macros.h"
#include "storage/index/b_plus_tree_debug.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/**
 * @brief Helper function to decide whether current b+tree is empty
 * @return Returns true if this B+ tree has no keys and values.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  ReadPageGuard guard = bpm_->ReadPage(header_page_id_);
  auto root_page = guard.As<BPlusTreeHeaderPage>();
  return root_page->root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * HELPER FUNCTIONS
 *****************************************************************************/

/**
 * @brief Creates a new root page as a leaf page,
 *  adds the new root page to the write set
 *  updates the root page id in the header page
 *
 * @param header_page Pointer to the header page
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CreateNewRootAsLeafPage(BPlusTreeHeaderPage *header_page) -> page_id_t {
  BUSTUB_ASSERT(header_page->root_page_id_ == INVALID_PAGE_ID,
                "[CreateNewRootAsLeafPage] Root page is valid while creating new root for empty tree!");
  Context ctx;
  auto root_page_id = bpm_->NewPage();
  header_page->root_page_id_ = root_page_id;
  auto root_page = bpm_->WritePage(root_page_id).AsMut<LeafPage>();
  root_page->Init(leaf_max_size_);
  return root_page_id;
}

/**
 * Finds the leaf page containing the given key while acquiring necessary read locks.
 *
 * Locking Protocol:
 * 1. First acquires lock on parent page
 * 2. Then acquires lock on child page
 * 3. Once lock is acquired on child page, releases parent lock
 *
 * @param key The key to search for
 * @param ctx Context object containing lock information
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::FindLeafPageWithReadGuard(const KeyType &key, Context &ctx) {
  BUSTUB_ASSERT(!ctx.header_page_.has_value(),
                "[FindLeafPageWithReadGuard] Header page should be released after acquiring lock on root page");
  BUSTUB_ASSERT(ctx.root_page_id_ != INVALID_PAGE_ID, "[FindLeafPageWithReadGuard] Root page id is invalid");
  BUSTUB_ASSERT(ctx.read_set_.size() == 1 && ctx.read_set_.back().GetPageId() == ctx.root_page_id_,
                "[FindLeafPageWithReadGuard] Root Page guard is not in the read set");

  // Start from the root page
  auto current_page = ctx.read_set_.back().As<BPlusTreePage>();

  // Traverse down to leaf level
  while (!current_page->IsLeafPage()) {
    auto internal_page = ctx.read_set_.back().As<InternalPage>();
    auto child_page_id = internal_page->Lookup(key, comparator_);
    // Acquire lock on child page
    ctx.read_set_.push_back(bpm_->ReadPage(child_page_id));
    // Release lock on parent page
    ctx.read_set_.pop_front();
    current_page = ctx.read_set_.back().As<BPlusTreePage>();
  }

  BUSTUB_ASSERT(current_page->IsLeafPage(), "[FindLeafPageWithReadGuard] current_page != leaf page");
  BUSTUB_ASSERT(ctx.read_set_.size() == 1, "[FindLeafPageWithReadGuard] read set should have only the leaf page guard");
}

/**
 * Finds the leaf page containing the given key while acquiring necessary write locks.
 *
 * Locking Protocol:
 * 1. First acquires lock on parent page
 * 2. Then acquires lock on child page
 * 3. Once lock is acquired on child page, releases locks on all ancestors (parent-first) if child is safe
 *
 * @param key The key to search for
 * @param ctx Context object containing lock information
 * @param op The operation type (insertion or removal)
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::FindLeafPageWithWriteGuard(const KeyType &key, Context &ctx, Op op) {
  BUSTUB_ASSERT(ctx.header_page_.has_value(), "[FindLeafPageWithWriteGuard] Header page is not set");
  BUSTUB_ASSERT(ctx.root_page_id_ != INVALID_PAGE_ID, "[FindLeafPageWithWriteGuard] Root page id is invalid");
  BUSTUB_ASSERT(ctx.write_set_.size() == 1 && ctx.write_set_.back().GetPageId() == ctx.root_page_id_,
                "[FindLeafPageWithWriteGuard] Root Page guard is not in the write set");

  // Lambda function to check if the page is safe for the operation
  auto is_safe = [op](const BPlusTreePage *page) {
    return op == Op::INSERT ? page->IsInsertSafe() : page->IsRemoveSafe();
  };

  // Start from the root page
  auto current_page = ctx.write_set_.back().As<BPlusTreePage>();

  // Lambda function to check if the root page is safe for the operation
  auto root_condition = [op](const BPlusTreePage *page) {
    return op == Op::INSERT ? page->IsInsertSafe() : page->IsRootRemoveSafe();
  };

  // If root is safe, release header page lock
  if (root_condition(current_page)) {
    ctx.header_page_ = std::nullopt;
  }

  // Traverse down to leaf level
  while (!current_page->IsLeafPage()) {
    auto internal_page = ctx.write_set_.back().As<InternalPage>();
    auto child_page_id = internal_page->Lookup(key, comparator_);
    // Acquire lock on child page
    ctx.write_set_.push_back(bpm_->WritePage(child_page_id));
    auto child_page = ctx.write_set_.back().As<BPlusTreePage>();
    // If the child is safe, release all ancestor locks
    if (is_safe(child_page)) {
      if (ctx.header_page_.has_value()) {
        ctx.header_page_ = std::nullopt;
      }
      while (ctx.write_set_.size() > 1) {
        ctx.write_set_.pop_front();
      }
    }
    current_page = ctx.write_set_.back().As<BPlusTreePage>();
  }

  BUSTUB_ASSERT(ctx.write_set_.back().As<BPlusTreePage>()->IsLeafPage(),
                "[FindLeafPageWithWriteGuard] current_page != leaf page");
}

/**
 * @brief Recursively insert a key-value pair into the B+ tree after the leaf page is split
 *
 * @param ctx
 * @param left_page_id : the page id of the left page (existing page)
 * @param key : the key to insert in the parent page
 * @param right_page_id : the page id of the right page (newly created page)
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInParent(Context &ctx, page_id_t left_page_id, KeyType key, page_id_t right_page_id) {
  // Case 1: Left page is root page - create a new root page
  if (left_page_id == ctx.root_page_id_) {
    // printf("[InsertInParent] Creating new root page\n");
    BUSTUB_ASSERT(ctx.header_page_.has_value(), "[InsertInParent] Header page is not set while creating new root page");
    BUSTUB_ASSERT(ctx.write_set_.empty(), "[InsertInParent] Write set is not empty for root page");

    auto new_root_page_id = bpm_->NewPage();
    auto new_root_page_guard = bpm_->WritePage(new_root_page_id);
    auto new_root_page = new_root_page_guard.AsMut<InternalPage>();
    new_root_page->Init(internal_max_size_);

    // Set up new root's pointers and key
    new_root_page->ChangeSizeBy(2);
    new_root_page->SetValueAt(0, left_page_id);
    new_root_page->SetKeyAt(1, key);
    new_root_page->SetValueAt(1, right_page_id);

    // Update header page
    auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
    header_page->root_page_id_ = new_root_page_id;
    ctx.root_page_id_ = new_root_page_id;
    return;
  }

  if (ctx.write_set_.empty()) {
    // printf("[InsertInParent] Write set is empty\n");
    return;
  }

  // Case 2: Parent page is safe - insert into parent page
  auto parent_page = ctx.write_set_.back().AsMut<InternalPage>();
  if (parent_page->IsInsertSafe()) {
    // printf("[InsertInParent] Parent page is safe\n");
    parent_page->SafeInsert(key, right_page_id, comparator_);
    ctx.write_set_.pop_back();
    BUSTUB_ASSERT(ctx.write_set_.empty(), "[InsertInParent] Write set is not empty after parent page is safe");
    return;
  }

  // Case 3: Parent page needs to be split - create a new internal page
  auto new_page_id = bpm_->NewPage();
  auto new_page_guard = bpm_->WritePage(new_page_id);
  auto new_internal_page = new_page_guard.AsMut<InternalPage>();
  new_internal_page->Init(internal_max_size_);

  // Redistribute half of the keys and values from the old internal page to the new internal page
  KeyType split_key = parent_page->Distribute(new_internal_page, key, right_page_id, comparator_);

  left_page_id = ctx.write_set_.back().GetPageId();
  right_page_id = new_page_id;
  ctx.write_set_.pop_back();
  return InsertInParent(ctx, left_page_id, split_key, right_page_id);
}

/**
 * @brief Find a neighbor page to coalesce or redistribute when removing a key
 *
 * @param ctx
 * @param key
 * @param current_page
 * @return Tuple of neighbor page guard, remove operation (merge or redistribute), separator key , and neighbor type
 * (predecessor or successor)
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindNeighborForRemove(Context &ctx, const KeyType &key, BPlusTreePage *current_page)
    -> std::tuple<WritePageGuard, RemoveOp, KeyType, NeighborType> {
  // Get parent page and find potential neighbors with their separator keys
  auto parent_page = ctx.write_set_.at(ctx.write_set_.size() - 2).AsMut<InternalPage>();
  auto [prev_info, next_info] = parent_page->GetNeighborInfo(key, comparator_);

  std::optional<WritePageGuard> neighbor_guard;
  RemoveOp remove_op;
  KeyType separator_key;
  NeighborType neighbor_type;

  // Try next neighbor first if it exists
  if (next_info.value_.has_value()) {
    neighbor_guard = bpm_->WritePage(next_info.value_.value());
    auto next_page = neighbor_guard->As<BPlusTreePage>();

    // Check if redistribution is possible
    if (next_page->GetSize() >= (current_page->GetMinSize() + 1)) {
      separator_key = next_info.separator_key_.value();
      remove_op = RemoveOp::REDISTRIBUTE;
      neighbor_type = NeighborType::SUCCESSOR;
      return {std::move(neighbor_guard.value()), remove_op, separator_key, neighbor_type};
    }

    // Check if merge is possible
    if (next_page->GetSize() + current_page->GetSize() <= current_page->GetMaxSize()) {
      separator_key = next_info.separator_key_.value();
      remove_op = RemoveOp::MERGE;
      neighbor_type = NeighborType::SUCCESSOR;
      return {std::move(neighbor_guard.value()), remove_op, separator_key, neighbor_type};
    }

    // Release the neighbor guard
    neighbor_guard = std::nullopt;
  }

  BUSTUB_ASSERT(prev_info.value_.has_value(), "[FindNeighborForRemove] Previous neighbor is not set");

  // Try previous neighbor if next didn't work
  neighbor_guard = bpm_->WritePage(prev_info.value_.value());
  auto prev_page = neighbor_guard->As<BPlusTreePage>();

  // Check if redistribution is possible
  if (prev_page->GetSize() >= (current_page->GetMinSize() + 1)) {
    separator_key = prev_info.separator_key_.value();
    remove_op = RemoveOp::REDISTRIBUTE;
    neighbor_type = NeighborType::PREDECESSOR;
    return {std::move(neighbor_guard.value()), remove_op, separator_key, neighbor_type};
  }

  // Merge has to be possible
  BUSTUB_ASSERT(prev_page->GetSize() + current_page->GetSize() <= current_page->GetMaxSize(),
                "[FindNeighborForRemove] Previous page does not have enough space for merge");
  separator_key = prev_info.separator_key_.value();
  remove_op = RemoveOp::MERGE;
  neighbor_type = NeighborType::PREDECESSOR;
  return {std::move(neighbor_guard.value()), remove_op, separator_key, neighbor_type};
}

/**
 * @brief Recursive helper function to remove entries from the B+ tree
 *
 * @param ctx
 * @param key : the key to remove from the internal page
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromParent(Context &ctx, KeyType key) {
  BUSTUB_ASSERT(!ctx.write_set_.empty(), "[RemoveFromParent] Write set should not be empty");

  // First, delete the key from the parent page
  auto current_internal = ctx.write_set_.back().AsMut<InternalPage>();
  current_internal->Remove(key, comparator_);

  // If the parent page is root page
  if (ctx.IsRootPage(ctx.write_set_.back().GetPageId())) {
    // if it had only one child, delete the parent page and set the root page to the only child page
    if (current_internal->GetSize() == 1) {
      // printf("[RemoveFromParent] Internal page is root page and has only one child\n");
      BUSTUB_ASSERT(ctx.write_set_.size() == 1, "[Remove] Write set should have only the current internal page guard");
      BUSTUB_ASSERT(ctx.header_page_.has_value(), "[Remove] Header page should be set");
      ctx.header_page_->AsMut<BPlusTreeHeaderPage>()->root_page_id_ = current_internal->ValueAt(0);
      ctx.write_set_.pop_back();
      bpm_->DeletePage(ctx.root_page_id_);
    }
    return;
  }

  // Case 1: Internal page is safe for removal post removal
  if (!current_internal->TooFew()) {
    // printf("[RemoveFromParent] Internal page is safe for removal post removal\n");
    BUSTUB_ASSERT(ctx.write_set_.size() == 1, "[Remove] Write set should have only the current internal page guard");
    BUSTUB_ASSERT(!ctx.header_page_.has_value(), "[Remove] Header page should not be set");
    return;
  }

  BUSTUB_ASSERT(ctx.write_set_.size() >= 2,
                "[RemoveFromParent] Write set should have at least current and parent page");

  // Case 2: Internal page is not safe for removal post removal
  auto [neighbor_guard, remove_op, separator_key, neighbor_type] =
      FindNeighborForRemove(ctx, key, ctx.write_set_.back().AsMut<BPlusTreePage>());

  // Case 2.1: If merge is possible, merge the leaf page with the neighbor page
  if (remove_op == RemoveOp::MERGE) {
    // printf("[RemoveFromParent] Merge is possible\n");
    //  Determine which page is predecessor and which is successor
    bool is_neighbor_successor = (neighbor_type == NeighborType::SUCCESSOR);
    auto neighbor_internal = neighbor_guard.template AsMut<InternalPage>();

    // Set up pages for merging
    auto *target_page = is_neighbor_successor ? current_internal : neighbor_internal;
    auto *source_page = is_neighbor_successor ? neighbor_internal : current_internal;
    auto page_to_delete = is_neighbor_successor ? neighbor_guard.GetPageId() : ctx.write_set_.back().GetPageId();

    // printf("[RemoveFromParent] Target page (will contain merged results): %s\n",
    // is_neighbor_successor ? "neighbor internal" : "current internal");
    // printf("[RemoveFromParent] Source page (will be merged into target): %s\n",
    // is_neighbor_successor ? "neighbor internal" : "current internal");
    // printf("[RemoveFromParent] Page to delete: %s\n", is_neighbor_successor ? "neighbor internal" : "current
    // internal");

    // Merge pages and clean up
    target_page->Merge(source_page, separator_key, comparator_);
    ctx.write_set_.pop_back();
    bpm_->DeletePage(page_to_delete);
    return RemoveFromParent(ctx, separator_key);
  }

  BUSTUB_ASSERT(remove_op == RemoveOp::REDISTRIBUTE, "[Remove] Invalid remove operation");
  // printf("[RemoveFromParent] Redistribute is possible\n");
  //  Case 2.2: Redistribute the entries between the leaf page and the neighbor page
  KeyType redistributed_key = current_internal->Redistribute(neighbor_guard.template AsMut<InternalPage>(),
                                                             separator_key, neighbor_type, comparator_);

  // Replace the key in the parent page with the redistributed key
  auto parent_page = ctx.write_set_.at(ctx.write_set_.size() - 2).AsMut<InternalPage>();
  parent_page->ReplaceKey(separator_key, redistributed_key, comparator_);

  // Release all locks top down
  if (ctx.header_page_.has_value()) {
    ctx.header_page_ = std::nullopt;
  }
  while (!ctx.write_set_.empty()) {
    ctx.write_set_.pop_front();
  }
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/

/**
 * @brief Return the only value that associated with input key
 *
 * This method is used for point query
 *
 * @param key input key
 * @param[out] result vector that stores the only value that associated with input key, if the value exists
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  // Declaration of context instance. Using the Context is not necessary but advised.
  Context ctx;
  // Acquire lock on header page
  ctx.read_set_.push_back(bpm_->ReadPage(header_page_id_));
  auto header_page = ctx.read_set_.back().template As<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    return false;
  }
  // Acquire lock on root page
  ctx.root_page_id_ = header_page->root_page_id_;
  ctx.read_set_.push_back(bpm_->ReadPage(ctx.root_page_id_));

  // Release lock on header page
  ctx.read_set_.pop_front();

  // Traverse down to leaf level
  FindLeafPageWithReadGuard(key, ctx);
  auto leaf_page = ctx.read_set_.back().As<LeafPage>();
  auto value = leaf_page->Lookup(key, comparator_);
  if (value.has_value()) {
    result->push_back(value.value());
    return true;
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/**
 * @brief Insert constant key & value pair into b+ tree
 *
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 *
 * @param key the key to insert
 * @param value the value associated with key
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 *
 * Performance Note: The current implementation may not achieve optimal multi-threaded speedup (>1.1x) due to:
 * 1. Conservative locking strategy: We hold locks on ancestors until we confirm a node is safe for insertion,
 *    which can create contention when multiple threads try to modify the same portion of the tree.
 * 2. Lock acquisition order: We acquire locks in a top-down manner and may need to hold multiple locks
 *    simultaneously to prevent concurrent structural modifications, increasing the likelihood of lock conflicts.
 * 3. Single mutex for header page: All operations need to acquire the header page lock first, creating
 *    a potential bottleneck at the tree's entry point.
 * 4. Buffer pool contention: Multiple threads accessing the same pages create additional contention at
 *    the buffer pool level, as seen in the buffer pool manager's locking mechanism.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  // Store the header page guard in header_page_ since root might be modified
  Context ctx;
  ctx.header_page_ = bpm_->WritePage(header_page_id_);
  auto is_empty = ctx.header_page_->AsMut<BPlusTreeHeaderPage>()->root_page_id_ == INVALID_PAGE_ID;

  // If tree is empty, create a new leaf page as root page
  if (is_empty) {
    // Sets the root page id in the header page
    CreateNewRootAsLeafPage(ctx.header_page_->AsMut<BPlusTreeHeaderPage>());
  }

  // Initialize the context
  ctx.root_page_id_ = ctx.header_page_->AsMut<BPlusTreeHeaderPage>()->root_page_id_;
  ctx.write_set_.push_back(bpm_->WritePage(ctx.root_page_id_));

  // Find the leaf page to insert the key-value pair
  FindLeafPageWithWriteGuard(key, ctx, Op::INSERT);
  auto leaf_page = ctx.write_set_.back().AsMut<LeafPage>();
  // If the key already exists, return false
  if (leaf_page->KeyExists(key, comparator_)) {
    // Release all locks top down
    if (ctx.header_page_.has_value()) {
      ctx.header_page_ = std::nullopt;
    }
    while (!ctx.write_set_.empty()) {
      ctx.write_set_.pop_front();
    }
    return false;
  }

  // Case 1: Leaf page has space, insert the key-value pair
  if (leaf_page->IsInsertSafe()) {
    leaf_page->SafeInsert(key, value, comparator_);
    return true;
  }

  // Case 2: Leaf page needs to be split

  // Create a new leaf page
  auto new_leaf_page_id = bpm_->NewPage();
  auto new_leaf_page_guard = bpm_->WritePage(new_leaf_page_id);
  auto new_leaf_page = new_leaf_page_guard.AsMut<LeafPage>();
  new_leaf_page->Init(leaf_max_size_);

  // Link pages
  new_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
  leaf_page->SetNextPageId(new_leaf_page_id);

  // Distribute entries between pages
  leaf_page->Distribute(new_leaf_page, key, value, comparator_);

  // Get the left page id and right page id
  auto left_page_id = ctx.write_set_.back().GetPageId();
  auto right_page_id = new_leaf_page_id;

  // Release write locks on left page
  ctx.write_set_.pop_back();

  // Update Parent Page
  InsertInParent(ctx, left_page_id, new_leaf_page->KeyAt(0), right_page_id);
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/

/**
 * @brief Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 *
 * @param key input key
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  // Store the header page guard in header_page_ since root might be modified
  Context ctx;
  ctx.header_page_ = bpm_->WritePage(header_page_id_);
  auto is_empty = ctx.header_page_->AsMut<BPlusTreeHeaderPage>()->root_page_id_ == INVALID_PAGE_ID;

  // If tree is empty, nothing to do
  if (is_empty) {
    // printf("[Remove] Tree is empty\n");
    return;
  }

  // Initialize the context
  ctx.root_page_id_ = ctx.header_page_->AsMut<BPlusTreeHeaderPage>()->root_page_id_;
  ctx.write_set_.push_back(bpm_->WritePage(ctx.root_page_id_));

  // Find the leaf page to insert the key-value pair
  FindLeafPageWithWriteGuard(key, ctx, Op::REMOVE);
  auto current_leaf = ctx.write_set_.back().AsMut<LeafPage>();

  // If the key does not exist, return
  if (!current_leaf->KeyExists(key, comparator_)) {
    // printf("[Remove] Key does not exist\n");
    // Release all locks top down
    if (ctx.header_page_.has_value()) {
      ctx.header_page_ = std::nullopt;
    }
    while (!ctx.write_set_.empty()) {
      ctx.write_set_.pop_front();
    }
    return;
  }

  // Remove entry from the leaf page
  current_leaf->Remove(key, comparator_);

  // If leaf page is root page
  if (ctx.IsRootPage(ctx.write_set_.back().GetPageId())) {
    // if it is empty, remove it
    if (current_leaf->GetSize() == 0) {
      // printf("[Remove] Leaf page is root page and is empty\n");
      BUSTUB_ASSERT(ctx.write_set_.size() == 1, "[Remove] Write set should have only leaf page guard");
      BUSTUB_ASSERT(ctx.header_page_.has_value(), "[Remove] Header page should be set");
      // Release lock on leaf page
      // Delete the leaf page
      // Set the root page id to invalid page id
      ctx.write_set_.pop_back();
      bpm_->DeletePage(ctx.root_page_id_);
      ctx.header_page_->AsMut<BPlusTreeHeaderPage>()->root_page_id_ = INVALID_PAGE_ID;
    }
    return;
  }

  // Case 1: Leaf page is safe for removal post removal
  if (!current_leaf->TooFew()) {
    // printf("[Remove] Leaf page is safe for removal post removal\n");
    BUSTUB_ASSERT(ctx.write_set_.size() == 1, "[Remove] Write set should have only the leaf page guard");
    BUSTUB_ASSERT(!ctx.header_page_.has_value(), "[Remove] Header page is set while a non-root leaf page is safe");
    // Lock on leaf page will be released after the function returns
    return;
  }

  BUSTUB_ASSERT(ctx.write_set_.size() >= 2, "[Remove] Write set should have at least current and parent page");

  // Case 2: Leaf page is not root and is unsafe for removal post removal
  auto [neighbor_guard, remove_op, separator_key, neighbor_type] =
      FindNeighborForRemove(ctx, key, ctx.write_set_.back().AsMut<BPlusTreePage>());

  // Case 2.1: If merge is possible, merge the leaf page with the neighbor page
  if (remove_op == RemoveOp::MERGE) {
    // printf("[Remove] Merge is possible\n");
    // Determine which page is predecessor and which is successor
    bool is_neighbor_successor = (neighbor_type == NeighborType::SUCCESSOR);
    auto neighbor_leaf = neighbor_guard.template AsMut<LeafPage>();

    // Set up pages for merging
    auto *target_page = is_neighbor_successor ? current_leaf : neighbor_leaf;
    auto *source_page = is_neighbor_successor ? neighbor_leaf : current_leaf;
    auto page_to_delete = is_neighbor_successor ? neighbor_guard.GetPageId() : ctx.write_set_.back().GetPageId();

    /*
      printf("[Remove] Target page (will contain merged results): %s\n",
           is_neighbor_successor ? "current leaf" : "neighbor leaf");
      printf("[Remove] Source page (will be merged into target): %s\n",
           is_neighbor_successor ? "neighbor leaf" : "current leaf");
      printf("[Remove] Page to delete: %s\n", is_neighbor_successor ? "neighbor leaf" : "current leaf");
    */

    // Merge pages and clean up
    target_page->Merge(source_page, comparator_);
    ctx.write_set_.pop_back();
    bpm_->DeletePage(page_to_delete);
    return RemoveFromParent(ctx, separator_key);
  }

  BUSTUB_ASSERT(remove_op == RemoveOp::REDISTRIBUTE, "[Remove] Invalid remove operation");
  // printf("[Remove] Redistribute is possible\n");
  //  Case 2.2: Redistribute the entries between the leaf page and the neighbor page
  KeyType redistributed_key =
      current_leaf->Redistribute(neighbor_guard.template AsMut<LeafPage>(), neighbor_type, comparator_);

  // Replace the key in the parent page with the redistributed key
  auto parent_page = ctx.write_set_.at(ctx.write_set_.size() - 2).AsMut<InternalPage>();
  parent_page->ReplaceKey(separator_key, redistributed_key, comparator_);

  // Release all locks top down
  if (ctx.header_page_.has_value()) {
    ctx.header_page_ = std::nullopt;
  }
  while (!ctx.write_set_.empty()) {
    ctx.write_set_.pop_front();
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/

/**
 * @brief Find the leftmost leaf page with read guard
 *
 * @param ctx
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::FindBeginWithReadGuard(Context &ctx) {
  BUSTUB_ASSERT(!ctx.header_page_.has_value(),
                "[FindBeginWithReadGuard] Header page should be released after acquiring lock on root page");
  BUSTUB_ASSERT(ctx.root_page_id_ != INVALID_PAGE_ID, "[FindBeginWithReadGuard] Root page id is invalid");
  BUSTUB_ASSERT(ctx.read_set_.size() == 1 && ctx.read_set_.back().GetPageId() == ctx.root_page_id_,
                "[FindBeginWithReadGuard] Root Page guard is not in the read set");

  // Start from the root page
  auto current_page = ctx.read_set_.back().As<BPlusTreePage>();

  // Traverse down to leaf level
  while (!current_page->IsLeafPage()) {
    auto internal_page = ctx.read_set_.back().As<InternalPage>();
    // Get the first child page id
    auto child_page_id = internal_page->ValueAt(0);
    // Acquire lock on child page
    ctx.read_set_.push_back(bpm_->ReadPage(child_page_id));
    // Release lock on parent page
    ctx.read_set_.pop_front();
    current_page = ctx.read_set_.back().As<BPlusTreePage>();
  }

  BUSTUB_ASSERT(current_page->IsLeafPage(), "[FindBeginWithReadGuard] current_page != leaf page");
  BUSTUB_ASSERT(ctx.read_set_.size() == 1, "[FindBeginWithReadGuard] read set should have only the leaf page guard");
}

/**
 * @brief Find the rightmost leaf page with read guard
 *
 * @param ctx
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::FindEndWithReadGuard(Context &ctx) {
  BUSTUB_ASSERT(!ctx.header_page_.has_value(),
                "[FindEndWithReadGuard] Header page should be released after acquiring lock on root page");
  BUSTUB_ASSERT(ctx.root_page_id_ != INVALID_PAGE_ID, "[FindEndWithReadGuard] Root page id is invalid");
  BUSTUB_ASSERT(ctx.read_set_.size() == 1 && ctx.read_set_.back().GetPageId() == ctx.root_page_id_,
                "[FindEndWithReadGuard] Root Page guard is not in the read set");

  // Start from the root page
  auto current_page = ctx.read_set_.back().As<BPlusTreePage>();

  // Traverse down to leaf level
  while (!current_page->IsLeafPage()) {
    auto internal_page = ctx.read_set_.back().As<InternalPage>();
    // Get the last child page id
    auto child_page_id = internal_page->ValueAt(internal_page->GetSize() - 1);
    // Acquire lock on child page
    ctx.read_set_.push_back(bpm_->ReadPage(child_page_id));
    // Release lock on parent page
    ctx.read_set_.pop_front();
    current_page = ctx.read_set_.back().As<BPlusTreePage>();
  }

  BUSTUB_ASSERT(current_page->IsLeafPage(), "[FindEndWithReadGuard] current_page != leaf page");
  BUSTUB_ASSERT(ctx.read_set_.size() == 1, "[FindEndWithReadGuard] read set should have only the leaf page guard");
}

/**
 * @brief Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 *
 * You may want to implement this while implementing Task #3.
 *
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  Context ctx;
  // Acquire lock on header page
  ctx.read_set_.push_back(bpm_->ReadPage(header_page_id_));
  auto header_page = ctx.read_set_.back().template As<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(bpm_, INVALID_PAGE_ID, -1);
  }
  // Acquire lock on root page
  ctx.root_page_id_ = header_page->root_page_id_;
  ctx.read_set_.push_back(bpm_->ReadPage(ctx.root_page_id_));

  // Release lock on header page
  ctx.read_set_.pop_front();

  FindBeginWithReadGuard(ctx);
  return INDEXITERATOR_TYPE(bpm_, ctx.read_set_.back().GetPageId(), 0);
}

/**
 * @brief Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  // Acquire lock on header page
  Context ctx;
  ctx.read_set_.push_back(bpm_->ReadPage(header_page_id_));
  auto header_page = ctx.read_set_.back().template As<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(bpm_, INVALID_PAGE_ID, -1);
  }

  // Acquire lock on root page
  ctx.root_page_id_ = header_page->root_page_id_;
  ctx.read_set_.push_back(bpm_->ReadPage(ctx.root_page_id_));

  // Release lock on header page
  ctx.read_set_.pop_front();

  // Find the leaf page with the key
  FindLeafPageWithReadGuard(key, ctx);
  auto leaf_page = ctx.read_set_.back().As<LeafPage>();
  auto index = leaf_page->FindGreaterEqual(key, comparator_);
  return INDEXITERATOR_TYPE(bpm_, ctx.read_set_.back().GetPageId(), index);
}

/**
 * @brief Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  Context ctx;
  // Acquire lock on header page
  ctx.read_set_.push_back(bpm_->ReadPage(header_page_id_));
  auto header_page = ctx.read_set_.back().template As<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(bpm_, INVALID_PAGE_ID, -1);
  }
  // Acquire lock on root page
  ctx.root_page_id_ = header_page->root_page_id_;
  ctx.read_set_.push_back(bpm_->ReadPage(ctx.root_page_id_));

  // Release lock on header page
  ctx.read_set_.pop_front();

  FindEndWithReadGuard(ctx);
  auto leaf_page = ctx.read_set_.back().As<LeafPage>();
  return INDEXITERATOR_TYPE(bpm_, ctx.read_set_.back().GetPageId(), leaf_page->GetSize());
}

/**
 * @return Page id of the root of this tree
 *
 * You may want to implement this while implementing Task #3.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  Context ctx;
  ctx.read_set_.push_back(bpm_->ReadPage(header_page_id_));
  auto header_page = ctx.read_set_.back().As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
