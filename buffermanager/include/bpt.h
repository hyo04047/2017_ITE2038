#define PAGESIZE 4096
#define false 0
#define true 1
#define LEAF_ORDER 32
#define INTERNAL_ORDER 248

#include <unistd.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>

FILE *fp;
int fd;
int buffer_num;
int currentIndex;
int _nextRef;

typedef int bool;

typedef struct buffercontrolblock{
   char buffer[4096];
   int _tableID;
   off_t _pageOffset;
   bool _isDirty;
   int _pinCount;
   bool _refbit;
}BCB;

BCB *buf;

/*
 * name : buffer
 * argument : table_id, page_offset
 * return value : buffer control block pointer
 * comment : master buffer manage function
 */
BCB * buffer(int _table_id, off_t _pageOffset);

/*
 * name : readPageToBuffer
 * argument : table_id, page_offset
 * return value : if add is success return index, else return -1
 * do : add buffer for table_id & page_offset
 */
int readPageToBuffer(int _table_id, off_t _pageOffset);

/*
 * name : replaceBuffer
 * argument : table_id, page_offset
 * return value : if read is success return index, else return -1
 * do : replace buffer with clockwise algorithm
 */
int replaceBuffer(int _table_id, off_t _pageOffset);

/*
 * name : returnBuffer
 * argument : _buf, _dirty
 * return value : void
 * do : count down pincount of _buf and set _isDirty
 */
void returnBuffer(BCB *_buf, int _dirty);

/*
 * name : init_db
 * argument : number of buffer
 * return value : if init is success return 0, else return -1
 */
int init_db(int buf_num);

/*
 * name : open_db
 * argument : pathname
 * return value : if open is success return _table_id(=fd), else return -1
 */
int open_table(char *_pathname);

/*
 * name : close_table
 * argument : table_id
 * return value : if close is success return 0, else return -1
 */
int close_table(int _table_id);

// getter

/*
 * name : getNumberOfKeys
 * argument : page's offset
 * return value : number of key in page(int)
 */
int getNumberOfKeys(char *_page);

/* name : isLeaf
 * argument : page's offset
 * return value : page status whether page is leaf or not(int)
 * if page is leaf return 1, else return 0
 */
bool isLeaf(char *_page);

/*
 * name : getInternalKey
 * argument : page's offset, index of key
 * return value : The key at the specified index(int64_t)
 * comment : this function can work at Internal Page
 */
int64_t getInternalKey(char *_page, int _keyNum);

/*
 * name : getInternalOffset
 * argument : page's offset, index of offset
 * return value : An offset at the specified index(int64_t)
 * comment : this function can work at Internal page
 */
int64_t getInternalOffset(char *_page, int _offsetNum);

/*
 * name : getLeafKey
 * argument : page's offset, index of key
 * return value : A key at the specified index(int64_t)
 * comment : this function can work at Leaf Page
 */
int64_t getLeafKey(char *_page, int _keyNum);

/*
 * name : getLeafValue
 * argument : page's offset, index of value
 * return value : A value at the specified index(int64_t)
 * comment : this function can work at Leaf Page
 */
char *getLeafValue(char *_page, int _valueNum);

/*
 * name : getParent
 * argument : page's offset
 * return value : An offset that input offset's parent(int64_t)
 * comment : this function can work at both normal page and free page
 * but I recommend using it at normal page
 */
int64_t getParent(char *_page);

/*
 * name : getNextFreePage
 * argument : page's offset
 * return value : An offset that input offset's next free page(int64_t)
 * comment : this function can work at both normal page and free page
 * but I recommend using it at free page
 */
int64_t getNextFreePage(char *_page);

/*
 * name : getRightSibling
 * argument : page's offset
 * return value : An offset that input offset's right sibling
 */
int64_t getRightSibling(char *_page);

/*
 * name : getHeaderStatus
 * argument : status number that you want to get
 * kinds of status
 *    1 : next free page offset
 *    2 : root's offset
 *    3 : number of pages in file
 * return value : the status that you want to get
 */
int64_t getHeaderStatus(int _table_id, int _stat);


// setter
/*
 * name : setNumberOfKeys
 * argument : page's offset, number of keys
 * do : change page's number of keys into input
 */
void setNumberOfKeys(char *_page, int _num);

/*
 * name : upperNumKey
 * argument : page's offset
 * do : plus 1 from page's number of key
 */
void upperNumKey(char *_page);

/*
 * name : lowerNumKey
 * argument : page's offset
 * do : minus 1 from page's number of key
 */
void lowerNumKey(char *_page);

/*
 * name : setIsLeaf
 * argument : page's offset, status of isleaf
 * do : change isleaf status of page.
 * 1 means leafpage, 0 means internal page or header
 */
void setIsLeaf(char *_page, bool _isLeaf);

/*
 * name : setInternalKey
 * argument : page's offset, key, index of key
 * do : change the key at the specified index
 * comment : this function can work at the Internal Page
 */
void setInternalKey(char *_page, int64_t _key, int _keyNum);

/*
 * name : setInternalOffset
 * argument : page's offset, offset, index of offset
 * do : change the offset at the specified index
 * comment : this function can work at the Internal Page
 */
void setInternalOffset(char *_page, int64_t _offset, int _offsetNum);

/*
 * name : setLeafKey
 * argument : page's offset, key, index of key
 * do : change the key at the specified index
 * comment : this functino can work at the Leaf Page
 */
void setLeafKey(char *_page, int64_t _key, int _keyNum);

/*
 * name : setLeafValue
 * argument : page's offset, value, index of value
 * do : change the value at the specified index
 * comment : this function can work at the Leaf Page
 */
void setLeafValue(char *_page, char *value, int _ValueNum);

/*
 * name : setParent
 * argument : page's offset, parent's offset
 * do : change the parent offset at the page
 * comment : this function can work at both normal and free page
 * but I recommend using it at the normal page
 */
void setParent(char *_page, int64_t _parent);

/*
 * name : setRightSibling
 * argument : page's offset, sibling's offset
 * do : change the sibling offset at the page
 */
void setRightSibling(char *_page, int64_t _sibling);

/*
 * name : setHeaderStatus
 * argument
 * ==== stat ====
 *    1: first free page offset
 *    2: root page offset
 *    3: number of pages in file
 * ==== flag ==== ( this argument is needed when stat is 3 )
 *    -1: minus info from current number of pages
 *    0: setting the number of pages
 *    1: plus info to current number of pages
 * ==== info ====
 *    if stat 1, this means offset of first free page
 *    if stat 2, this means offset of root page
 *    if stat 3, this means number of page +, - or set
 *
 * do : change header status
 */
void setHeaderStatus(int _table_id, int _stat, int _flag, int64_t _info);

// initialize utility function

/*
 * name : initLeaf
 * argument : page's offset
 * do
 *    setting leaf page's number of key to 0
 *    initialize leaf page's all key and value
 */
void initLeaf(char *_page);

/*
 * name : initInternal
 * argument : page's offset
 * do
 *    setting internal page's number of key to 0
 *    initialize internal page's all key and value
 */
void initInternal(char *_page);

/*
 * name : initPage
 * argument : page's offset
 * do
 *    setting all page's bytes to 0
 *    insert this page to free page lists at head
 *    after this function this page is to be first of free page
 */
void initPage(int _table_id, BCB *_page);

// free page management function
/*
 * name : freePageToUse
 * argument : table_id
 * do
 *    if free page list is empty, make 5 free pages
 *    and add it at the free page list
 * return value : free page BCB that we can use it
 */
BCB * freePageToUse(int _table_id);

// insert
/*
 * name : insert
 * argument : table_id, key, value
 * return value : if insertion is success return 0,
 *                else return none zero value
 * comment : master insert function
 */
int insert(int _table_id, int64_t _key, char *_value);

/*
 * name : start_new_tree
 * argument : table_id, key, value
 * do
 *    if B+tree is not exists, make it
 * return value : if success return 0
 */
BCB * start_new_tree(int _table_id, int64_t _key, char *_value);

/*
 * name : insert_into_leaf
 * argument : table_id, leaf page's BCB, key, value
 * do
 *    insert key value pair into leaf page(use linear search)
 * return value : leaf page's offset that same with input
 */
BCB * insert_into_leaf(int _table_id, BCB *_leaf, int64_t _key, char *_value);

/*
 * name : insert_into_leaf_after_splitting
 * argument : table_id, leaf page's BCB, key, value
 * do
 *    insert key value pair into leaf page
 *    first split a full leaf page
 *    next fisrt half one insert into old,
 *    another half one insert into new
 * return value : BCB pointer
 */
BCB * insert_into_leaf_after_splitting(int _table_id, BCB *_leaf,
	int64_t _key, char *_value);

/*
 * name : insert_into_parent
 * argument : table_id, old page's offset(left), key, new page's offset(right)
 * do
 *    insert new page into leaf's parent page
 * return value
 *    if leaf is root, call insert_into_new_root
 *    else if parent has available space, call insert_into_internal
 *    else if call insert_into_internal_after_splitting
 */
BCB * insert_into_parent(int _table_id, BCB *_left, int64_t _key, BCB *_right);

/*
 * name : insert_into_new_root
 * argument : table_id, old page's offset(left), key, new page's offset(right)
 * do
 *    insert splitted pages into new root
 *    and write key into root
 * return value : new root's offset
 */
BCB * insert_into_new_root(int _table_id, BCB *_left, int64_t _key, BCB *_right);

/*
 * name : get_left_index
 * argument : old page's parent page offset, leaf's offset
 * return value : index of leaf's in the parent page
 */
int get_left_index(char *_parent, off_t _left);

/*
 * name : insert_into_internal
 * argument : old page's parent page offset(n), old page's index,
 *            key, new page's index
 * do
 *    insert new page into old page's parent
 * return value : BCB pointer
 */
BCB * insert_into_internal(BCB *_n, int _left_index,
	int64_t _key, BCB *_right);

/*
 * name : insert_into_internal_after_splitting
 * argument : table_id, oldpage's offset, old page's index, key,
 *            new page's offset
 * do
 *    parent page is full, so split parent page
 * return value : call insert_into_parent
 */
BCB * insert_into_internal_after_splitting(int _table_id, BCB *_oldPage, int _left_index,
	int64_t _key, BCB *_right);

/*
 * name : cut
 * argument : page's capacity
 * return value : min number of key that page must have
 */
int cut(int length);

// find
/*
 * name : find_leaf
 * argument : table_id, key
 * return value : An offset that key is exists
 */
BCB * find_leaf(int _table_id, int64_t _key);

/*
 * name : find
 * argument : table_id, key
 * return value : The value that is matched the key
 *                if not exists the key, return NULL
 */
char *find(int _table_id, int64_t _key);

// delete
/*
 * name : delete
 * argument : a key that you want to delete
 * return value : if deletion is success return 1, else return 0
 * comment : master delete function
 */
int delete(int _table_id, int64_t _key);

BCB * delete_entry(int _table_id, BCB *_leafKey, int64_t _key, int64_t _offset);
BCB * adjust_root(int _table_id, BCB * _root);
BCB * redistribute_pages(int _table_id, BCB *_leafKey, BCB *_neighbor,
	int neighbor_index, int k_prime_index, int k_prime);
BCB * coalesce_pages(int _table_id, BCB *_leafKey, BCB *_neighbor,
	int neighbor_index, int k_prime);
int get_neighbor_index(char *_parent, off_t _leaf);
BCB * remove_entry_from_page(BCB *_leafKey, int64_t _key, int64_t _offset);

// close
int close_table(int _table_id);
int shutdown_db();
