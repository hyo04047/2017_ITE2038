#define PAGESIZE 4096
typedef int bool;
#define false 0
#define true 1
#define LEAF_ORDER 32
#define INTERNAL_ORDER 248
#define LOG_BUFFER_NUM 1000

#include <unistd.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>

typedef struct buffercontrolblock{
	char buffer[4096];
	int _tableID;
	off_t _pageOffset;
	bool _isDirty;
	int _pinCount;
	bool _refbit;
	struct buffercontrolblock *_next;
	struct buffercontrolblock *_prev;
}BCB;

typedef struct header{
	off_t _free;
	off_t _root;
	int64_t _numberOfPages;
	int64_t _pageLSN;
}HEADER;

typedef struct hash {
  BCB *head;
}HASH;

typedef struct masterBCB{
	int _maxSize;
	int _nextRef;
	int _tableSize;
	int _xactNumber;
	HASH *searchTable;
	BCB *buffers;
	HEADER *header;
}MBCB;

typedef struct logrecord{
	int64_t _LSN;
	int64_t _prevLSN;
	int _xID;
	int _type;
	int _tableID;
	int _pageNumber;
	int _offset;
	int _datalength;
	int64_t _nextUndoLSN;
	char _oldimage[PAGESIZE]; // for insert and delete operation
	char _newimage[PAGESIZE]; // for insert and delete operation
	// char _oldimage[120]; // for update operation
	// char _newimage[120]; // for update operation
}LOG;

typedef struct masterLogBuffer{
	LOG *logBuffers;
	int _flushedLSN;
	int _logFd;
	int _logBufferIndex;
	int _logFlushIndex;
	bool _logging;
	off_t _lastxactLSN;
}MLOG;

MBCB manager;
MLOG logmanager;

int tableID_to_fd[11];

/*
 * name : open_db
 * argument : pathname
 * return value : if success return unique table id(fd), else return -1
 */
int open_table(char *_pathname);
/*
 * name : close_table
 * argument : table id
 * return value : if close is success return 0, else return -1
 */
int close_table(int tableID);
/*
 * name : init_db
 * argument : size of buffer
 * return value : if success, return 0. Otherwise return -1
 * do : initialize buffer control block(BCB)
 */
int init_db(int numBuf);
/*
 * name : shutdown_db
 * argument : none
 * do : flush all data from buffer and destroy allocated buffer
 * return value : if success, return 0. Otherwise return -1
 */
int shutdown_db();

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
off_t getInternalOffset(char *_page, int _offsetNum);

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
off_t getParent(char *_page);

/*
 * name : getNextFreePage
 * argument : page's offset
 * return value : An offset that input offset's next free page(int64_t)
 * comment : this function can work at both normal page and free page
 * but I recommend using it at free page
 */
off_t getNextFreePage(char *_page);

/*
 * name : getRightSibling
 * argument : page's offset
 * return value : An offset that input offset's right sibling
 */
off_t getRightSibling(char *_page);

/*
 * name : getHeaderStatus
 * argument : status number that you want to get
 * kinds of status
 *    1 : next free page offset
 *    2 : root's offset
 *    3 : number of pages in file
 * return value : the status that you want to get
 */
int64_t getHeaderStatus(int table_id, int _stat);


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

void setLeafKey(char *_page, int64_t _key, int _num);
void setLeafValue(char *_page, char *_value, int _num);
void setInternalKey(char *_page, int64_t _key, int _num);
void setInternalOffset(char *_page, off_t _offset, int _num);

/*
 * name : setIsLeaf
 * argument : page's offset, status of isleaf
 * do : change isleaf status of page.
 * 1 means leafpage, 0 means internal page or header
 */
void setIsLeaf(char *_page, bool _isLeaf);

void setParent(char *_page, int64_t _parent);

/*
 * name : setRightSibling
 * argument : page's offset, sibling's offset
 * do : change the sibling offset at the page
 */
void setRightSibling(char *_page, off_t _sibling);

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
void setHeaderStatus(int table_id, int _stat, int64_t _info);

// initialize utility function

/*
 * name : initLeaf
 * argument : page's offset
 * do
 *    setting leaf page's number of key to 0
 *    initialize leaf page's all key and value
 */
void initLeafelement(char *_pageOffset);

/*
 * name : initInternal
 * argument : page's offset
 * do
 *    setting internal page's number of key to 0
 *    initialize internal page's all key and value
 */
void initInternalelement(char *_pageOffset);

/*
 * name : initPage
 * argument : page's offset
 * do
 *    setting all page's bytes to 0
 *    insert this page to free page lists at head
 *    after this function this page is to be first of free page
 */
void initPage(int table_id, BCB *_pageOffset);

// free page management function
/*
 * name : freePageToUse
 * argument : none
 * do
 *    if free page list is empty, make 5 free pages
 *    and add it at the free page list
 * return value : free page that we can use it
 */
BCB* freePageToUse(int table_id);

// insert
/*
 * name : insert
 * argument : key, value
 * return value : if insertion is success return 0,
 *                else return none zero value
 * comment : master insert function
 */
int insert(int table_id, int64_t _key, char *_value);

/*
 * name : start_new_tree
 * argument : key, value
 * do
 *    if B+tree is not exists, make it
 * return value : if success return 0
 */
BCB* start_new_tree(int table_id, int64_t _key, char *_value);

/*
 * name : insert_into_leaf
 * argument : leaf page's offset, key, value
 * do
 *    insert key value pair into leaf page(use linear search)
 * return value : leaf page's offset that same with input
 */
BCB* insert_into_leaf(int table_id, BCB *_leaf, int64_t _key, char *_value);

/*
 * name : insert_into_leaf_after_splitting
 * argument : leaf page's offset, key, value
 * do
 *    insert key value pair into leaf page
 *    first split a full leaf page
 *    next fisrt half one insert into old,
 *    another half one insert into new
 * return value : call insert_into_parent
 */
BCB* insert_into_leaf_after_splitting(int table_id, BCB *_leaf,
	int64_t _key, char *_value);

/*
 * name : insert_into_parent
 * argument : old page's offset(left), key, new page's offset(right)
 * do
 *    insert new page into leaf's parent page
 * return value
 *    if leaf is root, call insert_into_new_root
 *    else if parent has available space, call insert_into_internal
 *    else if call insert_into_internal_after_splitting
 */
BCB* insert_into_parent(int table_id, BCB *_left, int64_t _key, BCB *_right, LOG* left_log, LOG* right_log);

/*
 * name : insert_into_new_root
 * argument : old page's offset(left), key, new page's offset(right)
 * do
 *    insert splitted pages into new root
 *    and write key into root
 * return value : new root's offset
 */
BCB* insert_into_new_root(int table_id, BCB *_left, int64_t _key, BCB *_right, LOG* left_log, LOG* right_log);

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
 * return value : none
 */
BCB* insert_into_internal(BCB *_n, int _left_index,
	int64_t _key, BCB *_right, LOG *n_log, LOG *right_log);

/*
 * name : insert_into_internal_after_splitting
 * argument : oldpage's offset, old page's index, key,
 *            new page's offset
 * do
 *    parent page is full, so split parent page
 * return value : call insert_into_parent
 */
BCB* insert_into_internal_after_splitting(int table_id, BCB *_oldPage,
  int _left_index, int64_t _key, BCB *_right, LOG *oldPage_log, LOG *right_log);

/*
 * name : cut
 * argument : page's capacity
 * return value : min number of key that page must have
 */
int cut(int length);

// find
/*
 * name : find_leaf
 * argument : key
 * return value : An offset that key is exists
 */
BCB* find_leaf(int table_id, int64_t _key);

/*
 * name : find
 * argument : key
 * return value : The value that is matched the key
 *                if not exists the key, return NULL
 */
char *find(int table_id, int64_t _key);

/*
 * name binarySearch
 * argument : page's offset, key
 * return value : index of the key
 * comment : this function is not completed
 */
int binarySearch(char *_page, int64_t _key);

// delete
/*
 * name : delete
 * argument : a key that you want to delete
 * return value : if deletion is success return 1, else return 0
 * comment : master delete function
 */
int delete(int table_id, int64_t _key);

BCB* delete_entry(int table_id, BCB *_leaf, int64_t _key, int64_t _offset);
BCB* adjust_root(int table_id, BCB *_root, LOG *root_log);
BCB* redistribute_pages(int table_id, BCB *_leaf, BCB *_neighbor,
	int neighbor_index, int k_prime_index, int k_prime, LOG *leaf_log, LOG *neighbor_log);
BCB* coalesce_pages(int table_id, BCB *_leaf, BCB *_neighbor,
	int neighbor_index, int k_prime, LOG *leaf_log, LOG *neighbor_log);
int get_neighbor_index(char *_parent, off_t _leaf);
BCB* remove_entry_from_page(BCB *_leafKey, int64_t _key, off_t _offset);

// buffer
/*
 * comment : master buffer function
 */
BCB* buffer(int table_id, off_t _pageOffset);
BCB* addPageOnBuffer(int table_id, off_t _pageOffset, bool isFree);
BCB* pageReplacement(int table_id, off_t _pageOffset, bool isFree);
void returnBuffer(BCB *_page, bool _flag);
int hashFunc(int table_id, off_t _pageOffset);
void hashDeletion(BCB *_page);
void hashInsertion(BCB *_page, int table_id, off_t _pageOffset);

// join
int join_table(int table_id_1, int table_id_2, char *_pathname);

// transaction and recovery
int begin_transaction();
int commit_transaction();
int abort_transaction();

int update(int table_id, int64_t _key, char *_value);

int64_t getPageLSN(char *_page);
void setPageLSN(char *_page, int64_t _pageLSN);

LOG* log_buffer();
void flush_log();

void recovery();
