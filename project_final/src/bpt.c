#include <stdio.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include "bpt.h"

int getNumberOfKeys(char *_page){
	int numKey;
	memcpy(&numKey, _page + 12, 4);

	return numKey;
}

bool isLeaf(char *_page){
	int leaf;
	memcpy(&leaf, _page + 8, 4);

	return leaf;
}

int64_t getInternalKey(char *_page, int _keyNum){
	int64_t key;
	memcpy(&key, _page + 128 + _keyNum * 16, 8);

	return key;
}

off_t getInternalOffset(char *_page, int _offsetNum){
	int64_t offset;
	memcpy(&offset, _page + 120 + _offsetNum * 16, 8);

	return offset;
}

int64_t getLeafKey(char *_page, int _keyNum){
	int64_t key;
	memcpy(&key, _page + 128 * (_keyNum + 1), 8);

	return key;
}

char* getLeafValue(char *_page, int _valueNum){
	char *value;
  value = calloc(1, 120);
  memcpy(value, _page + 8 + 128 * (_valueNum + 1), 120);

	return value;
}

off_t getParent(char *_page){
	off_t parentOffset;
	memcpy(&parentOffset, _page, 8);

	return parentOffset;
}

off_t getNextFreePage(char *_page){
	int64_t nextFreePageOffset;
	memcpy(&nextFreePageOffset, _page, 8);

	return nextFreePageOffset;
}

off_t getRightSibling(char *_page){
	off_t rightSibling;
	memcpy(&rightSibling, _page + 120, 8);

	return rightSibling;
}

int64_t getPageLSN(char *_page){
	int64_t pageLSN;
	memcpy(&pageLSN, _page + 24, 8);

	return pageLSN;
}

void setPageLSN(char *_page, int64_t _pageLSN){
	memcpy(_page + 24, &_pageLSN, 8);
}

void setLeafKey(char *_page, int64_t _key, int _num){
  memcpy(_page + 128 * (_num + 1), &_key, 8);
}

void setLeafValue(char *_page, char *_value, int _num){
  if(_value != NULL)
    memcpy(_page + 8 + (_num + 1) * 128, _value, 120);
}

void setInternalKey(char *_page, int64_t _key, int _num){
  memcpy(_page + 128 + _num * 16, &_key, 8);
}

void setInternalOffset(char *_page, off_t _offset, int _num){
  memcpy(_page + 120 + _num * 16, &_offset, 8);
}

void setNumberOfKeys(char *_page, int _num){
	memcpy(_page + 12, &_num, 4);
}

void setParent(char* _pageOffset, off_t _parent){
	memcpy(_pageOffset, &_parent, 8);
}

void setIsLeaf(char *_page, bool _isLeaf){
	memcpy(_page + 8, &_isLeaf, 4);
}

void setRightSibling(char *_page, off_t _sibling){
  memcpy(_page + 120, &_sibling, 8);
}

void upperNumKey(char *_page){
	int numKey;
	memcpy(&numKey, _page + 12, 4);
	numKey++;
	memcpy(_page + 12, &numKey, 4);
}

void lowerNumKey(char *_page){
	int numKey;
	memcpy(&numKey, _page + 12, 4);
	numKey--;
	memcpy(_page + 12, &numKey, 4);
}

int open_table(char *_pathname){
	if(manager._tableSize >= 10)
		return -1;
  int fd, table_id;
	if((fd = open(_pathname, O_RDWR, 0644)) < 0){
		if((fd = open(_pathname, O_CREAT|O_RDWR|O_TRUNC, 0644)) < 0){
			perror("Can't open file");
			return -1;
		}
		char tmp[2];
		strncpy(tmp, _pathname + 4, 2);
		table_id = atoi(tmp);
		tableID_to_fd[table_id] = fd;
		/*
			header page initialize
			파일이 존재하지 않으므로 파일을 생성하고
			Header Page를 초기화 한다.
			Free Page Offset(8bytes)은 PAGESIZE*2,
			Root Page Offset(8bytes)은 PAGESIZE,
			Number of pages(8bytes)는 8로 초기화 한다.
		*/
		manager.header[table_id]._free = PAGESIZE * 2;
		manager.header[table_id]._root = PAGESIZE;
		manager.header[table_id]._numberOfPages = 7;
		manager._xactNumber = 0;
		manager._tableSize++;
		pwrite(fd, &(manager.header[table_id]._free), 8, 0);
    pwrite(fd, &(manager.header[table_id]._root), 8, 8);
    pwrite(fd, &(manager.header[table_id]._numberOfPages), 8, 16);
		int64_t freep;
		int i;
		for(i = 2; i < 7; i++){
			freep = PAGESIZE * (i + 1);
      pwrite(fd, &freep, 8, PAGESIZE * i);
		}
		pwrite(fd, &i, 1, PAGESIZE * i - 1);
		return table_id;
	}
	char tmp[2];
	strncpy(tmp, _pathname + 4, 2);
	table_id = atoi(tmp);
	tableID_to_fd[table_id] = fd;

	char headStatus[24];
	pread(fd, headStatus, 24, 0);
	memcpy(&manager.header[table_id]._free, headStatus, 8);
	memcpy(&manager.header[table_id]._root, headStatus + 8, 8);
	memcpy(&manager.header[table_id]._numberOfPages, headStatus + 16, 8);
	manager._tableSize++;
	return table_id;
}

int close_table(int tableID){
	int i;
	char headStatus[24];
	memcpy(headStatus, &manager.header[tableID]._free, 8);
	memcpy(headStatus + 8, &manager.header[tableID]._root, 8);
	memcpy(headStatus + 16, &manager.header[tableID]._numberOfPages, 8);
	pwrite(tableID_to_fd[tableID], headStatus, 24, 0);

	flush_log();

	for(i = 0; i < manager._maxSize; i++){
		/*if(manager.buffers[i]._tableID == tableID &&
        manager.buffers[i]._pinCount > 0){
			break;
		}*/
		if(manager.buffers[i]._tableID == tableID &&
      manager.buffers[i]._isDirty == true){
      pwrite(tableID_to_fd[tableID], manager.buffers[i].buffer,
        4096, manager.buffers[i]._pageOffset);
      manager.buffers[i]._isDirty = false;
		}
	}
	if(i < manager._maxSize){
	  perror("There is some work left.\n");
		return -1;
	}

  fdatasync(tableID_to_fd[tableID]);

	if(close(tableID_to_fd[tableID]) < 0){
		perror("ERROR! Table is not closed");
		return -1;
	}	else{
		return 0;
	}
}

int init_db(int numBuf){
	int i;
	manager.header = (HEADER*)malloc(sizeof(HEADER)* 13);
	manager._tableSize = 0;
	manager._maxSize = numBuf;
	manager._nextRef = 0;

	logmanager._logBufferIndex = -1;
	logmanager._logFlushIndex = 0;

	logmanager.logBuffers = (LOG *)calloc(sizeof(LOG), LOG_BUFFER_NUM);
	if(logmanager.logBuffers == NULL){
		perror("memory allocation error at init_db");
		return -1;
	}

	if((logmanager._logFd = open("log", O_RDWR, 0644)) < 0){
		if((logmanager._logFd = open("log", O_CREAT|O_RDWR|O_TRUNC, 0644)) < 0){
			perror("Can't open file");
			return -1;
		}
	}
	off_t filesize;
	filesize = lseek(logmanager._logFd, 0, SEEK_END);
	logmanager.logBuffers[0]._prevLSN = filesize;
	logmanager._flushedLSN = filesize;

	for(i = 0; i < 1000; i++){
		logmanager.logBuffers[i]._LSN = -1;
		logmanager.logBuffers[i]._nextUndoLSN = -1;
	}

	logmanager._logging = false;

	for(i = 0; i < 11; i++)
		tableID_to_fd[i] = -1;

	manager.searchTable = (HASH *)calloc(sizeof(BCB *), numBuf);
	if(manager.searchTable == NULL){
		perror("memory allocation error at init_db");
		return -1;
	}

	manager.buffers = (BCB *)calloc(sizeof(BCB), numBuf);
	if(manager.buffers == NULL){
		perror("memory allocation error at init_db");
		return -1;
	}
	for(i = 0; i < numBuf; i++){
		manager.buffers[i]._tableID = -1;
		manager.buffers[i]._pageOffset = -1;
	}

	recovery();

	return 0;
}

int shutdown_db(){
	int i;
	for(i = 0; i < manager._maxSize; i++){
		/*if(manager.buffers[i]._pinCount > 0){
			break;
		}*/
	}
	if(i < manager._maxSize){
	  perror("There is some work left. Please shutdown_db after few seconds.\n");
		return -1;
	}

	free(logmanager.logBuffers);
	free(manager.buffers);
	free(manager.header);
	free(manager.searchTable);
	return 0;
}

BCB* start_new_tree(int table_id, int64_t _key, char *_value){
	BCB *root = buffer(table_id, manager.header[table_id]._root);
	LOG *root_log;

	if(logmanager._logging){
		root_log = log_buffer();
		root_log -> _type = 1;
		root_log -> _tableID = table_id;
		root_log -> _pageNumber = root -> _pageOffset / PAGESIZE;
		root_log -> _offset = root -> _pageOffset % PAGESIZE;
		root_log -> _datalength = 256;
		root_log -> _LSN = root_log -> _prevLSN + 48 + 2 * root_log -> _datalength;
		memcpy(root_log -> _oldimage, root -> buffer, 256);
	}

	// revise Root page and add (key, value) pair
	setIsLeaf(root -> buffer, 1);
	memcpy(root -> buffer + 136, _value, 120);
	memcpy(root -> buffer + 128, &_key, 8);
  setRightSibling(root -> buffer, 0);
  setParent(root -> buffer, 0);
  upperNumKey(root -> buffer);
	if(logmanager._logging){
		setPageLSN(root -> buffer, root_log -> _LSN);
		memcpy(root_log -> _newimage, root -> buffer, 256);
	}

	return root;
}

BCB* insert_into_leaf(int table_id, BCB *_leaf, int64_t _key, char *_value){
	int i, insertion_point = 0;
	int numKey;// = getNumberOfKeys(_leaf -> buffer);
	LOG *leaf_log;
	memcpy(&numKey, _leaf -> buffer + 12, 4);

	if(logmanager._logging){
		leaf_log = log_buffer();
		leaf_log -> _type = 1;
		leaf_log -> _tableID = table_id;
		leaf_log -> _pageNumber = _leaf -> _pageOffset / PAGESIZE;
		leaf_log -> _offset = _leaf -> _pageOffset % PAGESIZE;
		leaf_log -> _datalength = PAGESIZE;
		leaf_log -> _LSN = leaf_log -> _prevLSN + 48 + 2 * leaf_log -> _datalength;
		memcpy(leaf_log -> _oldimage, _leaf -> buffer, leaf_log -> _datalength);
	}

	while(insertion_point < numKey &&
		getLeafKey(_leaf -> buffer, insertion_point) < _key)
		insertion_point++;
	for(i = getNumberOfKeys(_leaf -> buffer); i > insertion_point; i--){
		memcpy(_leaf -> buffer + 128 * (i + 1), _leaf -> buffer + 128 * i, 8);
		memcpy(_leaf -> buffer + 8 + 128 * (i + 1), _leaf -> buffer + 8 + 128 * i, 120);
	}
  memcpy(_leaf -> buffer + 128 * (insertion_point + 1), &_key, 8);
	memcpy(_leaf -> buffer + 8 + (insertion_point + 1) * 128, _value, 120);
	upperNumKey(_leaf -> buffer);

	if(logmanager._logging){
		setPageLSN(_leaf -> buffer, leaf_log -> _LSN);
		memcpy(leaf_log -> _newimage, _leaf -> buffer, leaf_log -> _datalength);
	}

	return _leaf;
}

// 이 함수는 leaf page에 대해서만 작동
BCB* insert_into_leaf_after_splitting(int table_id, BCB* _leaf,
	int64_t _key, char *_value){
	int64_t *tempKeys;
	char **tempValues;
	int64_t new_key;
	int insertion_index, split, i, j, numKeyLeaf;
	BCB* _newPage;
	LOG *leaf_log, *newPage_log;

	if(logmanager._logging){
		leaf_log = log_buffer();
		leaf_log -> _type = 1;
		leaf_log -> _tableID = table_id;
		leaf_log -> _pageNumber = _leaf -> _pageOffset / PAGESIZE;
		leaf_log -> _offset = _leaf -> _pageOffset % PAGESIZE;
		leaf_log -> _datalength = PAGESIZE;
		memcpy(leaf_log -> _oldimage, _leaf -> buffer, leaf_log -> _datalength);
	}
	// 새로운 free page를 할당.

	tempKeys = (int64_t *)malloc(LEAF_ORDER * 8);
	if(tempKeys == NULL){
		perror("Temporary keys array");
		exit(1);
	}

	tempValues = (char **)malloc(LEAF_ORDER * sizeof(char *));
	if(tempValues == NULL){
		perror("Temporary values array");
		exit(1);
	}
	for(i = 0; i < LEAF_ORDER; i++)
		tempValues[i] = (char *)malloc(120);

	insertion_index = 0;
	while(insertion_index < LEAF_ORDER - 1 &&
		getLeafKey(_leaf -> buffer, insertion_index) < _key)
		insertion_index++;

	numKeyLeaf = getNumberOfKeys(_leaf -> buffer);
	for(i = 0, j = 0; i < numKeyLeaf; i++, j++){
		if(j == insertion_index) j++;
		memcpy(&tempKeys[j], _leaf -> buffer + 128 * (i + 1), 8);
		memcpy(tempValues[j], _leaf -> buffer + 8 + 128 * (i + 1), 120);
	}
	tempKeys[insertion_index] = _key;
	memcpy(tempValues[insertion_index], _value, 120);
  setNumberOfKeys(_leaf -> buffer, 0);
	split = cut(LEAF_ORDER - 1);

	for(i = 0; i < split; i++){
  	memcpy(_leaf -> buffer + 128 * (i + 1), &tempKeys[i], 8);
    memcpy(_leaf -> buffer + 8 + (i + 1) * 128, tempValues[i], 120);
		upperNumKey(_leaf -> buffer);
	}

	_newPage = buffer(table_id, -1);

	if(logmanager._logging){
		newPage_log = log_buffer();
		newPage_log -> _type = 1;
		newPage_log -> _tableID = table_id;
		newPage_log -> _pageNumber = _newPage -> _pageOffset / PAGESIZE;
		newPage_log -> _offset = _newPage -> _pageOffset % PAGESIZE;
		newPage_log -> _datalength = PAGESIZE;
		memcpy(newPage_log -> _oldimage, _newPage -> buffer, newPage_log -> _datalength);
	}

	setIsLeaf(_newPage -> buffer, true);
	setNumberOfKeys(_newPage -> buffer, 0);

	for(i = split, j = 0; i < LEAF_ORDER; i++, j++){
		memcpy(_newPage -> buffer + 128 * (j + 1), &tempKeys[i], 8);
    memcpy(_newPage -> buffer + 8 + (j + 1) * 128, tempValues[i], 120);
		upperNumKey(_newPage -> buffer);
	}

	for(i = 0; i < LEAF_ORDER; i++){
		free(tempValues[i]);
	}
	free(tempKeys);
	free(tempValues);

	off_t sibling;
	memcpy(&sibling, _leaf -> buffer + 120, 8);
	memcpy(_leaf -> buffer + 120, &(_newPage -> _pageOffset), 8);
	memcpy(_newPage -> buffer + 120, &sibling, 8);
  memcpy(_newPage -> buffer, _leaf -> buffer, 8);
	memcpy(&new_key, _newPage -> buffer + 128, 8);

	return insert_into_parent(table_id, _leaf, new_key, _newPage, leaf_log, newPage_log);
}

BCB* insert_into_parent(int table_id, BCB* _left, int64_t _key, BCB* _right,
	LOG* left_log, LOG* right_log){
	int left_index;
	LOG *parent_log;

	/*
		새로운 root가 생기는 경우
		즉, 인자인 _left가 본래 root 였던 경우이다.
	*/
 	BCB *parent = buffer(table_id, getParent(_left -> buffer));

	if(parent -> _pageOffset == 0){
    returnBuffer(parent, false);
		return insert_into_new_root(table_id, _left, _key, _right, left_log, right_log);
  }

	if(logmanager._logging){
		parent_log = log_buffer();
		parent_log -> _type = 1;
		parent_log -> _tableID = table_id;
		parent_log -> _pageNumber = parent -> _pageOffset / PAGESIZE;
		parent_log -> _offset = parent -> _pageOffset % PAGESIZE;
		parent_log -> _datalength = PAGESIZE;
		parent_log -> _LSN = parent_log -> _prevLSN + 48 + 2 * parent_log -> _datalength;
		memcpy(parent_log -> _oldimage, parent -> buffer, parent_log -> _datalength);
	}

	/* 일반 페이지에 추가되는 경우 */

	/* left node의 parent에서의 위치를 구한다. */
	left_index =
		get_left_index(parent -> buffer, _left -> _pageOffset);

	if(logmanager._logging){
		setPageLSN(_left -> buffer, left_log -> _LSN);
		memcpy(left_log -> _newimage, _left -> buffer, PAGESIZE);
	}
  returnBuffer(_left, true);

	/*
		key를 페이지에 삽입 가능한 경우
		fill factor > numberOfKey(node);
	 */
	if(getNumberOfKeys(parent -> buffer) < INTERNAL_ORDER){
    return insert_into_internal(parent, left_index, _key, _right, parent_log, right_log);
	}
	/* 해당 페이지의 fill factor가 가득 찬 경우 */
	return insert_into_internal_after_splitting(table_id, parent, left_index, _key, _right, parent_log, right_log);
}

BCB* insert_into_new_root(int table_id, BCB *_left, int64_t _key, BCB *_right,
	LOG *left_log, LOG *right_log){
	BCB *root = buffer(table_id, -1);
	LOG *root_log, *header_log;

	if(logmanager._logging){
		root_log = log_buffer();
		root_log -> _type = 1;
		root_log -> _tableID = table_id;
		root_log -> _pageNumber = root -> _pageOffset / PAGESIZE;
		root_log -> _offset = root -> _pageOffset % PAGESIZE;
		root_log -> _datalength = 144;
		root_log -> _LSN = root_log -> _prevLSN + 48 + 2 * root_log -> _datalength;
		memcpy(root_log -> _oldimage, root -> buffer, root_log -> _datalength);
	}

	setIsLeaf(root -> buffer, false);
	setNumberOfKeys(root -> buffer, 0);
	memcpy(root -> buffer + 128, &_key, 8);
  memcpy(root -> buffer + 120, &(_left -> _pageOffset), 8);
	memcpy(root -> buffer + 136, &(_right -> _pageOffset), 8);
	upperNumKey(root -> buffer);
	setParent(root -> buffer, 0);
	memcpy(_left -> buffer, &(root -> _pageOffset), 8);
	memcpy(_right -> buffer, &(root -> _pageOffset), 8);

	if(logmanager._logging){
		header_log = log_buffer();
		header_log -> _type = 1;
		header_log -> _tableID = table_id;
		header_log -> _pageNumber = 0;
		header_log -> _offset = 0;
		header_log -> _datalength = 32;
		memcpy(header_log -> _oldimage, &manager.header[table_id], header_log -> _datalength);
		manager.header[table_id]._pageLSN = header_log -> _LSN;
	}

	manager.header[table_id]._root = root -> _pageOffset;

	if(logmanager._logging){
		memcpy(header_log -> _newimage, &manager.header[table_id], header_log -> _datalength);

		setPageLSN(root -> buffer, root_log -> _LSN);
		setPageLSN(_left -> buffer, left_log -> _LSN);
		setPageLSN(_right -> buffer, right_log -> _LSN);

		memcpy(root_log -> _newimage, root -> buffer, root_log -> _datalength);
		memcpy(left_log -> _newimage, _left -> buffer, left_log -> _datalength);
		memcpy(right_log -> _newimage, _right -> buffer, right_log -> _datalength);
	}

  returnBuffer(_left, true);
  returnBuffer(_right, true);
	return root;
}

int get_left_index(char *_parent, off_t _left){
	int left_index = 0;

	while(left_index <= getNumberOfKeys(_parent) &&
		getInternalOffset(_parent, left_index) != _left)
		left_index++;
	return left_index;
}

BCB* insert_into_internal(BCB *_n, int _left_index,
	int64_t _key, BCB *_right, LOG *n_log, LOG *right_log){
	int i;
	for(i = getNumberOfKeys(_n -> buffer); i > _left_index; i--){
		memcpy(_n -> buffer + 128 + i * 16, _n -> buffer + 128 + (i - 1) * 16, 8);
		memcpy(_n -> buffer + 120 + (i + 1) * 16, _n -> buffer + 120 + i * 16, 8);
	}
	memcpy(_n -> buffer + 128 + _left_index * 16, &_key, 8);
	memcpy(_n -> buffer + 120 + (_left_index + 1) * 16, &(_right -> _pageOffset), 8);
	upperNumKey(_n -> buffer);

	if(logmanager._logging){
		setPageLSN(_n -> buffer, n_log -> _LSN);
		setPageLSN(_right -> buffer, right_log -> _LSN);

		memcpy(n_log -> _newimage, _n -> buffer, n_log -> _datalength);
		memcpy(right_log -> _newimage, _right -> buffer, right_log -> _datalength);
	}

  returnBuffer(_right, true);
	return _n;
}

BCB* insert_into_internal_after_splitting(int table_id, BCB* _oldPage, int _left_index,
	int64_t _key, BCB* _right, LOG* oldPage_log, LOG* right_log){
	int i, j, split, k_prime;
	BCB *newPage, *child;
	int64_t *tempKeys;
	off_t *tempOffsets;
	LOG *newPage_log, *child_log;

	tempKeys = malloc((INTERNAL_ORDER + 1) * sizeof(int64_t));
	if(tempKeys == NULL){
		perror("Temporary Keys array for splitting pages");
		exit(1);
	}
	tempOffsets = malloc((INTERNAL_ORDER + 2) * sizeof(off_t));
	if(tempOffsets == NULL){
		perror("Temporary values array for splitting pages");
		exit(1);
	}

	for(i = 0, j = 0; i < INTERNAL_ORDER + 1; i++, j++){
		if (j == _left_index + 1) j++;
		memcpy(&tempOffsets[j], _oldPage -> buffer + 120 + i * 16, 8);
	}

	for(i = 0, j = 0; i < INTERNAL_ORDER; i++, j++){
		if (j == _left_index) j++;
		memcpy(&tempKeys[j], _oldPage -> buffer + 128 + i * 16, 8);
	}

	tempOffsets[_left_index + 1] = _right -> _pageOffset;
	tempKeys[_left_index] = _key;

	split = cut(INTERNAL_ORDER);

  setNumberOfKeys(_oldPage -> buffer, 0);
	for(i = 0; i < split - 1; i++){
		memcpy(_oldPage -> buffer + 120 + i * 16, &tempOffsets[i], 8);
		memcpy(_oldPage -> buffer + 128 + i * 16, &tempKeys[i], 8);
		upperNumKey(_oldPage -> buffer);
	}
	memcpy(_oldPage -> buffer + 120 + i * 16, &tempOffsets[i], 8);
	k_prime = tempKeys[split - 1];

	newPage = buffer(table_id, -1);

	if(logmanager._logging){
		newPage_log = log_buffer();
		newPage_log -> _type = 1;
		newPage_log -> _tableID = table_id;
		newPage_log -> _pageNumber = newPage -> _pageOffset / PAGESIZE;
		newPage_log -> _offset = newPage -> _pageOffset % PAGESIZE;
		newPage_log -> _datalength = PAGESIZE;
		newPage_log -> _LSN = newPage_log -> _prevLSN + 48 + 2 * newPage_log -> _datalength;
		memcpy(newPage_log -> _oldimage, newPage -> buffer, newPage_log -> _datalength);
	}

  setIsLeaf(newPage -> buffer, false);
	setNumberOfKeys(newPage -> buffer, 0);
	for(++i, j = 0; i < INTERNAL_ORDER + 1; i++, j++){
		memcpy(newPage -> buffer + 120 + j * 16, &tempOffsets[i], 8);
		memcpy(newPage -> buffer + 128 + j * 16, &tempKeys[i], 8);
		upperNumKey(newPage -> buffer);
	}
	memcpy(newPage -> buffer + 120 + j * 16, &tempOffsets[i], 8);

	free(tempOffsets);
	free(tempKeys);
	memcpy(newPage -> buffer, _oldPage -> buffer, 8);

	int numKey = getNumberOfKeys(newPage -> buffer);
	for(i = 0; i <= numKey; i++){
		child = buffer(table_id, getInternalOffset(newPage -> buffer, i));

		if(logmanager._logging){
			child_log = log_buffer();
			child_log -> _type = 1;
			child_log -> _tableID = table_id;
			child_log -> _pageNumber = child -> _pageOffset / PAGESIZE;
			child_log -> _offset = child -> _pageOffset % PAGESIZE;
			child_log -> _datalength = 8;
			child_log -> _LSN = child_log -> _prevLSN + 48 + 2 * child_log -> _datalength;
			memcpy(child_log -> _oldimage, child -> buffer, child_log -> _datalength);
		}

		memcpy(child -> buffer, &(newPage -> _pageOffset), 8);

		if(logmanager._logging){
			setPageLSN(child -> buffer, child_log -> _LSN);
			memcpy(child_log -> _newimage, child -> buffer, child_log -> _datalength);
		}

    returnBuffer(child, true);
 	}

	if(logmanager._logging){
		setPageLSN(_right -> buffer, right_log -> _LSN);
		memcpy(right_log -> _newimage, _right -> buffer, right_log -> _datalength);
	}

  returnBuffer(_right, true);
	return insert_into_parent(table_id, _oldPage, k_prime, newPage, oldPage_log, newPage_log);
}

int cut(int length){
	if(length % 2 == 0)
		return length / 2;
	else
		return length/2 + 1;
}

int insert(int table_id, int64_t _key, char *_value){
	BCB *_leaf;
  char *tmp;
	/*
		이미 해당 key가 존재하면 insert하지 않고,
		-2를 return 한다.
	*/
	if((tmp = find(table_id, _key)) != NULL){
    free(tmp);
		return -2;
  }
	/*
		Root가 하나도 키를 가지고 있지 않은 경우에
		start_new_tree(int, char*)를 호출하여
		새로운 트리를 만든다.
	*/
  _leaf = buffer(table_id, manager.header[table_id]._root);
	if(getNumberOfKeys(_leaf -> buffer) == 0){
    returnBuffer(_leaf, false);
		_leaf = start_new_tree(table_id, _key, _value);
    returnBuffer(_leaf, true);
    return 0;
	} else {
    returnBuffer(_leaf, false);
  }

	/*
		이미 트리가 존재하는 경우에 대해서 기술한다.
	*/
	_leaf = find_leaf(table_id, _key);
	/*
		leaf page에 자리가 남아 있을 경우
	*/

	if (getNumberOfKeys(_leaf -> buffer) < LEAF_ORDER - 1){
		_leaf = insert_into_leaf(table_id, _leaf, _key, _value);
    returnBuffer(_leaf, true);
		return 0;
	}
	/*
		leaf page가 반드시 split 되어야 하는 경우
	*/
  _leaf = insert_into_leaf_after_splitting(table_id, _leaf, _key, _value);
	returnBuffer(_leaf, true);
  return 0;
}

BCB* find_leaf(int table_id, int64_t _key){
	int i = 0, j;
	BCB *page = buffer(table_id, manager.header[table_id]._root);
	if(getNumberOfKeys(page -> buffer) == 0)
		return page;
	while(!isLeaf(page -> buffer)){
    i = 0;
		int numKey = getNumberOfKeys(page -> buffer);
		while(i < numKey){
			if (_key >= getInternalKey(page -> buffer, i)) i++;
			else break;
		}
    returnBuffer(page, false);
		page = buffer(table_id, getInternalOffset(page -> buffer, i));
	}
	return page;
}

char *find(int table_id, int64_t _key){
	int i = 0;
	BCB *c = find_leaf(table_id, _key);
	if(getNumberOfKeys(c -> buffer) == 0){
    returnBuffer(c, false);
    return NULL;
  }
 	//i = binarySearch(c -> buffer, _key);
  int numKey = getNumberOfKeys(c -> buffer);
  for(i = 0; i < numKey; i++){
    if(getLeafKey(c -> buffer, i) == _key) break;
  }
  returnBuffer(c, false);
	//if(i == -1)
  if(i == numKey)
		return NULL;
	else
		return getLeafValue(c -> buffer, i);
}

// deletion
BCB* delete_entry(int table_id, BCB *_leaf, int64_t _key, int64_t _offset){
	int min_keys;
	off_t neighbor;
	int neighbor_index;
	int k_prime_index;
	int64_t k_prime;
	int capacity;
	LOG *leaf_log, *neighbor_log;

	if(logmanager._logging){
		leaf_log = log_buffer();
		leaf_log -> _xID = manager._xactNumber;
		leaf_log -> _type = 1;
		leaf_log -> _tableID = table_id;
		leaf_log -> _pageNumber = _leaf -> _pageOffset / PAGESIZE;
		leaf_log -> _offset = _leaf -> _pageOffset % PAGESIZE;
		leaf_log -> _datalength = PAGESIZE;
		leaf_log -> _LSN = leaf_log -> _prevLSN + 48 + 2 * leaf_log -> _datalength;
		memcpy(leaf_log -> _oldimage, _leaf -> buffer, leaf_log -> _datalength);
	}

	// leaf page에서 key와 value를 제거한다.
	_leaf = remove_entry_from_page(_leaf, _key, _offset);
	/*
		root 에서 deletion이 발생한 경우
	*/
	if(_leaf -> _pageOffset == manager.header[table_id]._root){
		return adjust_root(table_id, _leaf, leaf_log);
	}
	/*
		최소 가지고 있어야 하는 page의 개수를 지정한다.
	*/
	min_keys = isLeaf(_leaf -> buffer) ?
		cut(LEAF_ORDER - 1) : cut(INTERNAL_ORDER);

	/*
		internal page가 최소 가지고 있어야 할 page 이상을
		지운 뒤에도 가지고 있는 경우
	*/
	if(getNumberOfKeys(_leaf -> buffer) >= min_keys){
		if(logmanager._logging){
			setPageLSN(_leaf -> buffer, leaf_log -> _LSN);
			memcpy(leaf_log -> _newimage, _leaf -> buffer, leaf_log -> _datalength);
		}

    returnBuffer(_leaf, true);
		return NULL;
	}

	/*
		internal page가 최소 가지고 있어야 할 page 보다
		적게 가지게 되는 경우
		재분배 혹은 병합이 필요할 것이다.
	*/

	/*
		병합하기에 적절한 neighbor page를 찾는다.
		또한
	*/
	BCB *parent = buffer(table_id, getParent(_leaf -> buffer));

	neighbor_index = get_neighbor_index(parent -> buffer, _leaf -> _pageOffset);
	k_prime_index = neighbor_index == -1 ? 0 : neighbor_index;
	k_prime = getInternalKey(parent -> buffer, k_prime_index);

	neighbor = neighbor_index == -1 ?
		getInternalOffset(parent -> buffer, 1) :
			getInternalOffset(parent -> buffer, neighbor_index);
  returnBuffer(parent, false);

	BCB *_neighbor = buffer(table_id, neighbor);

	if(logmanager._logging){
		neighbor_log = log_buffer();
		neighbor_log -> _xID = manager._xactNumber;
		neighbor_log -> _type = 1;
		neighbor_log -> _tableID = table_id;
		neighbor_log -> _pageNumber = _neighbor -> _pageOffset / PAGESIZE;
		neighbor_log -> _offset = _neighbor -> _pageOffset % PAGESIZE;
		neighbor_log -> _datalength = PAGESIZE;
		neighbor_log -> _LSN = neighbor_log -> _prevLSN + 48 + 2 * neighbor_log -> _datalength;
		memcpy(neighbor_log -> _oldimage, _neighbor -> buffer, neighbor_log -> _datalength);
	}

	capacity = isLeaf(_leaf -> buffer) ? LEAF_ORDER : INTERNAL_ORDER;
	/* 병합 */
	if(getNumberOfKeys(_neighbor -> buffer) +
		getNumberOfKeys(_leaf -> buffer) < capacity){
		return coalesce_pages(table_id, _leaf, _neighbor, neighbor_index, k_prime, leaf_log, neighbor_log);
	}
	/* 재배치 */
	else{
		return redistribute_pages(table_id, _leaf, _neighbor, neighbor_index,
			k_prime_index, k_prime, leaf_log, neighbor_log);
	}
}

BCB* remove_entry_from_page(BCB* _leaf, int64_t _key, off_t _offset){
	int i, j, numKey;
	// key, value를 제거하고 다른 key, value를 한 칸씩 당겨온다.
	i = 0;
	numKey = getNumberOfKeys(_leaf -> buffer);
	if(isLeaf(_leaf -> buffer)){
		while(getLeafKey(_leaf -> buffer, i) != _key) i++;
		for(++i; i < numKey; i++){
      setLeafKey(_leaf -> buffer, getLeafKey(_leaf -> buffer, i), i - 1);
      char *tmp = getLeafValue(_leaf -> buffer, i);
      setLeafValue(_leaf -> buffer, tmp, i - 1);
      free(tmp);
		}
	} else {
		while(getInternalKey(_leaf -> buffer, i) != _key) i++;
		j = i;
		for(++i; i < numKey; i++)
      setInternalKey(_leaf -> buffer, getInternalKey(_leaf -> buffer, i), i - 1);
    while(getInternalOffset(_leaf -> buffer, j) != _offset) j++;
		for(++j; j < numKey + 1; j++){
      setInternalOffset(_leaf -> buffer, getInternalOffset(_leaf -> buffer, j), j - 1);
		}
	}
	lowerNumKey(_leaf -> buffer);
	// value 값을 모두 비운다.
	if(isLeaf(_leaf -> buffer)){
    setLeafKey(_leaf -> buffer, 0, getNumberOfKeys(_leaf -> buffer));
    setLeafValue(_leaf -> buffer, NULL, getNumberOfKeys(_leaf -> buffer));
  }
	else{
    setInternalKey(_leaf -> buffer, 0, getNumberOfKeys(_leaf -> buffer));
	  setInternalOffset(_leaf -> buffer, 0, getNumberOfKeys(_leaf -> buffer) + 1);
  }
	return _leaf;
}

BCB* adjust_root(int table_id, BCB *_root, LOG *root_log){
	BCB *newRoot;
	LOG *newRoot_log, *header_log;
	/*
		root가 empty가 아닐 떄
		더 이상 할 일이 없으므로 리턴한다.
	*/
	if(getNumberOfKeys(_root -> buffer) > 0){
		if(logmanager._logging){
			setPageLSN(_root -> buffer, root_log -> _LSN);
			memcpy(root_log -> _newimage, _root -> buffer, root_log -> _datalength);
		}

    returnBuffer(_root, true);
		return _root;
	}

	/*
		empty root
	*/

	// 만약 child를 가지고 있다면 그것을 new root로 한다.
	if(!isLeaf(_root -> buffer)){
		newRoot = buffer(table_id, getInternalOffset(_root -> buffer, 0));

		if(logmanager._logging){
			newRoot_log = log_buffer();
			newRoot_log -> _xID = manager._xactNumber;
			newRoot_log -> _type = 1;
			newRoot_log -> _tableID = table_id;
			newRoot_log -> _pageNumber = newRoot -> _pageOffset / PAGESIZE;
			newRoot_log -> _offset = newRoot -> _pageOffset % PAGESIZE;
			newRoot_log -> _datalength = 32;
			newRoot_log -> _LSN = newRoot_log -> _prevLSN + 48 + 2 * newRoot_log -> _datalength;
			memcpy(newRoot_log -> _oldimage, newRoot -> buffer, newRoot_log -> _datalength);

			header_log = log_buffer();
			header_log -> _type = 1;
			header_log -> _tableID = table_id;
			header_log -> _pageNumber = 0;
			header_log -> _offset = 0;
			header_log -> _datalength = 32;
			memcpy(header_log -> _oldimage, &manager.header[table_id], header_log -> _datalength);
			manager.header[table_id]._pageLSN = header_log -> _LSN;
		}
		manager.header[table_id]._root = newRoot -> _pageOffset;

		if(logmanager._logging){
			memcpy(header_log -> _newimage, &manager.header[table_id], header_log -> _datalength);
		}

		setParent(newRoot -> buffer, 0);
	} else {
	  // 만약 root가 leaf 라면 b+ tree는 빈 것이다.
    newRoot = NULL;
  }

	setParent(_root -> buffer, manager.header[table_id]._free);

	if(logmanager._logging){
		header_log = log_buffer();
		header_log -> _type = 1;
		header_log -> _tableID = table_id;
		header_log -> _pageNumber = 0;
		header_log -> _offset = 0;
		header_log -> _datalength = 32;
		memcpy(header_log -> _oldimage, &manager.header[table_id], header_log -> _datalength);
		manager.header[table_id]._pageLSN = header_log -> _LSN;
	}
	manager.header[table_id]._free = _root -> _pageOffset;

	if(logmanager._logging){
		memcpy(header_log -> _newimage, &manager.header[table_id], header_log -> _datalength);

		setPageLSN(_root -> buffer, root_log -> _LSN);
		memcpy(root_log -> _newimage, _root -> buffer, root_log -> _datalength);
	}

	returnBuffer(_root, true);
	if(newRoot != NULL){
		if(logmanager._logging){
			setPageLSN(newRoot -> buffer, newRoot_log -> _LSN);
			memcpy(newRoot_log -> _newimage, newRoot -> buffer, newRoot_log -> _datalength);
		}

    returnBuffer(newRoot, true);
	}

	return newRoot;
}

BCB* redistribute_pages(int table_id, BCB  *_leaf, BCB *_neighbor,
	int neighbor_index, int k_prime_index, int k_prime, LOG *leaf_log, LOG *neighbor_log){
	int i;
	BCB *temp;
	LOG *temp_log;
	/*
		_leafKey가 neighbor를 왼쪽에 가지고 있을 경우
	*/
	if(neighbor_index != -1){
		if(!isLeaf(_leaf -> buffer)){
      setInternalOffset(_leaf -> buffer,
        getInternalOffset(_leaf -> buffer, getNumberOfKeys(_leaf -> buffer)),
        getNumberOfKeys(_leaf -> buffer) + 1);
			for(i = getNumberOfKeys(_leaf -> buffer); i > 0; i--){
        setInternalKey(_leaf -> buffer, getInternalKey(_leaf -> buffer , i - 1), i);
        setInternalOffset(_leaf -> buffer, getInternalOffset(_leaf -> buffer, i - 1), i);
			}
		} else {
			for(i = getNumberOfKeys(_leaf -> buffer); i > 0; i--){
        setLeafKey(_leaf -> buffer, getLeafKey(_leaf -> buffer, i - 1), i);
 				char *tmp = getLeafValue(_leaf -> buffer, i - 1);
        setLeafValue(_leaf -> buffer, tmp, i);
				free(tmp);
			}
		}
		if(!isLeaf(_leaf -> buffer)){
      setInternalOffset(_leaf -> buffer,
        getInternalOffset(_neighbor -> buffer, getNumberOfKeys(_neighbor -> buffer)), 0);
			temp = buffer(table_id, getInternalOffset(_leaf -> buffer, 0));

			if(logmanager._logging){
				temp_log = log_buffer();
				temp_log -> _xID = manager._xactNumber;
				temp_log -> _type = 1;
				temp_log -> _tableID = table_id;
				temp_log -> _pageNumber = temp -> _pageOffset / PAGESIZE;
				temp_log -> _offset = temp -> _pageOffset % PAGESIZE;
				temp_log -> _datalength = 32;
				temp_log -> _LSN = temp_log -> _prevLSN + 48 + 2 * temp_log -> _datalength;
				memcpy(temp_log -> _oldimage, temp -> buffer, temp_log -> _datalength);
			}

			setParent(temp -> buffer, _leaf -> _pageOffset);

			if(logmanager._logging){
				setPageLSN(temp -> buffer, temp_log -> _LSN);
				memcpy(temp_log -> _newimage, temp -> buffer, temp_log -> _datalength);
			}

			returnBuffer(temp, true);
      setInternalOffset(_neighbor -> buffer, 0, getNumberOfKeys(_neighbor -> buffer));
      setInternalKey(_leaf -> buffer, k_prime, 0);
			temp = buffer(table_id, getParent(_leaf -> buffer));

			if(logmanager._logging){
				temp_log = log_buffer();
				temp_log -> _xID = manager._xactNumber;
				temp_log -> _type = 1;
				temp_log -> _tableID = table_id;
				temp_log -> _pageNumber = temp -> _pageOffset / PAGESIZE;
				temp_log -> _offset = temp -> _pageOffset % PAGESIZE;
				temp_log -> _datalength = PAGESIZE;
				temp_log -> _LSN = temp_log -> _prevLSN + 48 + 2 * temp_log -> _datalength;
				memcpy(temp_log -> _oldimage, temp -> buffer, temp_log -> _datalength);
			}

      setInternalKey(temp -> buffer, getInternalKey(_neighbor -> buffer,
        getNumberOfKeys(_neighbor -> buffer) - 1), k_prime_index);

			if(logmanager._logging){
				setPageLSN(temp -> buffer, temp_log -> _LSN);
				memcpy(temp_log -> _newimage, temp -> buffer, temp_log -> _datalength);
			}

      returnBuffer(temp, true);
		}
		else {
			char *tmp = getLeafValue(_neighbor -> buffer,
				getNumberOfKeys(_neighbor -> buffer) - 1);
      setLeafValue(_leaf -> buffer, tmp, 0);
			free(tmp);
      setLeafValue(_neighbor -> buffer, NULL, getNumberOfKeys(_neighbor -> buffer) - 1);
      setLeafKey(_leaf -> buffer, getLeafKey(_neighbor -> buffer,
        getNumberOfKeys(_neighbor -> buffer) - 1), 0);
			temp = buffer(table_id, getParent(_leaf -> buffer));

			if(logmanager._logging){
				temp_log = log_buffer();
				temp_log -> _xID = manager._xactNumber;
				temp_log -> _type = 1;
				temp_log -> _tableID = table_id;
				temp_log -> _pageNumber = temp -> _pageOffset / PAGESIZE;
				temp_log -> _offset = temp -> _pageOffset % PAGESIZE;
				temp_log -> _datalength = PAGESIZE;
				temp_log -> _LSN = temp_log -> _prevLSN + 48 + 2 * temp_log -> _datalength;
				memcpy(temp_log -> _oldimage, temp -> buffer, temp_log -> _datalength);
			}

      setInternalKey(temp -> buffer, getLeafKey(_leaf -> buffer, 0), k_prime_index);

			if(logmanager._logging){
				setPageLSN(temp -> buffer, temp_log -> _LSN);
				memcpy(temp_log -> _newimage, temp -> buffer, temp_log -> _datalength);
			}
      returnBuffer(temp, true);
		}
	}

	else {
		if(isLeaf(_leaf -> buffer)){
      setLeafKey(_leaf -> buffer, getLeafKey(_neighbor -> buffer, 0),
        getNumberOfKeys(_leaf -> buffer));
			char *tmp = getLeafValue(_neighbor -> buffer, 0);
      setLeafValue(_leaf -> buffer, tmp, getNumberOfKeys(_leaf -> buffer));
      free(tmp);
      temp = buffer(table_id, getParent(_leaf -> buffer));

			if(logmanager._logging){
				temp_log = log_buffer();
				temp_log -> _xID = manager._xactNumber;
				temp_log -> _type = 1;
				temp_log -> _tableID = table_id;
				temp_log -> _pageNumber = temp -> _pageOffset / PAGESIZE;
				temp_log -> _offset = temp -> _pageOffset % PAGESIZE;
				temp_log -> _datalength = PAGESIZE;
				temp_log -> _LSN = temp_log -> _prevLSN + 48 + 2 * temp_log -> _datalength;
				memcpy(temp_log -> _oldimage, temp -> buffer, temp_log -> _datalength);
			}

      setInternalKey(temp -> buffer, getLeafKey(_neighbor -> buffer, 1), k_prime_index);

			if(logmanager._logging){
				setPageLSN(temp -> buffer, temp_log -> _LSN);
				memcpy(temp_log -> _newimage, temp -> buffer, temp_log -> _datalength);
			}

      returnBuffer(temp, true);
		}
		else {
      setInternalKey(_leaf -> buffer, k_prime, getNumberOfKeys(_leaf -> buffer));
      setInternalOffset(_leaf -> buffer, getInternalOffset(_neighbor -> buffer, 0),
        getNumberOfKeys(_leaf -> buffer) + 1);
			temp = buffer(table_id, getInternalOffset(_leaf -> buffer,
				getNumberOfKeys(_leaf -> buffer) + 1));

			if(logmanager._logging){
				temp_log = log_buffer();
				temp_log -> _xID = manager._xactNumber;
				temp_log -> _type = 1;
				temp_log -> _tableID = table_id;
				temp_log -> _pageNumber = temp -> _pageOffset / PAGESIZE;
				temp_log -> _offset = temp -> _pageOffset % PAGESIZE;
				temp_log -> _datalength = 32;
				temp_log -> _LSN = temp_log -> _prevLSN + 48 + 2 * temp_log -> _datalength;
				memcpy(temp_log -> _oldimage, temp -> buffer, temp_log -> _datalength);
			}

			setParent(temp -> buffer, _leaf -> _pageOffset);

			if(logmanager._logging){
				setPageLSN(temp -> buffer, temp_log -> _LSN);
				memcpy(temp_log -> _newimage, temp -> buffer, temp_log -> _datalength);
			}

      returnBuffer(temp, true);
			temp = buffer(table_id, getParent(_leaf -> buffer));

			if(logmanager._logging){
				temp_log = log_buffer();
				temp_log -> _xID = manager._xactNumber;
				temp_log -> _type = 1;
				temp_log -> _tableID = table_id;
				temp_log -> _pageNumber = temp -> _pageOffset / PAGESIZE;
				temp_log -> _offset = temp -> _pageOffset % PAGESIZE;
				temp_log -> _datalength = PAGESIZE;
				temp_log -> _LSN = temp_log -> _prevLSN + 48 + 2 * temp_log -> _datalength;
				memcpy(temp_log -> _oldimage, temp -> buffer, temp_log -> _datalength);
			}

      setInternalKey(temp -> buffer, getInternalKey(_neighbor -> buffer, 0), k_prime_index);

			if(logmanager._logging){
				setPageLSN(temp -> buffer, temp_log -> _LSN);
				memcpy(temp_log -> _newimage, temp -> buffer, temp_log -> _datalength);
			}

      returnBuffer(temp, true);
		}
		if(!isLeaf(_neighbor -> buffer)){
			for(i = 0; i < getNumberOfKeys(_neighbor -> buffer) - 1; i++){
        setInternalKey(_neighbor -> buffer, getInternalKey(_neighbor -> buffer, i + 1), i);
        setInternalOffset(_neighbor -> buffer,
          getInternalOffset(_neighbor -> buffer, i + 1), i);
			}
		}
		else {
			for(i = 0; i < getNumberOfKeys(_neighbor -> buffer) - 1; i++){
        setLeafKey(_neighbor -> buffer, getLeafKey(_neighbor -> buffer, i + 1), i);
				char *tmp = getLeafValue(_neighbor -> buffer, i + 1);
        setLeafValue(_neighbor -> buffer, tmp, i);
				free(tmp);
			}
		}
		if(!isLeaf(_leaf -> buffer))
      setInternalOffset(_neighbor -> buffer, getInternalOffset(_neighbor -> buffer, i + 1), i);
	}

	upperNumKey(_leaf -> buffer);
	lowerNumKey(_neighbor -> buffer);

	if(logmanager._logging){
		setPageLSN(_leaf -> buffer, leaf_log -> _LSN);
		setPageLSN(_neighbor -> buffer, neighbor_log -> _LSN);
		memcpy(leaf_log -> _newimage, _leaf -> buffer, leaf_log -> _datalength);
		memcpy(neighbor_log -> _newimage, _neighbor -> buffer, neighbor_log -> _datalength);
	}

  returnBuffer(_leaf, true);
  returnBuffer(_neighbor, true);
	return NULL;
}

BCB* coalesce_pages(int table_id, BCB *_leaf, BCB *_neighbor,
	int neighbor_index, int k_prime, LOG *leaf_log, LOG *neighbor_log){
	int i, j, neighbor_insertion_index, leafKey_end;
	BCB *tmp;
	LOG *tmp_log, *header_log;

	/*
		_leafKey가 극단적으로 왼쪽에 있고 바로 옆에 neighbor가
		존재하면 둘의 위치를 바꿔준다.
	*/

	if(neighbor_index == -1){
		tmp = _leaf;
		_leaf = _neighbor;
		_neighbor = tmp;
	}

	/*
		_leafKey로 부터 key와 value를 copying 하기 위한
		시작 index를 설정한다.
	*/
	neighbor_insertion_index = getNumberOfKeys(_neighbor -> buffer);

	/*
		leaf가 하나도 없는 page.
	*/
	if(!isLeaf(_leaf -> buffer)){
    setInternalKey(_neighbor -> buffer, k_prime, neighbor_insertion_index);
		upperNumKey(_neighbor -> buffer);

		leafKey_end = getNumberOfKeys(_leaf -> buffer);

		for(i = neighbor_insertion_index + 1, j = 0; j < leafKey_end;
			i++, j++){
      setInternalKey(_neighbor -> buffer, getInternalKey(_leaf -> buffer, j), i);
      setInternalOffset(_neighbor -> buffer, getInternalOffset(_leaf -> buffer, j), i);
			upperNumKey(_neighbor -> buffer);
			lowerNumKey(_leaf -> buffer);
		}
		/*
			offset의 수는 항상 key보다 하나 많아야 한다.
		*/
    setInternalOffset(_neighbor -> buffer, getInternalOffset(_leaf -> buffer, j), i);

		/*
			모든 자식들은 이제 반드시 같은 parent를 가리켜야 한다.
		*/

		for(i = 0; i < getNumberOfKeys(_neighbor -> buffer) + 1; i++){
			tmp = buffer(table_id, getInternalOffset(_neighbor -> buffer, i));

			if(logmanager._logging){
				tmp_log = log_buffer();
				tmp_log -> _xID = manager._xactNumber;
				tmp_log -> _type = 1;
				tmp_log -> _tableID = table_id;
				tmp_log -> _pageNumber = tmp -> _pageOffset / PAGESIZE;
				tmp_log -> _offset = tmp -> _pageOffset % PAGESIZE;
				tmp_log -> _datalength = 32;
				tmp_log -> _LSN = tmp_log -> _prevLSN + 48 + 2 * tmp_log -> _datalength;
				memcpy(tmp_log -> _oldimage, tmp -> buffer, tmp_log -> _datalength);
			}

			setParent(tmp -> buffer, _neighbor -> _pageOffset);

			if(logmanager._logging){
				setPageLSN(tmp -> buffer, tmp_log -> _LSN);
				memcpy(tmp_log -> _newimage, tmp -> buffer, tmp_log -> _datalength);
			}

      returnBuffer(tmp, true);
		}
	}
	/*
		leaf page의 경우 병합 방법
	*/
	else {
		for(i = neighbor_insertion_index, j = 0;
				j < getNumberOfKeys(_leaf -> buffer); i++, j++){
      setLeafKey(_neighbor -> buffer, getLeafKey(_leaf -> buffer, j), i);
      char *tmp = getLeafValue(_leaf -> buffer, j);
      setLeafValue(_neighbor -> buffer, tmp, i);
      free(tmp);
			upperNumKey(_neighbor -> buffer);
		}
    setRightSibling(_neighbor -> buffer, getRightSibling(_leaf -> buffer));
	}
	BCB *parent = buffer(table_id, getParent(_leaf -> buffer));
	BCB *k = delete_entry(table_id, parent, k_prime, _leaf -> _pageOffset);
	setParent(_leaf -> buffer, manager.header[table_id]._free);

	if(logmanager._logging){
		header_log = log_buffer();
		header_log -> _type = 1;
		header_log -> _tableID = table_id;
		header_log -> _pageNumber = 0;
		header_log -> _offset = 0;
		header_log -> _datalength = 32;
		memcpy(header_log -> _oldimage, &manager.header[table_id], header_log -> _datalength);
		manager.header[table_id]._pageLSN = header_log -> _LSN;
	}

	manager.header[table_id]._free = _leaf -> _pageOffset;

	if(logmanager._logging){
		memcpy(header_log -> _newimage, &manager.header[table_id], header_log -> _datalength);

		setPageLSN(_leaf -> buffer, leaf_log -> _LSN);
		setPageLSN(_neighbor -> buffer, neighbor_log -> _LSN);

		memcpy(leaf_log -> _newimage, _leaf -> buffer, leaf_log -> _datalength);
		memcpy(neighbor_log -> _newimage, _neighbor -> buffer, neighbor_log -> _datalength);
	}

	returnBuffer(_leaf, true);
  returnBuffer(_neighbor, true);
	return k;
}

int get_neighbor_index(char *_parent, off_t _leaf){
	int i;
	/*
		parent가 _leafKey를 가리키고 있는 바로 앞의
		키의 인덱스를 가져온다.
	*/
	int numKey = getNumberOfKeys(_parent);
	for(i = 0; i <= numKey; i++)
		if(getInternalOffset(_parent, i) == _leaf)
			return i - 1;

  // 만약 _leafKey가 가장 왼쪽의 page라면 -1을 리턴한다.
	return -1;
}

/*
	delete에 성공하면 0을 리턴, 실패하면 -1을 리턴한다.
*/
int delete(int table_id, int64_t _key){
	BCB *_leaf;
	char *_value;

	_value = find(table_id, _key);
	_leaf = find_leaf(table_id, _key);
	if(_value != NULL && _leaf != NULL){
		delete_entry(table_id, _leaf, _key, 0);
    free(_value);
		return 0;
	}
	return -1;
}

// master buffer manager function
BCB* buffer(int table_id, off_t _pageOffset){
  if(_pageOffset == -1)
    return freePageToUse(table_id);
	BCB *page = manager.searchTable[hashFunc(table_id, _pageOffset)].head;
	while(page != NULL){
		if(page -> _tableID == table_id && page -> _pageOffset == _pageOffset){
			page -> _refbit = true;
			page -> _pinCount++;
			return page;
		}
		page = page -> _next;
	}
	return pageReplacement(table_id, _pageOffset, false);
}

/*
 * using clock LRU algorithm
 */
BCB *pageReplacement(int table_id, off_t _pageOffset, bool isFree){
  BCB *current;
  int rpPageID = -1;
	int clk_hand = manager._nextRef;
	int maxSize = manager._maxSize;
  while(rpPageID == -1){
    current = &(manager.buffers[clk_hand]);
    if(current -> _pinCount == 0 && current -> _refbit == false) {
			flush_log();
      if(current -> _isDirty == true){
        pwrite(tableID_to_fd[current -> _tableID], current -> buffer, 4096, current -> _pageOffset);
      }
			hashDeletion(current);
			if(isFree == false)
				pread(tableID_to_fd[table_id], current -> buffer, 4096, _pageOffset);
			hashInsertion(current, table_id, _pageOffset);
			rpPageID = clk_hand;
    } else if (current -> _pinCount == 0 && current -> _refbit == true){
      current -> _refbit = false;
    }
    clk_hand = (clk_hand + 1) % maxSize;
  }
  manager._nextRef = clk_hand;
  return current;
}

void returnBuffer(BCB *_page, bool _flag){
  _page -> _pinCount--;
  if(_flag == true)
      _page -> _isDirty = true;
}

BCB* freePageToUse(int table_id){
	LOG *header_log;
	off_t freePageOffset = manager.header[table_id]._free;
  if(freePageOffset >= manager.header[table_id]._numberOfPages * PAGESIZE){
		if(logmanager._logging){
			header_log = log_buffer();
			header_log -> _type = 1;
			header_log -> _tableID = table_id;
			header_log -> _pageNumber = 0;
			header_log -> _offset = 0;
			header_log -> _datalength = 32;
			memcpy(header_log -> _oldimage, &manager.header[table_id], header_log -> _datalength);
			manager.header[table_id]._pageLSN = header_log -> _LSN;
		}

		manager.header[table_id]._free += PAGESIZE;
		manager.header[table_id]._numberOfPages++;

		if(logmanager._logging){
			memcpy(header_log -> _newimage, &manager.header[table_id], header_log -> _datalength);
		}

		return pageReplacement(table_id, freePageOffset, true);
	}
	else {
		BCB* temp = pageReplacement(table_id, freePageOffset, false);

		if(logmanager._logging){
			LOG *header_log = log_buffer();
			header_log -> _type = 1;
			header_log -> _tableID = table_id;
			header_log -> _pageNumber = 0;
			header_log -> _offset = 0;
			header_log -> _datalength = 32;
			memcpy(header_log -> _oldimage, &manager.header[table_id], header_log -> _datalength);
			manager.header[table_id]._pageLSN = header_log -> _LSN;
		}

		manager.header[table_id]._free = getParent(temp -> buffer);

		if(logmanager._logging){
			memcpy(header_log -> _newimage, &manager.header[table_id], header_log -> _datalength);
		}

		return temp;
  }
}

void hashDeletion(BCB *_page){
	if(_page -> _pageOffset == -1)
		return;
	int hash = hashFunc(_page -> _tableID, _page -> _pageOffset);
	BCB *head = manager.searchTable[hash].head;
	if(head == _page)
		manager.searchTable[hash].head = _page -> _next;
	if(_page -> _next != NULL)
		_page -> _next -> _prev = _page -> _prev;
	if(_page -> _prev != NULL)
		_page -> _prev -> _next = _page -> _next;
	_page -> _prev = NULL;
	_page -> _next = NULL;
}

void hashInsertion(BCB *_page, int table_id, off_t _pageOffset){
	int hash = hashFunc(table_id, _pageOffset);
	BCB *head = manager.searchTable[hash].head;
	_page -> _pageOffset = _pageOffset;
	_page -> _tableID = table_id;
	_page -> _isDirty = false;
	_page -> _refbit = true;
	_page -> _pinCount = 1;

	_page -> _next = head;
	if(head != NULL)
		head -> _prev = _page;
	manager.searchTable[hash].head = _page;
}

int hashFunc(int table_id, off_t _pageOffset){
  return (((_pageOffset / 4096) + table_id) % manager._maxSize);
}

int join_table(int table_id_1, int table_id_2, char *_pathname){
	FILE *fp;
	BCB *output;
	BCB *table_1, *table_2;

	// open output file for join result
	if((fp = fopen(_pathname, "w")) == NULL){
		perror("fopen error");
		exit(1);
	}

	// get buffer for sort-merge join
	output = pageReplacement(0, 0, true);
	table_1 = buffer(table_id_1, manager.header[table_id_1]._root);
	table_2 = buffer(table_id_2, manager.header[table_id_2]._root);

	// move to left-most leaf
	while(!isLeaf(table_1 -> buffer)){
		returnBuffer(table_1, false);
		table_1 = buffer(table_id_1, getInternalOffset(table_1->buffer, 0));
	}
	while(!isLeaf(table_2 -> buffer)){
		returnBuffer(table_2, false);
		table_2 = buffer(table_id_2, getInternalOffset(table_2->buffer, 0));
	}

	int index_table_1 = 0; // index for table1
	int index_table_2 = 0; // index for table2
	int length = 0; // index for output file

	while(1){
		// if scan all records of one page,
		// 	 move to next page with buffer manager
		// if it is last page of table
		//   end loop and return buffer
		if(index_table_1 == getNumberOfKeys(table_1 -> buffer)){
			off_t rightSiblingOffset = getRightSibling(table_1 -> buffer);
			returnBuffer(table_1, false);
			if(rightSiblingOffset == 0){
				returnBuffer(table_2, false);
				break;
			}
			table_1 = buffer(table_id_1, rightSiblingOffset);
			index_table_1 = 0;
			continue;
		}
		if(index_table_2 == getNumberOfKeys(table_2 -> buffer)){
			off_t rightSiblingOffset = getRightSibling(table_2 -> buffer);
			returnBuffer(table_2, false);
			if(rightSiblingOffset == 0){
				returnBuffer(table_1, false);
				break;
			}
			table_2 = buffer(table_id_2, rightSiblingOffset);
			index_table_2 = 0;
			continue;
		}

		// do sort-merge join
		// if equivalent record is found,
		//   write it to output buffer with result format
		// then when output buffer is full,
		//   write output buffer to output file
		if(getLeafKey(table_1 -> buffer, index_table_1)
			< getLeafKey(table_2 -> buffer, index_table_2))
			index_table_1++;
		else if(getLeafKey(table_1 -> buffer, index_table_1)
			> getLeafKey(table_2 -> buffer, index_table_2))
			index_table_2++;
		else{
			if(length > (PAGESIZE - 1 - 256)){
				fwrite(output -> buffer, 1, length, fp);
				length = 0;
			}
			length += sprintf(output -> buffer + length, "%lld,%s,%lld,%s\n",
				getLeafKey(table_1 -> buffer, index_table_1),
		  	getLeafValue(table_1 -> buffer, index_table_1),
				getLeafKey(table_2 -> buffer, index_table_2),
			  getLeafValue(table_2 -> buffer, index_table_2));
			index_table_1++;
			index_table_2++;
		}
	}

	// write remained data which is not written because it's not full
	//   then return output buffer
	fwrite(output->buffer, 1, length, fp);
	returnBuffer(output, false);

	fclose(fp);

	return 0;
}

int begin_transaction(){
	manager._xactNumber++;
	logmanager._logging = true;

	LOG *begin_log = log_buffer();

	begin_log -> _type = 0;
	begin_log -> _tableID = -1;
	begin_log -> _pageNumber = -1;
	begin_log -> _offset = -1;
	begin_log -> _datalength = 0;
	begin_log -> _LSN = begin_log -> _prevLSN + 48;

	return 0;
}

int commit_transaction(){
	LOG *commit_log = log_buffer();

	commit_log -> _type = 2;
	commit_log -> _tableID = -1;
	commit_log -> _pageNumber = -1;
	commit_log -> _offset = -1;
	commit_log -> _datalength = 0;
	commit_log -> _LSN = commit_log -> _prevLSN + 48;

	logmanager._logging = false;
	flush_log();

	return 0;
}

int abort_transaction(){
	LOG *abort_log = log_buffer();

	abort_log -> _type = 3;
	abort_log -> _tableID = -1;
	abort_log -> _pageNumber = -1;
	abort_log -> _offset = -1;
	abort_log -> _datalength = 0;
	abort_log -> _LSN = abort_log -> _prevLSN + 48;

	logmanager._logging = false;

	flush_log();

	LOG *aborted_log = calloc(sizeof(LOG), 1);
	int flushCount = 0;
	off_t currentLSN;
	off_t nextUndoLSN;
	do{
		pread(logmanager._logFd, aborted_log, currentLSN - nextUndoLSN, nextUndoLSN);

		LOG *undo_log = log_buffer();
		BCB *undo_page = buffer(aborted_log -> _tableID, aborted_log -> _pageNumber * PAGESIZE);

		undo_log -> _type = 4;
		undo_log -> _xID = aborted_log -> _xID;
		undo_log -> _tableID = aborted_log -> _tableID;
		undo_log -> _pageNumber = aborted_log -> _pageNumber;
		undo_log -> _offset = aborted_log -> _offset;
		undo_log -> _datalength = aborted_log -> _datalength;
		undo_log -> _nextUndoLSN = aborted_log -> _nextUndoLSN;
		undo_log -> _LSN = undo_log -> _prevLSN + 48 + 2 * undo_log -> _datalength;
		memcpy(undo_log -> _newimage, aborted_log -> _oldimage, undo_log -> _datalength);
		memcpy(undo_log -> _oldimage, aborted_log -> _oldimage +
			undo_log -> _datalength, undo_log -> _datalength);
		memcpy(undo_page -> buffer + aborted_log -> _offset,
			aborted_log -> _oldimage, undo_log -> _datalength);
		setPageLSN(undo_page -> buffer, undo_log -> _LSN);
		returnBuffer(undo_page, true);

		flushCount++;
		nextUndoLSN = aborted_log -> _nextUndoLSN;
		currentLSN = aborted_log -> _prevLSN;

		if(flushCount == 5){
			int i;
			for(i = 0; i < manager._maxSize; i++){
				flush_log();
				if(manager.buffers[i]._isDirty == true){
		      pwrite(tableID_to_fd[manager.buffers[i]._tableID],
						manager.buffers[i].buffer, 4096, manager.buffers[i]._pageOffset);
					fdatasync(tableID_to_fd[manager.buffers[i]._tableID]);
	      manager.buffers[i]._isDirty = false;
				}
			}
			flushCount = 0;
		}
	}while(nextUndoLSN >= 0);

	flush_log();

	return 0;
}

int update(int table_id, int64_t _key, char *_value){
	if(strlen(_value) > 120)
		return -1;

	BCB *leaf = find_leaf(table_id, _key);
	LOG *leaf_log;

	if(logmanager._logging){
		leaf_log = log_buffer();
		leaf_log -> _xID = manager._xactNumber;
		leaf_log -> _type = 1;
		leaf_log -> _tableID = table_id;
		leaf_log -> _pageNumber = leaf -> _pageOffset / PAGESIZE;
		leaf_log -> _datalength = 120;
		leaf_log -> _LSN = leaf_log -> _prevLSN + 48 + 2 * leaf_log -> _datalength;
	}

	int i;
	for(i = 0; i < getNumberOfKeys(leaf -> buffer); i++){
		if(getLeafKey(leaf -> buffer, i) == _key)
			break;
	}

	if(i == getNumberOfKeys(leaf -> buffer)){
		perror("can't find value");
		return -1;
	}
	else{
		if(logmanager._logging){
			leaf_log -> _offset = 128 + 8 + i * 128;
			memcpy(leaf_log -> _oldimage, leaf -> buffer + 128 + 8 + i * 128, 120);
		}

		setLeafValue(leaf -> buffer, _value, i);

		if(logmanager._logging){
			setPageLSN(leaf -> buffer, leaf_log -> _LSN);
			memcpy(leaf_log -> _newimage, leaf -> buffer + 128 + 8 + i * 128, 120);
		}

		returnBuffer(leaf, true);
		return 0;
	}
}

// log buffer manager
LOG* log_buffer(){
	logmanager._logBufferIndex++;
	/*
		if log buffer array is full,
		flush log buffer and start from first
	*/
	if(logmanager._logBufferIndex == LOG_BUFFER_NUM){
		flush_log();
		logmanager._logBufferIndex = 0;
		logmanager.logBuffers[0]._prevLSN = logmanager.logBuffers[LOG_BUFFER_NUM - 1]._LSN;
	}

	/*
		set prevLSN before using
	*/
	if(logmanager._logBufferIndex != 0){
		logmanager.logBuffers[logmanager._logBufferIndex]._prevLSN =
			logmanager.logBuffers[logmanager._logBufferIndex - 1]._LSN;
	}

	/*
		set nextUndoLSN(using for undo)
		set transaction id
	*/
	if(logmanager._logBufferIndex != 0){
		if(logmanager.logBuffers[logmanager._logBufferIndex - 1]._type == 0)
			logmanager.logBuffers[logmanager._logBufferIndex]._nextUndoLSN = -1;
		else
			logmanager.logBuffers[logmanager._logBufferIndex]._nextUndoLSN
			= logmanager.logBuffers[logmanager._logBufferIndex - 1]._prevLSN;
	}
	else{
		if(logmanager.logBuffers[LOG_BUFFER_NUM - 1]._type == 0)
			logmanager.logBuffers[logmanager._logBufferIndex]._nextUndoLSN = -1;
		else
			logmanager.logBuffers[logmanager._logBufferIndex]._nextUndoLSN
			= logmanager.logBuffers[LOG_BUFFER_NUM - 1]._prevLSN;
	}
	logmanager.logBuffers[logmanager._logBufferIndex]._xID = manager._xactNumber;

	return &logmanager.logBuffers[logmanager._logBufferIndex];
}

// flush log buffer function
void flush_log(){
	/*
		write buffer to disk(file)
		incresing index until LSN > _flushedLSN
	*/
	while(logmanager.logBuffers[logmanager._logFlushIndex]._LSN
		> logmanager._flushedLSN){
		pwrite(logmanager._logFd, &logmanager.logBuffers[logmanager._logFlushIndex],
			48, logmanager.logBuffers[logmanager._logFlushIndex]._prevLSN);
		if(logmanager.logBuffers[logmanager._logFlushIndex]._type == 1 ||
			logmanager.logBuffers[logmanager._logFlushIndex]._type == 4){
			pwrite(logmanager._logFd, logmanager.logBuffers[logmanager._logFlushIndex]._oldimage,
				logmanager.logBuffers[logmanager._logFlushIndex]._datalength,
				logmanager.logBuffers[logmanager._logFlushIndex]._prevLSN + 48);
			pwrite(logmanager._logFd, logmanager.logBuffers[logmanager._logFlushIndex]._newimage,
				logmanager.logBuffers[logmanager._logFlushIndex]._datalength,
				logmanager.logBuffers[logmanager._logFlushIndex]._prevLSN + 48
				+ logmanager.logBuffers[logmanager._logFlushIndex]._datalength);
		}
		logmanager._flushedLSN = logmanager.logBuffers[logmanager._logFlushIndex]._LSN;
		logmanager._logFlushIndex = (logmanager._logFlushIndex + 1) % LOG_BUFFER_NUM;
	}

	fdatasync(logmanager._logFd);
}

// recovery function
void recovery(){
	/*
		redo phase
	*/
	off_t filesize;
	filesize = lseek(logmanager._logFd, 0, SEEK_END);

	LOG *redo_log = calloc(sizeof(LOG), 1); // log structure for read log file

	/*
		in-memory variables for recovery
	*/
	off_t LSN = 0;
	off_t prevLSN = 0;
	bool xactEnd = true; // check whether transaction is commited
	off_t lastCLE = 0;
	off_t nextUndoLSN = -1;
	int flushCount = 0; // for periodical buffer flush
	int i = 0;

	/*
		redo-history
		redo until reach end of file
	*/
	while(prevLSN != filesize){
		pread(logmanager._logFd, &LSN, 8, prevLSN);
		pread(logmanager._logFd, redo_log, LSN - prevLSN, prevLSN);

		switch (redo_log -> _type) {
			case 0: // begin transaction
				nextUndoLSN = -1;
				xactEnd = false;
				break;
			case 4: // compensation log entry
				lastCLE = prevLSN;
			case 1: // update transaction
				if(tableID_to_fd[redo_log -> _tableID] == -1){
					char pathname[7];
					sprintf(pathname, "DATA%d", redo_log -> _tableID);
					open_table(pathname);
				}
				logmanager._lastxactLSN = prevLSN;
				if(redo_log -> _type == 1) {
					pwrite(logmanager._logFd, &nextUndoLSN, 8, prevLSN + 40);
					nextUndoLSN = prevLSN;
				}
				BCB *redo_page = buffer(redo_log -> _tableID, redo_log -> _pageNumber * PAGESIZE);
				if(getPageLSN(redo_page -> buffer) >= LSN){
					returnBuffer(redo_page, false);
					break;
				}

				memcpy(redo_page -> buffer + redo_log -> _offset,
					redo_log -> _oldimage + redo_log -> _datalength,
					redo_log -> _datalength);
				setPageLSN(redo_page -> buffer, redo_log -> _LSN);
				returnBuffer(redo_page, true);
				flushCount++;
				break;
			case 2: // commit transaction
				xactEnd = true;
				break;
			case 3: // abort transaction
				xactEnd = true;
				break;
			default:
				perror("log error");
				exit(-1);
		}
		prevLSN = redo_log -> _LSN;

		/*
			periodical flush using flushCount
		*/
		if(flushCount == 5){
			for(i = 0; i < manager._maxSize; i++){
				if(manager.buffers[i]._isDirty == true){
		      pwrite(tableID_to_fd[manager.buffers[i]._tableID],
						manager.buffers[i].buffer, 4096, manager.buffers[i]._pageOffset);
		      manager.buffers[i]._isDirty = false;
				}
			}
			flushCount = 0;
		}
	}

	/*
		undo phase
	*/

	/*
		first undo uncommited xact
	*/

	if(xactEnd == false){
		off_t uncommited = logmanager._lastxactLSN;
		do{
			pread(logmanager._logFd, &LSN, 8, uncommited);
			pread(logmanager._logFd, redo_log, LSN - uncommited, uncommited);

			LOG *undo_log = log_buffer();
			BCB *undo_page = buffer(redo_log -> _tableID, redo_log -> _pageNumber * PAGESIZE);
			uncommited = redo_log -> _nextUndoLSN;

			if(getPageLSN(undo_page -> buffer) < redo_log -> _LSN)
				continue;

			undo_log -> _type = 4;
			undo_log -> _xID = redo_log -> _xID;
			undo_log -> _tableID = redo_log -> _tableID;
			undo_log -> _pageNumber = redo_log -> _pageNumber;
			undo_log -> _offset = redo_log -> _offset;
			undo_log -> _datalength = redo_log -> _datalength;
			undo_log -> _nextUndoLSN = redo_log -> _nextUndoLSN;
			undo_log -> _LSN = undo_log -> _prevLSN + 48 + 2 * undo_log -> _datalength;
			memcpy(undo_log -> _newimage, redo_log -> _oldimage, undo_log -> _datalength);
			memcpy(undo_log -> _oldimage, redo_log -> _oldimage +
				undo_log -> _datalength, undo_log -> _datalength);
			memcpy(undo_page -> buffer + redo_log -> _offset,
				redo_log -> _oldimage, undo_log -> _datalength);
			setPageLSN(undo_page -> buffer, undo_log -> _LSN);
			returnBuffer(undo_page, true);

			flushCount++;

			if(flushCount == 5){
				for(i = 0; i < manager._maxSize; i++){
					flush_log();
					if(manager.buffers[i]._isDirty == true){
			      pwrite(tableID_to_fd[manager.buffers[i]._tableID],
							manager.buffers[i].buffer, 4096, manager.buffers[i]._pageOffset);
						fdatasync(tableID_to_fd[manager.buffers[i]._tableID]);
			      manager.buffers[i]._isDirty = false;
					}
				}
				flushCount = 0;
			}
		}while(uncommited >= 0);
	}

	/*
		if CLE exists, read nextUndoLSN
	*/
	if(lastCLE > 0){
		pread(logmanager._logFd, &lastCLE, 8, lastCLE + 40);
	}

	/*
		do undo following nextUndoLSN
	*/
	if(lastCLE > 0 && lastCLE <= filesize){
		do{
			// printf("lastCLE : %lld\n", lastCLE);
			pread(logmanager._logFd, &LSN, 8, lastCLE);
			pread(logmanager._logFd, redo_log, LSN - lastCLE, lastCLE);

			LOG *undo_log = log_buffer();
			BCB *undo_page = buffer(redo_log -> _tableID, redo_log -> _pageNumber * PAGESIZE);

			undo_log -> _type = 4;
			undo_log -> _xID = redo_log -> _xID;
			undo_log -> _tableID = redo_log -> _tableID;
			undo_log -> _pageNumber = redo_log -> _pageNumber;
			undo_log -> _offset = redo_log -> _offset;
			undo_log -> _datalength = redo_log -> _datalength;
			undo_log -> _nextUndoLSN = redo_log -> _nextUndoLSN;
			undo_log -> _LSN = undo_log -> _prevLSN + 48 + 2 * undo_log -> _datalength;
			memcpy(undo_log -> _newimage, redo_log -> _oldimage, undo_log -> _datalength);
			memcpy(undo_log -> _oldimage, redo_log -> _oldimage +
				undo_log -> _datalength, undo_log -> _datalength);
			memcpy(undo_page -> buffer + redo_log -> _offset,
				redo_log -> _oldimage, undo_log -> _datalength);
			setPageLSN(undo_page -> buffer, undo_log -> _LSN);
			returnBuffer(undo_page, true);

			flushCount++;
			lastCLE = redo_log -> _nextUndoLSN;

			if(flushCount == 5){
				for(i = 0; i < manager._maxSize; i++){
					flush_log();
					if(manager.buffers[i]._isDirty == true){
			      pwrite(tableID_to_fd[manager.buffers[i]._tableID],
							manager.buffers[i].buffer, 4096, manager.buffers[i]._pageOffset);
						fdatasync(tableID_to_fd[manager.buffers[i]._tableID]);
			      manager.buffers[i]._isDirty = false;
					}
				}
				flushCount = 0;
			}
		}while(lastCLE >= 0);
	}

	/*
		flush all buffer and close tables used in recovery
	*/
	for(i = 0; i < manager._tableSize; i++){
		if(tableID_to_fd[i] != -1){
			fdatasync(tableID_to_fd[i]);
			close(tableID_to_fd[i]);
		}
	}
}
