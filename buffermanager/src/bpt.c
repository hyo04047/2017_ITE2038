#include <stdio.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include "bpt.h"

int getNumberOfKeys(char *_page){
   int numKey;
   memcpy(&numKey, _page + 12, sizeof(int));

   return numKey;
}

bool isLeaf(char *_page){
	int leaf;
	memcpy(&leaf, _page + 8, sizeof(int));

	return leaf;
}

int64_t getInternalKey(char *_page, int _keyNum){
	int64_t key;
	memcpy(&key, _page + 128 + _keyNum * 16, sizeof(int64_t));

	return key;
}

int64_t getInternalOffset(char *_page, int _offsetNum){
	int64_t offset;
	memcpy(&offset, _page + 120 + _offsetNum * 16, sizeof(int64_t));

	return offset;
}

int64_t getLeafKey(char *_page, int _keyNum){
	int64_t key;
	memcpy(&key, _page + 128 + 128 * _keyNum, sizeof(int64_t));

	return key;
}

char * getLeafValue(char *_page, int _valueNum){
	char *tmp;
  tmp = calloc(sizeof(char), 120);
	memcpy(tmp, _page + 128 + 8 + 128 * _valueNum, 120);
	int len = strlen(tmp) + 1;
	char *value;
	value = calloc(sizeof(char), len);
	strncpy(value, tmp, len);
  free(tmp);
	return value;
}

int64_t getParent(char *_page){
	int64_t parentOffset;
	memcpy(&parentOffset, _page, sizeof(int64_t));

	return parentOffset;
}

int64_t getNextFreePage(char *_page){
	int64_t nextFreePageOffset;
	memcpy(&nextFreePageOffset, _page, sizeof(int64_t));

	return nextFreePageOffset;
}

int64_t getRightSibling(char *_page){
	int64_t rightSibling;
	memcpy(&rightSibling, _page + 120, sizeof(int64_t));

	return rightSibling;
}

int64_t getHeaderStatus(int table_id, int _stat){
   BCB *header = buffer(table_id, 0);
   int64_t headerInfo = 0;
   switch(_stat){
      case 1:
         memcpy(&headerInfo, header->buffer, sizeof(int64_t));
         break;
      case 2:
         memcpy(&headerInfo, header->buffer + 8, sizeof(int64_t));
         break;
      case 3:
         memcpy(&headerInfo, header->buffer + 16, sizeof(int64_t));
      default: break;
   }
   returnBuffer(header, 0);
   return headerInfo;
}

void setInternalKey(char *_page, int64_t _key, int _keyNum){
	memcpy(_page + 128 + _keyNum * 16, &_key, sizeof(int64_t));
}

void setInternalOffset(char *_page, int64_t _offset, int _offsetNum){
	memcpy(_page + 120 + _offsetNum * 16, &_offset, sizeof(int64_t));
}

void setLeafKey(char *_page, int64_t _key, int _keyNum){
	memcpy(_page + 128 + _keyNum * 128, &_key, sizeof(int64_t));
}

void setLeafValue(char *_page, char *_value, int _valueNum){
  if(_value == NULL){
    char *tmp;
    tmp = calloc(1, 120);
    memcpy(_page + 8 + (_valueNum + 1) * 128, tmp, 120);
    free(tmp);
  } else {
    memcpy(_page + 8 + (_valueNum+ 1) * 128, _value, 120);
  }
}

void setNumberOfKeys(char *_page, int _num){
	memcpy(_page + 12, &_num, sizeof(int));
}

void setParent(char *_page, int64_t _parent){
	memcpy(_page, &_parent, sizeof(int64_t));
}

void setRightSibling(char *_page, int64_t _sibling){
	memcpy(_page + 120, &_sibling, sizeof(int64_t));
}

void setIsLeaf(char *_page, bool _isLeaf){
	memcpy(_page + 8, &_isLeaf, sizeof(bool));
}

void setHeaderStatus(int _table_id, int _stat, int _flag, int64_t _info){
	BCB *header = buffer(_table_id, 0);
	switch(_stat){
		case 1:
			memcpy(header->buffer, &_info, sizeof(int64_t));
			break;
		case 2:
			memcpy(header->buffer + 8, &_info, sizeof(int64_t));
			break;
		case 3:
		switch(_flag){
			int64_t a;
			case -1:
				a = getHeaderStatus(_table_id, 3) - _info;
				memcpy(header->buffer + 16, &a, sizeof(int64_t));
				break;
			case 0:
				memcpy(header->buffer + 16, &_info, sizeof(int64_t));
				break;
			case 1:
				a = getHeaderStatus(_table_id, 3) + _info;
				memcpy(header->buffer + 16, &a, sizeof(int64_t));
				break;
			default: break;
		}
		default: break;
	}
	returnBuffer(header, 1);
}

void upperNumKey(char *_page){
	int numKey;
	memcpy(&numKey, _page + 12, sizeof(int));
	numKey++;
	memcpy(_page + 12, &numKey, sizeof(int));
}

void lowerNumKey(char *_page){
	int numKey;
	memcpy(&numKey, _page + 12, sizeof(int));
	numKey--;
	memcpy(_page + 12, &numKey, sizeof(int));
}

void initPage(int _table_id,BCB *_page){
  int i;
	char *tmp;
	tmp = calloc(32, 128);
	if(tmp == NULL){
		perror("Memory allocation");
		exit(1);
	}
	memcpy(_page->buffer, tmp, 4096);
	free(tmp);
	int64_t bfpOffset = getHeaderStatus(_table_id, 1);
	setParent(_page->buffer, bfpOffset);
	setHeaderStatus(_table_id, 1, 0, _page->_pageOffset);
  returnBuffer(_page, 1);
}

int init_db(int buf_num){
  buffer_num = buf_num;
	buf = malloc(sizeof(BCB) * buf_num);
	if(buf == NULL){
		perror("malloc error\n");
		return -1;
	}
	int i = 0;
	for(i = 0; i < buf_num; ++i){
		buf[i]._tableID = 0;
		buf[i]._isDirty = 0;
		buf[i]._pinCount = 0;
		buf[i]._refbit = 0;
	}
    currentIndex = 0;
    _nextRef = 0;
	return 0;
}

BCB * buffer(int _table_id, off_t _pageOffset){
    int i;
    int index;

    for(i = 0; i < buffer_num; i++){
        if(_table_id == buf[i]._tableID
           && _pageOffset == buf[i]._pageOffset){
            buf[i]._pinCount++;
            return (&(buf[i]));
        }
    }

    if(currentIndex < buffer_num){
        index = readPageToBuffer(_table_id, _pageOffset);
    }
    else{
        index = replaceBuffer(_table_id, _pageOffset);
    }
    return (&(buf[index]));
}

int readPageToBuffer(int _table_id, off_t _pageOffset){
    pread(_table_id, buf[currentIndex].buffer, 4096, _pageOffset);
    buf[currentIndex]._tableID = _table_id;
    buf[currentIndex]._pageOffset = _pageOffset;
    buf[currentIndex]._refbit = true;
    buf[currentIndex]._pinCount = 1;
    currentIndex++;

    return (currentIndex - 1);
}

int replaceBuffer(int _table_id, off_t _pageOffset){
    int index = -1;
    BCB *current = NULL;
    int clk_hand = _nextRef;
    while(index == -1){
        current = &(buf[clk_hand]);
        if(current->_pinCount == 0 && current->_refbit == false) {
            if(current->_isDirty == true){
                pwrite(_table_id, current->buffer, 4096, current->_pageOffset);
            }
            pread(_table_id, current->buffer, 4096, _pageOffset);
            index = clk_hand;
            current->_pageOffset = _pageOffset;
            current->_tableID = _table_id;
            current->_isDirty = false;
            current->_refbit = true;
            current->_pinCount = 1;
        } else if (current->_pinCount == 0 && current->_refbit == true){
            current->_refbit = false;
        }

        clk_hand = (clk_hand + 1) % buffer_num;
    }
    _nextRef = clk_hand;

    return index;
}

void returnBuffer(BCB * _buf, int _dirty){
  _buf->_pinCount--;
  if(_dirty == 1)
    _buf->_isDirty = _dirty;
}

int open_table(char *_pathname){
	if((fd = open(_pathname, O_RDWR, 0644)) < 0){
		if((fd = open(_pathname, O_RDWR | O_CREAT | O_TRUNC, 0644)) < 0){
			perror("Can't open file\n");
			return -1;
		}

    int64_t root = PAGESIZE;
    int64_t free = PAGESIZE * 2;
    int64_t size = 8;
    pwrite(fd, &free, 8, 0);
    pwrite(fd, &root, 8, 8);
    pwrite(fd, &size, 8, 16);

		int i;
    for(i = 2; i < 7; i++){
			free = PAGESIZE * (i + 1);
      pwrite(fd, &free, 8, PAGESIZE * i);
		}
	}
    if(fd > 12){
        return -1;
    }

	return fd;
}

int close_table(int _table_ID){
    if(close(_table_ID) < 0){
        printf("ERROR! Table is not closed\n");
        return -1;
    }
    else{
        return 0;
    }
}

int shutdown_db(){
    int i;
    for(i = 0; i < buffer_num; i++){
        if(buf[i]._pinCount > 0){
            break;
        }
        if(buf[i]._isDirty == true){
            pwrite(buf[i]._tableID, buf[i].buffer,
                   4096, buf[i]._pageOffset);
        }
    }

    free(buf);
    return 0;
}

BCB * start_new_tree(int _table_id, int64_t _key, char *_value){
  BCB *root_ = buffer(_table_id, getHeaderStatus(_table_id, 2));
	setIsLeaf(root_->buffer, 1);
	setLeafKey(root_->buffer, _key, 0);
	setLeafValue(root_->buffer, _value, 0);
  setRightSibling(root_->buffer, 0);
  setParent(root_->buffer, 0);
  upperNumKey(root_->buffer);

	return root_;
}

BCB * freePageToUse(int _table_id){
   int64_t freePageOffset;
   int64_t nextFreePageOffset;
   off_t tmp;

   tmp = lseek(_table_id, 0, SEEK_END);
   freePageOffset = getHeaderStatus(_table_id, 1);
   BCB *buf = buffer(_table_id, freePageOffset);

   nextFreePageOffset = getParent(buf->buffer);
   off_t next;

   if(nextFreePageOffset > tmp){
      int numberOfPages = getHeaderStatus(_table_id, 3);

      int i;
      setHeaderStatus(_table_id, 1, 0, PAGESIZE * numberOfPages);
      next = PAGESIZE * numberOfPages;
      pwrite(_table_id, &next, sizeof(off_t), nextFreePageOffset);
      for(i = numberOfPages; i < numberOfPages + 99; i++){
         next = PAGESIZE * (i + 1);
         pwrite(_table_id, &next, sizeof(off_t), PAGESIZE * i);
      }

      setHeaderStatus(_table_id, 3, 1, 100);
      fdatasync(_table_id);
   }
   setNumberOfKeys(buf->buffer, 0);
   setHeaderStatus(_table_id, 1, 0, nextFreePageOffset);
   return buf;
}

BCB * insert_into_leaf(int _table_id, BCB *_leaf, int64_t _key,
	char *_value){
	int i, insertion_point = 0;
	int numKey = getNumberOfKeys(_leaf->buffer);

	while(insertion_point < numKey &&
		getLeafKey(_leaf->buffer, insertion_point) < _key)
		insertion_point++;

	for(i = getNumberOfKeys(_leaf->buffer); i > insertion_point; i--){
		setLeafKey(_leaf->buffer, getLeafKey(_leaf->buffer, i - 1), i);
    char *tmp = getLeafValue(_leaf->buffer, i - 1);
		setLeafValue(_leaf->buffer, tmp, i);
    free(tmp);
	}
	setLeafKey(_leaf->buffer, _key, insertion_point);
	setLeafValue(_leaf->buffer, _value, insertion_point);
	upperNumKey(_leaf->buffer);

	return _leaf;
}

BCB * insert_into_leaf_after_splitting(int _table_id, BCB *_leaf,
	int64_t _key, char *_value){
	int64_t *tempKeys;
	char **tempValues;
	int insertion_index, split, new_key, i, j, numKeyLeaf;

  BCB * newPage_ = freePageToUse(_table_id);
	setIsLeaf(newPage_->buffer, 1);
	tempKeys = malloc(LEAF_ORDER * sizeof(int64_t));
	if(tempKeys == NULL){
		perror("Temporary keys array.");
		exit(1);
	}

	tempValues = malloc(LEAF_ORDER * sizeof(char *));
	if(tempValues == NULL){
		perror("Temporary values array.");
		exit(1);
	}
    
    for(i = 0; i < LEAF_ORDER; i++)
        tempValues[i] = (char *)malloc(120);

	insertion_index = 0;
	while(insertion_index < LEAF_ORDER - 1 &&
		getLeafKey(_leaf->buffer, insertion_index) < _key)
		insertion_index++;

	numKeyLeaf = getNumberOfKeys(_leaf->buffer);
	for(i = 0, j = 0; i < numKeyLeaf; i++, j++){
		if(j == insertion_index) j++;
		tempKeys[j] = getLeafKey(_leaf->buffer, i);
		tempValues[j] = getLeafValue(_leaf->buffer, i);
	}
	tempKeys[insertion_index] = _key;
	tempValues[insertion_index] = _value;
  setNumberOfKeys(_leaf->buffer, 0);
	split = cut(LEAF_ORDER - 1);

	for(i = 0; i < split; i++){
		setLeafValue(_leaf->buffer, tempValues[i], i);
		setLeafKey(_leaf->buffer, tempKeys[i], i);
		upperNumKey(_leaf->buffer);
	}
	for(i = split, j = 0; i < LEAF_ORDER; i++, j++){
		setLeafValue(newPage_->buffer, tempValues[i], j);
		setLeafKey(newPage_->buffer, tempKeys[i], j);
		upperNumKey(newPage_->buffer);
	}
  for(i = 0; i < LEAF_ORDER; i++){
    if(i == insertion_index) continue;
    free(tempValues[i]);
  }
	free(tempKeys);
	free(tempValues);
	int64_t sibling = getRightSibling(_leaf->buffer);
	setRightSibling(_leaf->buffer, newPage_->_pageOffset);
	setRightSibling(newPage_->buffer, sibling);

	setParent(newPage_->buffer, getParent(_leaf->buffer));
	new_key = getLeafKey(newPage_->buffer, 0);
	return insert_into_parent(_table_id, _leaf, new_key, newPage_);
}

BCB * insert_into_parent(int _table_id, BCB *_left, int64_t _key, BCB *_right){
	int left_index;

  BCB *parent_ = buffer(_table_id, getParent(_left -> buffer));

	if(parent_->_pageOffset == 0){
    returnBuffer(parent_, 0);
		return insert_into_new_root(_table_id, _left, _key, _right);
  }

	left_index = get_left_index(parent_->buffer, _left->_pageOffset);
  returnBuffer(_left, 1);

	if(getNumberOfKeys(parent_->buffer) < INTERNAL_ORDER){
		return insert_into_internal(parent_, left_index, _key, _right);
	}

	return insert_into_internal_after_splitting(_table_id, parent_, left_index, _key, _right);
}

BCB * insert_into_new_root(int _table_id, BCB *_left, int64_t _key, BCB *_right){
	BCB *root_ = freePageToUse(_table_id);
	setIsLeaf(root_->buffer, 0);
	setInternalKey(root_->buffer, _key, 0);
	setInternalOffset(root_->buffer, _left->_pageOffset, 0);
	setInternalOffset(root_->buffer, _right->_pageOffset, 1);
	upperNumKey(root_->buffer);
	setParent(root_->buffer, 0);
	setParent(_left->buffer, root_->_pageOffset);
	setParent(_right->buffer, root_->_pageOffset);
	setHeaderStatus(_table_id, 2, 0, root_->_pageOffset);

  returnBuffer(_left, 1);
  returnBuffer(_right, 1);
	return root_;
}

int get_left_index(char *_parent, off_t _left){
	int left_index = 0;

	while(left_index <= getNumberOfKeys(_parent) &&
		getInternalOffset(_parent, left_index) != _left)
		left_index++;
	return left_index;
}

BCB * insert_into_internal(BCB *_n, int _left_index,
	int64_t _key, BCB *_right){
	int i;

	for(i = getNumberOfKeys(_n->buffer); i > _left_index; i--){
		setInternalOffset(_n->buffer, getInternalOffset(_n->buffer, i), i + 1);
		setInternalKey(_n->buffer, getInternalKey(_n->buffer, i - 1), i);
	}
	setInternalOffset(_n->buffer, _right->_pageOffset, _left_index + 1);
	setInternalKey(_n->buffer, _key, _left_index);
	upperNumKey(_n->buffer);

  returnBuffer(_right, 1);
  return _n;
}

BCB * insert_into_internal_after_splitting(int _table_id, BCB *_oldPage, int _left_index,
	int64_t _key, BCB *_right){
	int i, j, split, k_prime;
	BCB *newPage_;
  BCB *child_;
	int64_t *tempKeys;
	int64_t *tempOffsets;

	tempKeys = malloc((INTERNAL_ORDER + 1) * sizeof(int64_t));
	if(tempKeys == NULL){
		perror("Temporary Keys array for splitting pages");
		exit(1);
	}
	tempOffsets = malloc((INTERNAL_ORDER + 2) * sizeof(int64_t));
	if(tempOffsets == NULL){
		perror("Temporary values array for splitting pages");
		exit(1);
	}

	int numKey = getNumberOfKeys(_oldPage->buffer);
	for(i = 0, j = 0; i < INTERNAL_ORDER + 1; i++, j++){
		if (j == _left_index + 1) j++;
		tempOffsets[j] = getInternalOffset(_oldPage->buffer, i);
	}

	for(i = 0, j = 0; i < INTERNAL_ORDER; i++, j++){
		if (j == _left_index) j++;
		tempKeys[j] = getInternalKey(_oldPage->buffer, i);
	}

	tempOffsets[_left_index + 1] = _right->_pageOffset;
	tempKeys[_left_index] = _key;

	split = cut(INTERNAL_ORDER);

	newPage_ = freePageToUse(_table_id);
  setIsLeaf(newPage_->buffer, false);
  setNumberOfKeys(_oldPage->buffer, 0);

	for(i = 0; i < split - 1; i++){
		setInternalOffset(_oldPage->buffer, tempOffsets[i], i);
		setInternalKey(_oldPage->buffer, tempKeys[i], i);
		upperNumKey(_oldPage->buffer);
	}
	setInternalOffset(_oldPage->buffer, tempOffsets[i], i);
	k_prime = tempKeys[split - 1];

	for(++i, j = 0; i < INTERNAL_ORDER + 1; i++, j++){
		setInternalOffset(newPage_->buffer, tempOffsets[i], j);
		setInternalKey(newPage_->buffer, tempKeys[i], j);
		upperNumKey(newPage_->buffer);
	}
	setInternalOffset(newPage_->buffer, tempOffsets[i], j);
	free(tempOffsets);
	free(tempKeys);
	setParent(newPage_->buffer, getParent(_oldPage->buffer));
	numKey = getNumberOfKeys(newPage_->buffer);

	for(i = 0; i <= numKey; i++){
    child_ = buffer(_table_id, getInternalOffset(newPage_ -> buffer, i));
 	  setParent(child_->buffer, newPage_->_pageOffset);
    returnBuffer(child_, 1);
 	}

  returnBuffer(_right, 1);
	return insert_into_parent(_table_id, _oldPage, k_prime, newPage_);
}

int cut(int length){
	if(length % 2 == 0)
		return length / 2;
	else
		return length/2 + 1;
}

int insert(int _table_id, int64_t _key, char *_value){
  char *tmp;

	if((tmp = find(_table_id, _key)) != NULL){
    free(tmp);
		return -2;
	}

  BCB *leaf_ = buffer(_table_id, getHeaderStatus(_table_id, 2));
	if(getNumberOfKeys(leaf_->buffer) == 0){
    returnBuffer(leaf_, 0);
		leaf_ = start_new_tree(_table_id, _key, _value);
    returnBuffer(leaf_, 1);
		return 0;
	}
  else
    returnBuffer(leaf_, 0);

	leaf_ = find_leaf(_table_id, _key);

	if (getNumberOfKeys(leaf_->buffer) < LEAF_ORDER - 1){
		leaf_ = insert_into_leaf(_table_id, leaf_, _key, _value);
    returnBuffer(leaf_, 1);
		return 0;
	}

	leaf_ = insert_into_leaf_after_splitting(_table_id, leaf_, _key, _value);
  returnBuffer(leaf_, 1);
	return 0;
}

BCB *find_leaf(int _table_id, int64_t _key){
	int i = 0;
	int64_t c = getHeaderStatus(_table_id, 2);

  BCB *leaf_ = buffer(_table_id, c);
	if(getNumberOfKeys(leaf_->buffer) == 0)
		return leaf_;
	while(!isLeaf(leaf_->buffer)){
		i = 0;
		int numKey = getNumberOfKeys(leaf_->buffer);
		while(i < numKey){
			if (_key >= getInternalKey(leaf_->buffer, i)) i++;
			else break;
		};
    returnBuffer(leaf_, 0);
    leaf_ = buffer(_table_id, getInternalOffset(leaf_->buffer, i));
	}
	return leaf_;
}

char *find(int _table_id, int64_t _key){
	int i = 0;
	BCB *c = find_leaf(_table_id, _key);
	if(getNumberOfKeys(c->buffer) == 0){
    returnBuffer(c, 0);
    return NULL;
  }
	int numKey = getNumberOfKeys(c->buffer);
	for(i = 0; i < numKey; i++)
		if(getLeafKey(c->buffer, i) == _key) break;
  returnBuffer(c, 0);
  if(i == numKey)
		return NULL;
	else
		return getLeafValue(c->buffer, i);
}

// deletion
BCB * delete_entry(int _table_id, BCB *_leafKey, int64_t _key, int64_t _offset){
	int min_keys;
	int64_t neighbor;
	int neighbor_index;
	int k_prime_index;
	int64_t k_prime;
	int capacity;

	_leafKey = remove_entry_from_page(_leafKey, _key, _offset);

	if(_leafKey->_pageOffset == getHeaderStatus(_table_id, 2)){
    return adjust_root(_table_id, _leafKey);
  }

	min_keys = isLeaf(_leafKey->buffer) ? cut(LEAF_ORDER - 1) : cut(INTERNAL_ORDER);

	if(getNumberOfKeys(_leafKey->buffer) >= min_keys){
		returnBuffer(_leafKey, 1);
    return NULL;
  }

  BCB *parent_ = buffer(_table_id, getParent(_leafKey->buffer));
  neighbor_index = get_neighbor_index(parent_ -> buffer, _leafKey -> _pageOffset);
	k_prime_index = neighbor_index == -1 ? 0 : neighbor_index;
	k_prime = getInternalKey(parent_->buffer, k_prime_index);
	neighbor = neighbor_index == -1 ?
		getInternalOffset(parent_->buffer, 1) :
			getInternalOffset(parent_->buffer, neighbor_index);
  returnBuffer(parent_, 0);

  BCB *neighbor_ = buffer(_table_id, neighbor);
	capacity = isLeaf(_leafKey->buffer) ? LEAF_ORDER : INTERNAL_ORDER;

	if(getNumberOfKeys(neighbor_->buffer) + getNumberOfKeys(_leafKey->buffer) < capacity){
		return coalesce_pages(_table_id, _leafKey, neighbor_, neighbor_index, k_prime);
  }

	else{
		return redistribute_pages(_table_id, _leafKey, neighbor_, neighbor_index,
			k_prime_index, k_prime);
  }
}

BCB * adjust_root(int _table_id, BCB *_root){
  BCB *newRoot_;

	if(getNumberOfKeys(_root->buffer) > 0){
    returnBuffer(_root, 1);
		return _root;
  }

	if(!isLeaf(_root->buffer)){
		newRoot_ = buffer(_table_id, getInternalOffset(_root->buffer, 0));
		setHeaderStatus(_table_id, 2, 0, newRoot_->_pageOffset);
		setParent(newRoot_->buffer, 0);
	}

	else{
		newRoot_ = NULL;
  }

  initPage(_table_id, _root);

    if(newRoot_ != NULL){
    returnBuffer(newRoot_, 1);
    }
	return newRoot_;
}

BCB * redistribute_pages(int _table_id, BCB *_leafKey, BCB *_neighbor,
	int neighbor_index, int k_prime_index, int k_prime){
	int i;
  BCB *tmp_;

	if(neighbor_index != -1){
		if(!isLeaf(_leafKey->buffer)){
			setInternalOffset(_leafKey->buffer,
				getInternalOffset(_leafKey->buffer, getNumberOfKeys(_leafKey->buffer)),
				getNumberOfKeys(_leafKey->buffer) + 1);
		  for(i = getNumberOfKeys(_leafKey->buffer); i > 0; i--){
			  setInternalKey(_leafKey->buffer, getInternalKey(_leafKey->buffer, i - 1), i);
			  setInternalOffset(_leafKey->buffer, getInternalOffset(
				  _leafKey->buffer, i - 1), i);
		  }
    } else {
      for(i = getNumberOfKeys(_leafKey->buffer); i > 0; i--){
        setLeafKey(_leafKey->buffer, getLeafKey(_leafKey->buffer, i - 1), i);
        char *_tmp = getLeafValue(_leafKey->buffer, i - 1);
        setLeafValue(_leafKey->buffer, _tmp, i);
        free(_tmp);
      }
    }
		if(!isLeaf(_leafKey->buffer)){
			setInternalOffset(_leafKey->buffer, getInternalOffset(_neighbor->buffer,
				getNumberOfKeys(_neighbor->buffer)), 0);
      tmp_ = buffer(_table_id, getInternalOffset(_leafKey->buffer, 0));
			setParent(tmp_->buffer, _leafKey->_pageOffset);
      returnBuffer(tmp_, 1);
			setInternalOffset(_neighbor->buffer, 0, getNumberOfKeys(_neighbor->buffer));
			setInternalKey(_leafKey->buffer, k_prime, 0);
      tmp_ = buffer(_table_id, getParent(_leafKey->buffer));
			setInternalKey(tmp_->buffer, getInternalKey(_neighbor->buffer,
				getNumberOfKeys(_neighbor->buffer) - 1), k_prime_index);
      returnBuffer(tmp_, 1);
		} else {
      char *_tmp = getLeafValue(_neighbor->buffer, getNumberOfKeys(_neighbor->buffer) - 1);
			setLeafValue(_leafKey->buffer, _tmp, 0);
      free(_tmp);
			setLeafValue(_neighbor->buffer, NULL, getNumberOfKeys(_neighbor->buffer) - 1);
			setLeafKey(_leafKey->buffer, getLeafKey(_neighbor->buffer, getNumberOfKeys(
				_neighbor->buffer) - 1), 0);
      tmp_ = buffer(_table_id, getParent(_leafKey->buffer));
			setInternalKey(tmp_->buffer, getLeafKey(_leafKey->buffer, 0),
				k_prime_index);
      returnBuffer(tmp_, 1);
		}
	}

	else {
		if(isLeaf(_leafKey->buffer)){
			setLeafKey(_leafKey->buffer, getLeafKey(_neighbor->buffer, 0),
				getNumberOfKeys(_leafKey->buffer));
      char *_tmp = getLeafValue(_neighbor->buffer, 0);
			setLeafValue(_leafKey->buffer, _tmp, getNumberOfKeys(_leafKey->buffer));
      free(_tmp);
      tmp_ = buffer(_table_id, getParent(_leafKey->buffer));
			setInternalKey(tmp_->buffer, getLeafKey(_neighbor->buffer, 1),
				k_prime_index);
      returnBuffer(tmp_, 1);
		} else {
			setInternalKey(_leafKey->buffer, k_prime, getNumberOfKeys(_leafKey->buffer));
			setInternalOffset(_leafKey->buffer, getInternalOffset(_neighbor->buffer, 0),
				getNumberOfKeys(_leafKey->buffer) + 1);
      tmp_ = buffer(_table_id, getInternalOffset(_leafKey->buffer, getNumberOfKeys(_leafKey->buffer) + 1));
			setParent(tmp_->buffer, _leafKey->_pageOffset);
      returnBuffer(tmp_, 1);
      tmp_ = buffer(_table_id, getParent(_leafKey->buffer));
			setInternalKey(tmp_->buffer, getInternalKey(
				_neighbor->buffer, 0), k_prime_index);
      returnBuffer(tmp_, 1);
		}
		if(!isLeaf(_neighbor->buffer)){
			for(i = 0; i < getNumberOfKeys(_neighbor->buffer) - 1; i++){
				setInternalKey(_neighbor->buffer, getInternalKey(_neighbor->buffer, i + 1), i);
				setInternalOffset(_neighbor->buffer,
					getInternalOffset(_neighbor->buffer, i + 1), i);
			}
		} else {
			for(i = 0; i < getNumberOfKeys(_neighbor->buffer) - 1; i++){
				setLeafKey(_neighbor->buffer, getLeafKey(_neighbor->buffer, i + 1), i);
        char *_tmp = getLeafValue(_neighbor->buffer, i + 1);
				setLeafValue(_neighbor->buffer, _tmp, i);
        free(_tmp);
			}
		}
		if(!isLeaf(_leafKey->buffer))
			setInternalOffset(_neighbor->buffer, getInternalOffset(
				_neighbor->buffer, i + 1), i);
	}

	upperNumKey(_leafKey->buffer);
	lowerNumKey(_neighbor->buffer);

  returnBuffer(_leafKey, 1);
  returnBuffer(_neighbor, 1);
	return NULL;
}

BCB * coalesce_pages(int _table_id, BCB *_leafKey, BCB * _neighbor,
	int neighbor_index, int k_prime){
	int i, j, neighbor_insertion_index, leafKey_end;
	BCB *tmp_;

	if(neighbor_index == -1){
		tmp_ = _leafKey;
		_leafKey = _neighbor;
		_neighbor = tmp_;
	}

	neighbor_insertion_index = getNumberOfKeys(_neighbor->buffer);

	if(!isLeaf(_leafKey->buffer)){
    setInternalKey(_neighbor->buffer, k_prime, neighbor_insertion_index);
		upperNumKey(_neighbor->buffer);

		leafKey_end = getNumberOfKeys(_leafKey->buffer);

		for(i = neighbor_insertion_index + 1, j = 0; j < leafKey_end;
			i++, j++){
			setInternalKey(_neighbor->buffer, getInternalKey(_leafKey->buffer, j), i);
			setInternalOffset(_neighbor->buffer, getInternalOffset(_leafKey->buffer, j), i);
			upperNumKey(_neighbor->buffer);
			lowerNumKey(_leafKey->buffer);
		}

		setInternalOffset(_neighbor->buffer, getInternalOffset(_leafKey->buffer, j), i);

		for(i = 0; i < getNumberOfKeys(_neighbor->buffer) + 1; i++){
      tmp_ = buffer(_table_id, getInternalOffset(_neighbor->buffer, i));
			setParent(tmp_->buffer, _neighbor->_pageOffset);
      returnBuffer(tmp_, 1);
		}
	}

	else {
		for(i = neighbor_insertion_index, j = 0;
				j < getNumberOfKeys(_leafKey->buffer); i++, j++){
			setLeafKey(_neighbor->buffer, getLeafKey(_leafKey->buffer, j), i);
      char *_tmp = getLeafValue(_leafKey->buffer, j);
			setLeafValue(_neighbor->buffer, _tmp, i);
      free(_tmp);
			upperNumKey(_neighbor->buffer);
		}
    setRightSibling(_neighbor->buffer, getRightSibling(_leafKey->buffer));
	}

  BCB * parent_ = buffer(_table_id, getParent(_leafKey->buffer));
  BCB * k = delete_entry(_table_id, parent_, k_prime, _leafKey->_pageOffset);
  initPage(_table_id, _leafKey);
  returnBuffer(_neighbor, 1);
	return k;
}

int get_neighbor_index(char *_parent, off_t _leaf){
	int i;

	int numKey = getNumberOfKeys(_parent);
	for(i = 0; i <= numKey; i++)
		if(getInternalOffset(_parent, i) == _leaf)
			return i - 1;

	return -1;
}

BCB * remove_entry_from_page(BCB *_leafKey, int64_t _key, int64_t _offset){
	int i, j, numKey;

	i = 0;
	numKey = getNumberOfKeys(_leafKey->buffer);
	if(isLeaf(_leafKey->buffer)){
		while(getLeafKey(_leafKey->buffer, i) != _key) i++;
		for(++i; i < numKey; i++){
			setLeafKey(_leafKey->buffer, getLeafKey(_leafKey->buffer, i), i - 1);
      char *tmp = getLeafValue(_leafKey->buffer, i);
			setLeafValue(_leafKey->buffer, tmp, i - 1);
      free(tmp);
		}
	} else {
		while(getInternalKey(_leafKey->buffer, i) != _key) i++;
		j = i;
		for(++i; i < numKey; i++)
			setInternalKey(_leafKey->buffer, getInternalKey(_leafKey->buffer, i), i - 1);
    while(getInternalOffset(_leafKey->buffer, j) != _offset) j++;
		for(++j; j < numKey + 1; j++)
			setInternalOffset(_leafKey->buffer, getInternalOffset(_leafKey->buffer,	j)
				, j - 1);
	}
	lowerNumKey(_leafKey->buffer);

	if(isLeaf(_leafKey->buffer)){
    setLeafKey(_leafKey->buffer, 0, getNumberOfKeys(_leafKey->buffer));
		setLeafValue(_leafKey->buffer, NULL, getNumberOfKeys(_leafKey->buffer));
  }
	else{
    setInternalKey(_leafKey->buffer, 0, getNumberOfKeys(_leafKey->buffer));
	  setInternalOffset(_leafKey->buffer, 0, getNumberOfKeys(_leafKey->buffer) + 1);
  }
	return _leafKey;
}

int delete(int _table_id, int64_t _key){
	BCB *leafKey_;
	char *leafValue;

	leafValue = find(_table_id, _key);
	leafKey_ = find_leaf(_table_id, _key);
	if(leafValue != NULL && leafKey_ != NULL){
		delete_entry(_table_id, leafKey_, _key, 0);
    free(leafValue);
		return 0;
	}
	return -1;
}
