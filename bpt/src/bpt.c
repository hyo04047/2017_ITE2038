/*
 *  bpt.c
 */
#define Version "1.14"

#include "bpt.h"

// GLOBALS.

FILE * db_fp;

// header_page * header_pg;

int fd;

int freepg = 0;

// FUNCTION DEFINITIONS.

/* buffer sync */
void buf_sync() {
    fflush(db_fp);
    fdatasync(fd);
}

/* get is leaf */
int if_is_leaf(off_t pageoff) {
    fseek(db_fp, pageoff + 8L, SEEK_SET);
    int check;
    fread(&check, 4, 1, db_fp);
    return check;
}

/* get number of keys */
int get_num_key(off_t pageoff) {
    fseek(db_fp, pageoff + 12L, SEEK_SET);
    int number;
    fread(&number, 4, 1, db_fp);
    return number;
}

/* get internal key */
int64_t get_internal_key(off_t pageoff, int i) {
    fseek(db_fp, pageoff + 128 + 16 * i, SEEK_SET);
    int64_t key;
    fread(&key, 8, 1, db_fp);
    return key;
}

/* get leaf key */
int64_t get_leaf_key(off_t pageoff, int i) {
    fseek(db_fp, pageoff + 128 + 128 * i, SEEK_SET);
    int64_t key;
    fread(&key, 8, 1, db_fp);
    return key;
}

/* get next page offset */
off_t get_internal_child_offset(off_t pageoff, int i) {
    fseek(db_fp, pageoff + 120 + 16 * i, SEEK_SET);
    off_t tmp;
    fread(&tmp, 8, 1, db_fp);
    return tmp;
}

/* get value */
char * get_value(off_t pageoff, int i) {
    fseek(db_fp, pageoff + 136 + 128 * i, SEEK_SET);
    char * value = calloc(sizeof(char), 120);
    fread(value, 120, 1, db_fp);
    return value;
}

/* set leaf key */
void set_leaf_key(off_t pageoff, int i, int64_t input) {
    fseek(db_fp, pageoff + 128 + 128 * i, SEEK_SET);
    fwrite(&input, 8, 1, db_fp);
}

/* set leaf value */
void set_value(off_t pageoff, int i, char * input) {
    fseek(db_fp, pageoff + 136 + 128 * i, SEEK_SET);
    fwrite(input, 120, 1, db_fp);
}

/* set number of keys */
void set_num_key(off_t pageoff, int i) {
    fseek(db_fp, pageoff + 12L, SEEK_SET);
    fwrite(&i, 4, 1, db_fp);
}

/* change number of keys for i */
void change_num_key(off_t pageoff, int i) {
    int tmp = get_num_key(pageoff);
    tmp += i;
    fseek(db_fp, pageoff + 12L, SEEK_SET);
    fwrite(&tmp, 4, 1, db_fp);
}

/* get free page offset */
off_t get_freepgo() {
    off_t c = header_pg->next_fp;
    long int len;
    off_t save;
    fseek(db_fp, 0, SEEK_END);
    len = ftell(db_fp);
    if(c > len || c == 0) {
        freepg += 4096;
        header_pg->next_fp = freepg;
        save = header_pg->next_fp;
    }
    else {
        fseek(db_fp, c, SEEK_SET);
        fread(&save, 8, 1, db_fp);
	if(save > len || save == '\0')	save = 0;
        header_pg->next_fp = save;
    }
    fseek(db_fp, 0L, SEEK_SET);
    fwrite(&save, 8, 1, db_fp);

    return c;
}

/* set right sibling */
void set_right_sibling(off_t pageoff, off_t rightsibling) {
    fseek(db_fp, pageoff + 120, SEEK_SET);
    fwrite(&rightsibling, 8, 1, db_fp);
}

/* set parent offset */
void set_parent_offset(off_t pageoff, off_t parent) {
    fseek(db_fp, pageoff, SEEK_SET);
    fwrite(&parent, 8, 1, db_fp);
}

/* get parent offset */
off_t get_parent_offset(off_t pageoff) {
    fseek(db_fp, pageoff, SEEK_SET);
    off_t tmp;
    fread(&tmp, 8, 1, db_fp);
    return tmp;
}

/* set internal key */
void set_internal_key(off_t pageoff, int i, int64_t input) {
    fseek(db_fp, pageoff + 128 + 16 * i, SEEK_SET);
    fwrite(&input, 8, 1, db_fp);
}

/* set internal offset */
void set_internal_offset(off_t pageoff, int i, off_t input) {
    fseek(db_fp, pageoff + 120 + 16 * i, SEEK_SET);
    fwrite(&input, 8, 1, db_fp);
}

/* get right sibling */
off_t get_right_sibling(off_t pageoff) {
    fseek(db_fp, pageoff + 120, SEEK_SET);
    off_t tmp;
    fread(&tmp, 8, 1, db_fp);
    return tmp;
}

/* Open DB file.
*/

int open_db(char *pathname) {
    if((db_fp = fopen(pathname, "r+")) == NULL)
        db_fp = fopen(pathname, "w+");
    if(db_fp == NULL){
        printf("open error\n");
        return -1;
    }
    fd = fileno(db_fp);
    if(fd < 3){
        printf("file descripter error\n");
        return -1;
    }

    header_pg = (header_page *)malloc(sizeof(header_page));

    /* Set Free page offset */
    if(fseek(db_fp, 0L, SEEK_SET) < 0){
        printf("fseek error\n");
        return -1;
    }
    if(fread(&(header_pg->next_fp), sizeof(off_t), 1, db_fp) == -1){
        printf("fread error\n");
        return -1;
    }
    if(header_pg->next_fp == '\0' || header_pg->next_fp == 0){
        freepg += 4096;
        header_pg->next_fp = freepg;
        fseek(db_fp, 0L, SEEK_SET);
        fwrite(&freepg, 8, 1, db_fp);
        // fseek(db_fp, freepg, SEEK_SET);
        // int tmp = 0;
        // fwrite(&tmp, 8, 1, db_fp);
    }

    /* Set Root page offset */
    if(fseek(db_fp, 8L, SEEK_SET) < 0){
        printf("fseek error\n");
        return -1;
    }
    if(fread(&(header_pg->root_pg), sizeof(off_t), 1, db_fp) == -1){
        printf("fread error\n");
        return -1;
    }
    if(header_pg->root_pg == '\0'){
        header_pg->root_pg = -1;
        fseek(db_fp, 8L, SEEK_SET);
        int64_t num = -1;
        fwrite(&num, 8, 1, db_fp);
    }

    /* Set Number of pages */
    if(fseek(db_fp, 16L, SEEK_SET) < 0){
        printf("fseek error\n");
        return -1;
    }
    if(fread(&(header_pg->num_pages), sizeof(int64_t), 1, db_fp) == -1){
        printf("fread error\n");
        return -1;
    }
    if(header_pg->num_pages == '\0'){
        header_pg->num_pages = 1;
        fseek(db_fp, 16L, SEEK_SET);
        int64_t num = 1;
        fwrite(&num, 8, 1, db_fp);
    }
    return 0;
}

/* Finds and returns the record to which
 * a key refers.
 */
char * find( int64_t key ) {
    int i = 0;
    off_t c = find_leaf( key );
    if (c == -1) return NULL;
    for (i = 0; i < get_num_key(c); i++)
        if (get_leaf_key(c, i) == key) break;
    if (i == get_num_key(c)) return NULL;
    else{
        return get_value(c, i);
    }
}

int64_t find_leaf( int64_t key ) {
    int i = 0;
    off_t c = header_pg->root_pg;
    if (header_pg->num_pages == 1) {
        return c;
    }
    while (!if_is_leaf(c)) {
        i = 0;
        while (i < get_num_key(c)) {
            if (key >= get_internal_key(c, i)) i++;
            else break;
        }
        c = get_internal_child_offset(c, i);
    }
    return c;
}

/* Finds the appropriate place to
 * split a page that is too big into two.
 */
int cut( int length ) {
    if (length % 2 == 0)
        return length/2;
    else
        return length/2 + 1;
}


// INSERTION

/* Creates a new record to hold the value
 * to which a key refers.
 */
record * make_record(int64_t key, char * value) {
    record * new_record = (record *)malloc(sizeof(record));
    new_record->key_ = key;
    strcpy(new_record->value_, value);
    return new_record;
}


/* Creates a new general page, which can be adapted
 * to serve as either a leaf or an internal page.
 */
off_t make_page( void ) {
    /* move to free page */
    off_t tmp = get_freepgo();

    /* set isleaf */
    fseek(db_fp, tmp + 8, SEEK_CUR);
    int isleaf_ = false;
    fwrite(&isleaf_, 4, 1, db_fp);

    /* set number of keys */
    int num = 0;
    fwrite(&num, 4, 1, db_fp);

    int pages = ++(header_pg->num_pages);
    fseek(db_fp, 16L, SEEK_SET);
    fwrite(&pages, 8, 1, db_fp);

    /* set parent page offset */
    fseek(db_fp, tmp, SEEK_SET);
    int64_t num2 = -1;
    fwrite(&num2, 8, 1, db_fp);

    return tmp;
}

/* Creates a new leaf by creating a page
 * and then adapting it appropriately.
 */
off_t make_leaf( void ) {
    off_t new_pageo = make_page();
    fseek(db_fp, new_pageo + 8L, SEEK_SET);
    int num = 1;
    fwrite(&num, 4, 1, db_fp);
    return new_pageo;
}


/* Helper function used in insert_into_parent
 * to find the index of the parent's pointer to
 * the page to the left of the key to be inserted.
 */
int get_left_index(off_t parent, off_t left) {

    int left_index = 0;
    while (left_index <= get_num_key(parent) &&
            get_internal_child_offset(parent, left_index) != left)
        left_index++;
    return left_index;
}

/* Inserts a new pointer to a record and its corresponding
 * key into a leaf.
 * Returns the altered leaf.
 */
int insert_into_leaf( off_t leaf, record * pointer ) {

    int i, insertion_point;

    insertion_point = 0;
    while (insertion_point < get_num_key(leaf) && get_leaf_key(leaf, insertion_point) < pointer->key_)
        insertion_point++;

    for (i = get_num_key(leaf); i > insertion_point; i--) {
        set_leaf_key(leaf, i, get_leaf_key(leaf, i - 1));
        set_value(leaf, i, get_value(leaf, i - 1));
    }
    set_leaf_key(leaf, insertion_point, pointer->key_);
    set_value(leaf, insertion_point, pointer->value_);
    change_num_key(leaf, 1);
    return 0;
}


/* Inserts a new key and pointer
 * to a new record into a leaf so as to exceed
 * the tree's order, causing the leaf to be split
 * in half.
 */
int insert_into_leaf_after_splitting(off_t leaf, record * pointer) {

    off_t new_leaf;
    record * temp_record;
    int insertion_index, split, i, j;
    int64_t new_key;

    new_leaf = make_leaf();

    temp_record = malloc( LEAF_ORDER * sizeof(record) );

    insertion_index = 0;
    while (insertion_index < LEAF_ORDER - 1 && get_leaf_key(leaf, insertion_index) < pointer->key_)
        insertion_index++;

    for (i = 0, j = 0; i < get_num_key(leaf); i++, j++) {
        if (j == insertion_index) j++;
        temp_record[j].key_ = get_leaf_key(leaf, i);
        strcpy(temp_record[j].value_, get_value(leaf, i));
    }

    temp_record[insertion_index].key_ = pointer->key_;
    strcpy(temp_record[insertion_index].value_, pointer->value_);

    set_num_key(leaf, 0);

    split = cut(LEAF_ORDER - 1);

    for (i = 0; i < split; i++) {
        set_value(leaf, i, temp_record[i].value_);
        set_leaf_key(leaf, i, temp_record[i].key_);
        change_num_key(leaf, 1);
    }

    for (i = split, j = 0; i < LEAF_ORDER; i++, j++) {
      set_value(new_leaf, j, temp_record[i].value_);
      set_leaf_key(new_leaf, j, temp_record[i].key_);
      change_num_key(new_leaf, 1);
    }

    free(temp_record);

    off_t tmp = get_right_sibling(leaf);

    set_right_sibling(leaf, new_leaf);
    set_parent_offset(new_leaf, get_parent_offset(leaf));
    set_right_sibling(new_leaf, tmp);
    new_key = get_leaf_key(new_leaf, 0);

    return insert_into_parent(leaf, new_key, new_leaf);
}


/* Inserts a new key and pointer to a page
 * into a page into which these can fit
 * without violating the B+ tree properties.
 */
int insert_into_page(off_t parent, int left_index, int64_t key, off_t right) {
    int i;

    for (i = get_num_key(parent); i > left_index; i--) {
        set_internal_offset(parent, i + 1, get_internal_child_offset(parent, i));
        set_internal_key(parent, i, get_internal_key(parent, i - 1));
    }
    set_internal_offset(parent, left_index + 1, right);
    set_internal_key(parent, left_index, key);
    change_num_key(parent, 1);
    return 0;
}


/* Inserts a new key and pointer to a page
 * into a page, causing the page's size to exceed
 * the order, and causing the page to split into two.
 */
int insert_into_page_after_splitting(off_t old_page, int left_index,
        int64_t key, off_t right) {
    int i, j, split, k_prime;
    off_t new_page, child;
    int64_t * temp_keys;
    off_t * temp_offset;

    /* First create a temporary set of keys and pointers
     * to hold everything in order, including
     * the new key and pointer, inserted in their
     * correct places.
     * Then create a new page and copy half of the
     * keys and pointers to the old page and
     * the other half to the new.
     */

    temp_offset = malloc( (INTERNAL_ORDER + 1) * sizeof(off_t) );
    temp_keys = malloc( INTERNAL_ORDER * sizeof(int64_t) );

    for (i = 0, j = 0; i < get_num_key(old_page) + 1; i++, j++) {
        if (j == left_index + 1) j++;
        temp_offset[j] = get_internal_child_offset(old_page, i);
    }

    for (i = 0, j = 0; i < get_num_key(old_page); i++, j++) {
        if (j == left_index) j++;
        temp_keys[j] = get_internal_key(old_page, i);
    }

    temp_offset[left_index + 1] = right;
    temp_keys[left_index] = key;

    /* Create the new page and copy
     * half the keys and pointers to the
     * old and half to the new.
     */
    split = cut(INTERNAL_ORDER);
    new_page = make_page();
    set_num_key(old_page, 0);
    for (i = 0; i < split - 1; i++) {
        set_internal_offset(old_page, i, temp_offset[i]);
        set_internal_key(old_page, i, temp_keys[i]);
        change_num_key(old_page, 1);
    }
    set_internal_offset(old_page, i, temp_offset[i]);
    k_prime = temp_keys[split - 1];
    for (++i, j = 0; i < INTERNAL_ORDER; i++, j++) {
        set_internal_offset(new_page, j, temp_offset[i]);
        set_internal_key(new_page, j, temp_keys[i]);
        change_num_key(new_page, 1);
    }
    set_internal_offset(new_page, j, temp_offset[i]);
    free(temp_offset);
    free(temp_keys);
    set_parent_offset(new_page, get_parent_offset(old_page));
    for (i = 0; i <= get_num_key(new_page); i++) {
        child = get_internal_child_offset(new_page, i);
        set_parent_offset(child, new_page);
    }

    /* Insert a new key into the parent of the two
     * pages resulting from the split, with
     * the old page to the left and the new to the right.
     */

    return insert_into_parent(old_page, k_prime, new_page);
}



/* Inserts a new page (leaf or internal page) into the B+ tree.
 * Returns the root of the tree after insertion.
 */
int insert_into_parent(off_t left, int64_t key, off_t right) {

    int left_index;
    off_t parent;

    parent = get_parent_offset(left);

    /* Case: new root. */

    if (parent == -1)
        return insert_into_new_root(left, key, right);

    /* Case: leaf or page. (Remainder of
     * function body.)
     */

    /* Find the parent's pointer to the left
     * page.
     */

    left_index = get_left_index(parent, left);


    /* Simple case: the new key fits into the page.
     */

    if (get_num_key(parent) < INTERNAL_ORDER - 1)
        return insert_into_page(parent, left_index, key, right);

    /* Harder case:  split a page in order
     * to preserve the B+ tree properties.
     */

    return insert_into_page_after_splitting(parent, left_index, key, right);
}


/* Creates a new root for two subtrees
 * and inserts the appropriate key into
 * the new root.
 */
int insert_into_new_root(off_t left, int64_t key, off_t right) {

    off_t root = make_page();
    set_internal_key(root, 0, key);
    set_internal_offset(root, 0, left);
    set_internal_offset(root, 1, right);
    change_num_key(root, 1);
    set_parent_offset(root, -1);
    set_parent_offset(left, root);
    set_parent_offset(right, root);
    header_pg->root_pg = root;
    fseek(db_fp, 8L, SEEK_SET);
    fwrite(&root, 8, 1, db_fp);

    return 0;
}



/* First insertion:
 * start a new tree.
 */
int start_new_tree(record * pointer) {

    /* increase number of key */
    off_t root = make_leaf();
    fseek(db_fp, root + 12L, SEEK_SET);
    int one = 1;
    fwrite(&one, 4, 1, db_fp);

    /* input record to root */
    fseek(db_fp, root + 128L, SEEK_SET);
    fwrite(pointer, 128, 1, db_fp);

    header_pg->root_pg = root;
    fseek(db_fp, 8L, SEEK_SET);
    fwrite(&root, 8, 1, db_fp);

    return 0;
}



/* Master insertion function.
 * Inserts a key and an associated value into
 * the B+ tree, causing the tree to be adjusted
 * however necessary to maintain the B+ tree
 * properties.
 */
int insert( int64_t key, char * value ) {

    record * pointer;
    off_t leaf;
    int result;

    /* The current implementation ignores
     * duplicates.
     */

    if (find(key) != NULL){
        return -1;
    }

    /* Create a new record for the
     * value.
     */
    pointer = make_record(key, value);


    /* Case: the tree does not exist yet.
     * Start a new tree.
     */

    if (header_pg->root_pg == -1){
        result = start_new_tree(pointer);
        free(pointer);
        buf_sync();
        return result;
    }

    /* Case: the tree already exists.
     * (Rest of function body.)
     */
    leaf = find_leaf(key);

    /* Case: leaf has room for key and pointer.
     */

    if (get_num_key(leaf) < LEAF_ORDER - 1) {
        result = insert_into_leaf(leaf, pointer);
        buf_sync();
        free(pointer);
        return result;
    }

    /* Case:  leaf must be split.
     */
    result = insert_into_leaf_after_splitting(leaf, pointer);
    free(pointer);
    buf_sync();

    return result;
}




// DELETION.

/* Utility function for deletion.  Retrieves
 * the index of a page's nearest neighbor (sibling)
 * to the left if one exists.  If not (the page
 * is the leftmost child), returns -1 to signify
 * this special case.
 */
int get_neighbor_index( off_t key_leaf ) {

    int i;

    /* Return the index of the key to the left
     * of the pointer in the parent pointing
     * to n.
     * If n is the leftmost child, this means
     * return -1.
     */
    for (i = 0; i <= get_num_key(get_parent_offset(key_leaf)); i++)
        if (get_internal_child_offset(get_parent_offset(key_leaf), i) == key_leaf)
            return i - 1;
}


off_t remove_entry_from_page(off_t key_leaf, int64_t key, off_t child) {

    int i, num_pointers;

    // Remove the key and shift other keys accordingly.
    i = 0;
    char * value = calloc(sizeof(char), 120);
    value = find(key);
    if(if_is_leaf(key_leaf)){
        while (get_leaf_key(key_leaf, i) != key)
            i++;
        for (++i; i < get_num_key(key_leaf); i++)
            set_leaf_key(key_leaf, i - 1, get_leaf_key(key_leaf, i));
    }
    else{
        while (get_internal_key(key_leaf, i) != key){
            i++;
        }
        for (++i; i < get_num_key(key_leaf); i++)
          set_internal_key(key_leaf, i - 1, get_internal_key(key_leaf, i));
    }

    // Remove the pointer and shift other pointers accordingly.
    // First determine number of pointers.
    num_pointers = if_is_leaf(key_leaf) ? get_num_key(key_leaf) : get_num_key(key_leaf) + 1;
    i = 0;
    if(if_is_leaf(key_leaf)){
        while (strcmp(get_value(key_leaf, i), value) != 0){
            i++;
        }
        for (++i; i < num_pointers; i++)
            set_value(key_leaf, i - 1, get_value(key_leaf, i));
    }
    else{
        while (get_internal_child_offset(key_leaf, i) != child)
            i++;
        for (++i; i < num_pointers; i++)
            set_internal_offset(key_leaf, i - 1, get_internal_child_offset(key_leaf, i));
    }
    // One key fewer.
    change_num_key(key_leaf, -1);
    free(value);
	
    return key_leaf;
}


int adjust_root() {

    off_t new_root;

    /* Case: nonempty root.
     * Key and pointer have already been deleted,
     * so nothing to be done.
     */

    if (get_num_key(header_pg->root_pg) > 0)
        return 0;

    /* Case: empty root.
     */

    // If it has a child, promote
    // the first (only) child
    // as the new root.

    if (!if_is_leaf(header_pg->root_pg)) {
        new_root = get_internal_child_offset(header_pg->root_pg, 0);
        header_pg->root_pg = new_root;
        fseek(db_fp, 8L, SEEK_SET);
        fwrite(&new_root, 8, 1, db_fp);
        set_parent_offset(header_pg->root_pg, -1);
    }

    // If it is a leaf (has no children),
    // then the whole tree is empty.

    else{
        new_root = -1;
        header_pg->root_pg = new_root;
        fseek(db_fp, 8L, SEEK_SET);
        fwrite(&new_root, 8, 1, db_fp);
        set_parent_offset(header_pg->root_pg, -1);
      }

    return 0;
}


/* Coalesces a page that has become
 * too small after deletion
 * with a neighboring page that
 * can accept the additional entries
 * without exceeding the maximum.
 */
int coalesce_pages(off_t n, off_t neighbor, int neighbor_index, int k_prime) {

    int i, j, neighbor_insertion_index, n_end;
    off_t tmp;
    int result;

    /* Swap neighbor with page if page is on the
     * extreme left and neighbor is to its right.
     */

    if (neighbor_index == -1) {
        tmp = n;
        // int64_t tmp_key = get_internal_key(get_parent_offset(n), 0);
        // set_internal_key(get_parent_offset(n), 0, get_internal_key(n, 1));
        // set_internal_key(get_parent_offset(n), 1, tmp_key);
        // set_internal_offset(get_parent_offset(n), 0, neighbor);
        // set_internal_offset(get_parent_offset(n), 1, n);
        n = neighbor;
        neighbor = tmp;
    }

    /* Starting point in the neighbor for copying
     * keys and pointers from n.
     * Recall that n and neighbor have swapped places
     * in the special case of n being a leftmost child.
     */

    neighbor_insertion_index = get_num_key(neighbor);

    /* Case:  nonleaf page.
     * Append k_prime and the following pointer.
     * Append all pointers and keys from the neighbor.
     */

    if (!if_is_leaf(n)) {

        /* Append k_prime.
         */

        set_internal_key(neighbor, neighbor_insertion_index, k_prime);
        change_num_key(neighbor, 1);

        n_end = get_num_key(n);

        for (i = neighbor_insertion_index + 1, j = 0; j < n_end; i++, j++) {
            set_internal_key(neighbor, i, get_internal_key(n, j));
            set_internal_offset(neighbor, i, get_internal_child_offset(n, j));
            change_num_key(neighbor, 1);
            change_num_key(n, -1);
        }

        /* The number of pointers is always
         * one more than the number of keys.
         */

        set_internal_offset(neighbor, i, get_internal_child_offset(n, j));

        /* All children must now point up to the same parent.
         */

        for (i = 0; i < get_num_key(neighbor) + 1; i++) {
            tmp = get_internal_child_offset(neighbor, i);
            set_parent_offset(tmp, neighbor);
        }
    }

    /* In a leaf, append the keys and pointers of
     * n to the neighbor.
     * Set the neighbor's last pointer to point to
     * what had been n's right neighbor.
     */

    else {
        for (i = neighbor_insertion_index, j = 0; j < get_num_key(n); i++, j++) {
            set_leaf_key(neighbor, i, get_leaf_key(n, j));
            set_value(neighbor, i, get_value(n, j));
            change_num_key(neighbor, 1);
        }
        set_right_sibling(neighbor, get_right_sibling(n));
    }

    result = delete_entry(get_parent_offset(n), k_prime, n);
    /* insert n into freepage list */
    off_t c = header_pg->next_fp;
    fseek(db_fp, 0, SEEK_SET);
    fwrite(&n, 8, 1, db_fp);
    fseek(db_fp, n, SEEK_SET);
    fwrite(&c, 8, 1, db_fp);
    return result;
}


/* Redistributes entries between two pages when
 * one has become too small after deletion
 * but its neighbor is too big to append the
 * small page's entries without exceeding the
 * maximum
 */
int redistribute_pages(off_t n, off_t neighbor, int neighbor_index,
        int k_prime_index, int k_prime) {

    int i;
    off_t tmp;

    /* Case: n has a neighbor to the left.
     * Pull the neighbor's last key-pointer pair over
     * from the neighbor's right end to n's left end.
     */
    if (neighbor_index != -1) {
        if (!if_is_leaf(n))
            set_internal_offset(n, get_num_key(n) + 1, get_internal_child_offset(n, get_num_key(n)));
        if (if_is_leaf(n)){
            for (i = get_num_key(n); i > 0; i--) {
                set_leaf_key(n, i, get_leaf_key(n, i - 1));
                set_value(n, i, get_value(n, i - 1));
            }
        }
        else {
            for (i = get_num_key(n); i > 0; i--) {
                set_internal_key(n, i, get_internal_key(n, i - 1));
                set_internal_offset(n, i, get_internal_child_offset(n, i - 1));
            }
        }
        if (!if_is_leaf(n)) {
            set_internal_offset(n, 0, get_internal_child_offset(neighbor, get_num_key(neighbor)));
            tmp = get_internal_child_offset(n, 0);
            set_parent_offset(tmp, n);
            set_internal_key(n, 0, k_prime);
            set_internal_key(get_parent_offset(n), k_prime_index, get_internal_key(neighbor, get_num_key(neighbor) - 1));
        }
        else {
            set_value(n, 0, get_value(neighbor, get_num_key(neighbor) - 1));
            set_leaf_key(n, 0, get_leaf_key(neighbor, get_num_key(neighbor) - 1));
            set_internal_key(get_parent_offset(n), k_prime_index, get_leaf_key(n, 0));
        }
    }

    /* Case: n is the leftmost child.
     * Take a key-pointer pair from the neighbor to the right.
     * Move the neighbor's leftmost key-pointer pair
     * to n's rightmost position.
     */

    else {
        if (if_is_leaf(n)) {
            set_leaf_key(n, get_num_key(n), get_leaf_key(neighbor, 0));
            set_value(n, get_num_key(n), get_value(neighbor, 0));
            set_internal_key(get_parent_offset(n), k_prime_index, get_leaf_key(neighbor, 1));
        }
        else {
            set_internal_key(n, get_num_key(n), k_prime);
            set_internal_offset(n, get_num_key(n) + 1, get_internal_child_offset(neighbor, 0));
            tmp = get_internal_child_offset(n, get_num_key(n) + 1);
            set_parent_offset(tmp, n);
            set_internal_key(get_parent_offset(n), k_prime_index, get_internal_key(neighbor, 0));
        }
        if (if_is_leaf(neighbor)){
            for (i = 0; i < get_num_key(neighbor) - 1; i++) {
                set_leaf_key(neighbor, i, get_leaf_key(neighbor, i + 1));
                set_value(neighbor, i, get_value(neighbor, i + 1));
            }
        }
        else {
            for (i = 0; i < get_num_key(neighbor) - 1; i++) {
                set_internal_key(neighbor, i, get_internal_key(neighbor, i + 1));
                set_internal_offset(neighbor, i, get_internal_child_offset(neighbor, i + 1));
            }
        }
        if (!if_is_leaf(n))
            set_internal_offset(neighbor, i, get_internal_child_offset(neighbor, i + 1));
    }

    /* n now has one more key and one more pointer;
     * the neighbor has one fewer of each.
     */

    change_num_key(n, 1);
    change_num_key(neighbor, -1);

    return 0;
}


/* Deletes an entry from the B+ tree.
 * Removes the record and its key and pointer
 * from the leaf, and then makes all appropriate
 * changes to preserve the B+ tree properties.
 */
int delete_entry( off_t key_leaf, int64_t key, off_t child ) {

    int64_t min_keys;
    off_t neighbor;
    int neighbor_index;
    int k_prime_index, k_prime;
    int capacity;

    // Remove key and pointer from page.
    key_leaf = remove_entry_from_page(key_leaf, key, child);

    /* Case:  deletion from the root.
     */

    if (key_leaf == header_pg->root_pg){
        return adjust_root();
    }

    /* Case:  deletion from a page below the root.
     * (Rest of function body.)
     */

    /* Determine minimum allowable size of page,
     * to be preserved after deletion.
     */

    min_keys = if_is_leaf(key_leaf) ? cut(LEAF_ORDER - 1) : cut(INTERNAL_ORDER) - 1;

    /* Case:  page stays at or above minimum.
     * (The simple case.)
     */

    if (get_num_key(key_leaf) >= min_keys)
        return 0;

    /* Case:  page falls below minimum.
     * Either coalescence or redistribution
     * is needed.
     */

    /* Find the appropriate neighbor page with which
     * to coalesce.
     * Also find the key (k_prime) in the parent
     * between the pointer to page n and the pointer
     * to the neighbor.
     */

    neighbor_index = get_neighbor_index( key_leaf );
    k_prime_index = neighbor_index == -1 ? 0 : neighbor_index;
    k_prime = get_internal_key(get_parent_offset(key_leaf), k_prime_index);
    neighbor = neighbor_index == -1 ? get_internal_child_offset(get_parent_offset(key_leaf), 1) :
        get_internal_child_offset(get_parent_offset(key_leaf), neighbor_index);

    capacity = if_is_leaf(key_leaf) ? LEAF_ORDER : INTERNAL_ORDER - 1;

    /* Coalescence. */

    if (get_num_key(neighbor) + get_num_key(key_leaf) < capacity){
        return coalesce_pages(key_leaf, neighbor, neighbor_index, k_prime);
    }

    /* Redistribution. */

    else{
        return redistribute_pages(key_leaf, neighbor, neighbor_index, k_prime_index, k_prime);
    }
}



/* Master deletion function.
 */
int delete(int64_t key) {

    off_t key_leaf;
    int result;
    off_t a = 0;

    key_leaf = find_leaf(key);
    if (key_leaf != -1) {
        result = delete_entry(key_leaf, key, 0);
    }
    buf_sync();
    return result;
}
