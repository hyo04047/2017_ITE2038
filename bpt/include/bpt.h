#ifndef __BPT_H__
#define __BPT_H__

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <stdint.h>
#include <sys/types.h>
#include <unistd.h>
#define bool int
#define false 0
#define true 1

// ORDER.
#define LEAF_ORDER 31
#define INTERNAL_ORDER 249

// TYPES.

/* Type representing the record
 * to which a given key refers.
 * In a real B+ tree system, the
 * record would hold data (in a database)
 * or a file (in an operating system)
 * or some other information.
 * Users can rewrite this part of the code
 * to change the type and content
 * of the value field.
 */

/* Type representing a page in the B+ tree.
 * This type is general enough to serve for both
 * the leaf and the internal page.
 * The heart of the page is the array
 * of keys and the array of corresponding
 * pointers.  The relation between keys
 * and pointers differs between leaves and
 * internal pages.  In a leaf, the index
 * of each key equals the index of its corresponding
 * pointer, with a maximum of order - 1 key-pointer
 * pairs.  The last pointer points to the
 * leaf to the right (or NULL in the case
 * of the rightmost leaf).
 * In an internal page, the first pointer
 * refers to lower pages with keys less than
 * the smallest key in the keys array.  Then,
 * with indices i starting at 0, the pointer
 * at i + 1 points to the subtree with keys
 * greater than or equal to the key in this
 * page at index i.
 * The num_keys field is used to keep
 * track of the number of valid keys.
 * In an internal page, the number of valid
 * pointers is always num_keys + 1.
 * In a leaf, the number of valid pointers
 * to data is always num_keys.  The
 * last leaf pointer points to the next leaf.
 */

 typedef struct record {
     int64_t key_;
     char value_[120];
 } record;

typedef struct header_page {
    off_t next_fp;
    off_t root_pg;
    int64_t num_pages;
} header_page;

header_page * header_pg;

// FUNCTION PROTOTYPES.

// Get and Set FUNCTION

bool if_is_leaf(off_t pageoff);
int get_num_key(off_t pageoff);
int64_t get_internal_key(off_t pageoff, int i);
int64_t get_leaf_key(off_t pageoff, int i);
off_t get_internal_child_offset(off_t pageoff, int i);
char * get_value(off_t pageoff, int i);
void set_leaf_key(off_t pageoff, int i, int64_t input);
void set_value(off_t pageoff, int i, char * input);
void set_num_key(off_t pageoff, int i);
void change_num_key(off_t pageoff, int i);
off_t get_freepgo();
void set_right_sibling(off_t pageoff, off_t rightsibling);
void set_parent_offset(off_t pageoff, off_t parent);
off_t get_parent_offset(off_t pageoff);
void set_internal_key(off_t pageoff, int i, int64_t input);
void set_internal_offset(off_t pageoff, int i, off_t input);

// Open.

int open_db(char *pathname);

// Output and utility.

off_t find_leaf( int64_t key );
char * find( int64_t key );
int cut( int length );

// Insertion.

record * make_record(int64_t key, char * value);
off_t make_page( void );
off_t make_leaf( void );
int get_left_index(off_t parent, off_t left);
int insert_into_leaf( off_t leaf, record * pointer );
int insert_into_leaf_after_splitting(off_t leaf, record * pointer);
int insert_into_page(off_t parent,
        int left_index, int64_t key, off_t right);
int insert_into_page_after_splitting(off_t parent,
                                        int left_index,
        int64_t key, off_t right);
int insert_into_parent(off_t left, int64_t key, off_t right);
int insert_into_new_root(off_t left, int64_t key, off_t right);
int start_new_tree(record * pointer);
int insert( int64_t key, char * value );

// Deletion.

int get_neighbor_index( off_t key_leaf );
off_t remove_entry_from_page(off_t key_leaf, int64_t key, off_t child);
int adjust_root();
int coalesce_pages(off_t n, off_t neighbor,
                      int neighbor_index, int k_prime);
int redistribute_pages(off_t n, off_t neighbor,
                          int neighbor_index,
        int k_prime_index, int k_prime);
int delete_entry( off_t key_leaf, int64_t key, off_t child );
int delete( int64_t key );

#endif /* __BPT_H__*/
