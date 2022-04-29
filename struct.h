#include <iostream>
//
#ifndef DEF_DEF
#include "def.h"
#endif
//

// Row & Table & Page macros(size offset .etc)
#define size_of_attribute(Struct, Attribute) \
    sizeof(((Struct*)0)->Attribute)  //用来计算结构体的成员的大小
/* Row */
const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;
/* Table */
const uint32_t PAGE_SIZE = 4096;  //每页4096字节
#define TABLE_MAX_PAGES 100       //表中最多存储100页
/* END */

/* Pager */
typedef struct {
    int file_descriptor;
    uint32_t file_length;
    void* pages[TABLE_MAX_PAGES];
    uint32_t num_pages;  // pager存储的page数量
} Pager;

/* Table */
typedef struct {
    Pager* pager;
    uint32_t root_page_num;  // pager的根节点的page下标
} Table;

/* Cursor */
// Cursor标识表中的一个位置
typedef struct {
    Table* table;
    uint32_t cell_num;  // cell下标
    uint32_t page_num;  // page下标
    bool end_of_table;  //是否到达最后一个元素之后的位置
} Cursor;

/* Tree */
typedef enum : uint8_t
{
    NODE_INTERNAL,
    NODE_LEAF
} NodeType;

/*************** 节点布局 ***************/
/********* COMMON NODE *********/
// 通用节点头部布局
const uint32_t NODE_TYPE_SIZE = sizeof(NodeType);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_OFFSET;
const uint32_t COMMON_NODE_HEADER_SIZE =
    NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;
/********* LEAF NODE *********/
// 叶子节点头部布局
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE =
    COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;
// 叶子节点body布局
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET =
    LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
cuint32 LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
//叶子节点最多存储的cell个数
cuint32 LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;
//分裂时节点的cell个数
cuint32 LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) / 2;
cuint32 LEAF_NODE_LEFT_SPLIT_COUNT =
    (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT;
/********* INTERNAL NODE *********/
//内部节点头部布局
cuint32 INTERNAL_NODE_NUM_KEYS_SIZE = sizeof(uint32_t);
cuint32 INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE;
cuint32 INTERNAL_NODE_RIGHT_CHILD_SIZE = sizeof(uint32_t);
cuint32 INTERNAL_NODE_RIGHT_CHILD_OFFSET =
    INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
cuint32 INTERNAL_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE +
                                    INTERNAL_NODE_NUM_KEYS_SIZE +
                                    INTERNAL_NODE_RIGHT_CHILD_SIZE;
//内部节点body布局
cuint32 INTERNAL_NODE_KEY_SZIE=sizeof(uint32_t);
cuint32 INTERNAL_NODE_CHILD_SIZE=sizeof(uint32_t);
cuint32 INTERNAL_NODE_CELL_SIZE=INTERNAL_NODE_KEY_SZIE+INTERNAL_NODE_CHILD_SIZE;
//
/* End */