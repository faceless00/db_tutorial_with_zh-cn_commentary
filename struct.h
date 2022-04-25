#include <iostream>
//
#ifndef DEF_DEF
#include "def.h"
#endif
//

// Row & Table & Page macros(size offset .etc)
#define size_of_attribute(Struct, Attribute) sizeof(((Struct* )0)->Attribute)  //用来计算结构体的成员的大小
// Row
const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;
// Table
const uint32_t PAGE_SIZE = 4096;  //每页4096字节
#define TABLE_MAX_PAGES 100       //表中最多存储100页
// Page
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;  //每页行数,向下取整
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;  //页不一定刚好被行塞满
// END

// Pager
typedef struct {
    int file_descriptor;
    uint32_t file_length;
    void* pages[TABLE_MAX_PAGES];
} Pager;

// Table
typedef struct {
    uint32_t num_rows;  //待插入的行的下标，相当于总行数
    Pager* pager;
} Table;

// END