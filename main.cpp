#include <fcntl.h>
#include <unistd.h>

#include <cstdio>
#include <cstring>
#include <iostream>
//
#include "struct.h"
//
using namespace std;

InputBuffer* new_input_buffer();
void print_prompt();
void read_input(InputBuffer* input_buffer);
void close_input_buffer(InputBuffer* input_buffer);
//
MetaCommandResult do_meta_command(InputBuffer* input_buffer, Table* table);
PrepareResult prepare_statement(InputBuffer* input_buffer,
                                Statement* statement);
PrepareResult prepare_insert(InputBuffer* input_buffer, Statement* statement);
/* Row (de)serialize & print */
void serialize_row(Row* source,
                   void* destination);  //序列化数据，结构体->序列化紧凑数据
void deserialize_row(void* source,
                     Row* destination);  //逆序列化紧凑数据->结构体
void print_row(Row* row);                //打印一行数据
//打印叶子节点
void print_leaf_node(void* node);
//打印一些常量
void print_constatns();
/* insert,select,search,modify cmd */
ExecuteResult execute_insert(Statement* statement, Table* table);
ExecuteResult execute_select(Statement* statement, Table* table);
ExecuteResult execute_statement(Statement* statement, Table* table);
/* pager */
Pager* pager_open(const char* filename);
Table* db_open(const char* filename);
void db_close(Table* table);
void* get_page(Pager* pager, uint32_t page_num);
void pager_flush(Pager* pager, uint32_t page_num);
/* cursor */
Cursor* table_start(Table* table);
Cursor* table_end(Table* table);
void* cursor_value(Cursor* cursor);
void cursor_advance(Cursor* cursor);

/* leaf node field locate & leaf node init */

uint32_t* leaf_node_num_cells(void* node);
void* leaf_node_cell(void* node, uint32_t cell_num);
uint32_t* leaf_node_key(void* node, uint32_t cell_num);
void* leaf_node_value(void* node, uint32_t cell_num);
void initialize_leaf_node(void* node);
// insert pair into leaf node
void leaf_node_insert(Cursor* cursor, uint32_t key, Row* value);
//

// Main func
int main(int argc, char* argv[]) {
    if (argc < 2) {
        cout << "Must supply a database filename." << endl;
        exit(EXIT_FAILURE);
    }

    char* filename = argv[1];
    Table* table = db_open(filename);  //连接数据库，返回值是一张表

    InputBuffer* input_buffer = new_input_buffer();  //输入缓存

    while (true) {
        print_prompt();
        read_input(input_buffer);
        // 每一个case都要continue,这样不会执行后面的语句，直接进入下一次while
        if (input_buffer->buffer[0] == '.') {
            switch (do_meta_command(input_buffer, table)) {
                case (META_COMMAND_SUCCESS):
                    continue;
                case (META_COMMAND_UNRECOGNIZED_COMMAND):
                    cout << "Unrecognized command: " << input_buffer->buffer
                         << endl;
                    continue;
            }
        }
        Statement statement;
        switch (prepare_statement(input_buffer, &statement)) {
            case (PREPARE_SUCCESS):
                break;
            case (PREPARE_NEGATIVE_ID):
                cout << "ID must be positive." << endl;
                continue;
            case (PREPARE_STRING_TOO_LONG):
                cout << "String is too long." << endl;
                continue;
            case (PREPARE_SYNTAX_ERROR):
                cout << "Syntax error. Could not parse statement." << endl;
                continue;
            case (PREPARE_UNRECOGNIZED_STATEMENT):  //非inert&select
                cout << "Unrecognized keyword at start of: "
                     << input_buffer->buffer << endl;
                continue;
            default:
                continue;
        }
        switch (execute_statement(&statement, table)) {
            case (EXECUTE_SUCCESS):
                cout << "Executed." << endl;
                break;
            case (EXECUTE_TABLE_FULL):
                cout << "Error: Table full." << endl;
                break;
            default:
                cout << "Unrecognized Error." << endl;
                break;
        }
    }
    return 0;
}
//

// function region

/*************** input & convert input to statement & row serialize
 * ***************/
InputBuffer* new_input_buffer() {
    InputBuffer* input_buffer = new InputBuffer;
    input_buffer->buffer = NULL;
    input_buffer->buffer_length = 0;
    input_buffer->input_length = 0;
    return input_buffer;
}
void close_input_buffer(InputBuffer* input_buffer) {
    free(input_buffer->buffer);  //显然,是用堆分配的空间
    free(input_buffer);
}

void read_input(InputBuffer* input_buffer) {
    ssize_t bytes_read =
        getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);
    if (bytes_read <= 0) {
        printf("Error reading input\n");
        exit(EXIT_FAILURE);  // define EXIT_FAILURE 1
    }

    // 忽略换行符
    input_buffer->input_length = bytes_read - 1;
    input_buffer->buffer[bytes_read - 1] =
        0;  // c语言字符串结束标志为'\0',数值为0
}

void print_prompt() { cout << "db > "; }
//
MetaCommandResult do_meta_command(InputBuffer* input_buffer, Table* table) {
    if (strcmp(input_buffer->buffer, ".exit") == 0) {
        db_close(table);
        exit(EXIT_SUCCESS);
    } else if (strcmp(input_buffer->buffer, ".constants") == 0) {
        cout << "Constants:" << endl;
        print_constatns();
        return META_COMMAND_SUCCESS;
    } else if (strcmp(input_buffer->buffer, ".btree") == 0) {
        cout << "Tree:" << endl;
        print_leaf_node(get_page(table->pager, 0));
        return META_COMMAND_SUCCESS;
    } else {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

PrepareResult prepare_statement(InputBuffer* input_buffer,
                                Statement* statement) {
    if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
        statement->type = STATEMENT_INSERT;
        return prepare_insert(input_buffer, statement);
    }
    if (strcmp(input_buffer->buffer, "select") == 0) {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }

    return PREPARE_UNRECOGNIZED_STATEMENT;
}

PrepareResult prepare_insert(InputBuffer* input_buffer, Statement* statement) {
    statement->type = STATEMENT_INSERT;
    char* keyword = strtok(input_buffer->buffer, " ");  //其实是insert字符串
    char* id_string = strtok(NULL, " ");
    char* username = strtok(NULL, " ");
    char* email = strtok(NULL, " ");
    if (id_string == NULL || username == NULL || email == NULL) {
        return PREPARE_SYNTAX_ERROR;
    }
    int id = atoi(id_string);
    if (id < 0)
        return PREPARE_NEGATIVE_ID;
    if (strlen(username) > COLUMN_USERNAME_SIZE ||
        strlen(email) > COLUMN_EMAIL_SIZE)
        return PREPARE_STRING_TOO_LONG;
    statement->row_to_insert.id = id;
    strcpy(statement->row_to_insert.username, username);
    strcpy(statement->row_to_insert.email, email);
    return PREPARE_SUCCESS;
}

//
// well this func just print a row
void print_row(Row* row) {
    cout << "(" << row->id << ", " << row->username << ", " << row->email << ")"
         << endl;
}
/*
func: serialize_row
desc: 序列化行，将行存储到dest指针对应的空间
ret: 无
*/
void serialize_row(Row* source, void* destination) {
    memcpy((byte*)destination + ID_OFFSET, &(source->id), ID_SIZE);
    strncpy((char*)destination + USERNAME_OFFSET, (char*)&(source->username),
            USERNAME_SIZE);
    strncpy((char*)destination + EMAIL_OFFSET, (char*)&(source->email),
            EMAIL_SIZE);
}
/*
func: deserialize_row
param:
    source - 指向内存中行的指针
    dest - 存储转化后的行的Row结构体指针
desc: 反序列化行，将source转化成Row结构体
ret: 无
*/
void deserialize_row(void* source, Row* destination) {
    memcpy(&(destination->id), (byte*)source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->username), (byte*)source + USERNAME_OFFSET,
           USERNAME_SIZE);
    memcpy(&(destination->email), (byte*)source + EMAIL_OFFSET, EMAIL_SIZE);
}

void print_constatns() {
    cout << "ROW_SIZE: " << ROW_SIZE << endl
         << "COMMON_NODE_HEADER_SIZE: " << COMMON_NODE_HEADER_SIZE << endl
         << "LEAF_NODE_HEADER_SIZE: " << LEAF_NODE_HEADER_SIZE << endl
         << "LEAF_NODE_CELL_SIZE: " << LEAF_NODE_CELL_SIZE << endl
         << "LEAF_NODE_SPACE_FOR_CELLS: " << LEAF_NODE_SPACE_FOR_CELLS << endl
         << "LEAF_NODE_MAX_CELLS: " << LEAF_NODE_MAX_CELLS << endl;
}

void print_leaf_node(void* node) {
    uint32_t num_cells = *(leaf_node_num_cells(node));
    cout << "leaf (size " << num_cells << ")" << endl;
    for (uint32_t i = 0; i < num_cells; i++) {
        uint32_t key = *(leaf_node_key(node, i));
        cout << "  - " << i << " : " << key << endl;
    }
}

/*************** 增删查改 ***************/

// insert
ExecuteResult execute_insert(Statement* statement, Table* table) {
    void* node = get_page(table->pager, table->root_page_num);  // 获取根节点
    if (*(leaf_node_num_cells(node)) >= LEAF_NODE_MAX_CELLS) {
        return EXECUTE_TABLE_FULL;
    }

    Row* row_to_insert = &(statement->row_to_insert);
    Cursor* cursor = table_end(table);
    leaf_node_insert(cursor, row_to_insert->id, row_to_insert);

    free(cursor);  // cursor是malloc分配的,要记得free

    return EXECUTE_SUCCESS;
}
ExecuteResult execute_select(Statement* statement, Table* table) {
    Row row;
    Cursor* cursor = table_start(table);

    for (uint32_t i = 0;
         i < *(leaf_node_num_cells(table->pager->pages[table->root_page_num]));
         i++) {
        deserialize_row(cursor_value(cursor), &row);
        cursor_advance(cursor);
        print_row(&row);
    }

    free(cursor);  // cursor是用malloc分配的,不要忘了free.

    return EXECUTE_SUCCESS;
}
ExecuteResult execute_statement(Statement* statement, Table* table) {
    switch (statement->type) {
        case (STATEMENT_INSERT):
            return execute_insert(statement, table);
        case (STATEMENT_SELECT):
            return execute_select(statement, table);
        default:
            return EXECUTE_SUCCESS;
    }
}

/*************** database memory manage ***************/

Table* db_open(const char* filename) {
    Pager* pager = pager_open(filename);

    Table* table = (Table*)malloc(sizeof(Table));
    table->pager = pager;
    table->root_page_num = 0;

    if (pager->num_pages == 0) {
        //新的数据库文件.初始化page 0 为叶子节点
        void* root_node = get_page(pager, 0);
        initialize_leaf_node(root_node);
    }

    return table;
}

Pager* pager_open(const char* filename) {
    int fd = open(filename,
                  O_RDWR |      //读写模式
                      O_CREAT,  //不存在则新建
                  S_IWUSR |     //用户写入权限
                      S_IRUSR   //用户读取权限
    );
    if (fd == -1) {
        cout << "Unable to open file" << endl;
        exit(EXIT_FAILURE);
    }
    off_t file_length = lseek(fd, 0, SEEK_END);

    Pager* pager = (Pager*)malloc(sizeof(Pager));
    pager->file_descriptor = fd;
    pager->file_length = file_length;
    // get num of page of a db file
    pager->num_pages = file_length / PAGE_SIZE;

    //因为现在即使cell未满也会占满一页,所以file_length必定是PAGE_SIZE的整数倍
    if (file_length % PAGE_SIZE != 0) {
        cout << "Db file is not a whole number of pages. Corrput file." << endl;
        exit(EXIT_FAILURE);
    }

    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++)
        pager->pages[i] = NULL;
    return pager;
}

/*
func: get_page
params: pager-页对象 page_num-页的下标
desc: 获取下标为page_num的页,如果不在db文件中就新建一页.
ret: 指向该页的指针
*/
void* get_page(Pager* pager, uint32_t page_num) {
    if (page_num >= TABLE_MAX_PAGES) {
        cout << "Tried to fetch page number out of bounds. " << page_num + 1
             << " > " << TABLE_MAX_PAGES << endl;
        exit(EXIT_FAILURE);
    }
    if (pager->pages[page_num] == NULL) {
        //缓存未命中，分配内存然后从文件中读取
        void* page = malloc(PAGE_SIZE);
        uint32_t num_pages = pager->file_length / PAGE_SIZE;  //总页数

        // 如果请求的页在数据库文件中，就读取到内存
        if (page_num <= num_pages - 1) {
            lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
            ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
            if (bytes_read == -1) {
                cout << "Error reading file: " << errno << endl;
                exit(EXIT_FAILURE);
            }
        }
        pager->pages[page_num] = page;

        if (page_num >= pager->num_pages)
            pager->num_pages = page_num + 1;
    }
    return pager->pages[page_num];
}

/*
func: db_close
param: table
desc: 关闭数据库，将table->pager->num_pages页写入数据库文件，释放空间
*/
void db_close(Table* table) {
    Pager* pager = table->pager;
    for (uint32_t i = 0; i < pager->num_pages; i++) {  //写入被填满的页
        if (pager->pages[i] == NULL)
            continue;  //如果页没有读取到内存，说明该页数据没有改动，跳过此页
        pager_flush(pager, i);
        free(pager->pages[i]);
        pager->pages[i] = NULL;
    }

    int result = close(pager->file_descriptor);
    if (result == -1) {
        cout << "Error closing db file." << endl;
        exit(EXIT_FAILURE);
    }
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {  // check
        void* page = pager->pages[i];
        if (page != NULL) {
            free(page);
            pager->pages[i] = NULL;
        }
    }
    free(pager);
    free(table);
}

/*
func: pager_flush
params: pager   page_num-页的下标
desc: 向数据库文件中向第page_num对应的偏移处写入PAGE_SIZE个字节
*/
void pager_flush(Pager* pager, uint32_t page_num) {
    if (pager->pages[page_num] == NULL) {
        cout << "Tried to flush null page" << endl;
        exit(EXIT_FAILURE);
    }

    off_t offset =
        lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);

    if (offset == -1) {
        cout << "Error seeking: " << errno << endl;
        exit(EXIT_FAILURE);
    }

    ssize_t bytes_written =
        write(pager->file_descriptor, pager->pages[page_num], PAGE_SIZE);

    if (bytes_written == -1) {
        cout << "Error writing: " << errno << endl;
        exit(EXIT_FAILURE);
    }
}
//

/*************** Cursor func ***************/

/*
func: table_start
desc: 创建一个指向表的根节点的第一个cell的cursor
ret: 指向表开头的cursor
*/
Cursor* table_start(Table* table) {
    Cursor* cursor = (Cursor*)malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->page_num = table->root_page_num;
    cursor->cell_num = 0;

    void* root_node = get_page(table->pager, table->root_page_num);
    uint32_t num_cells = *(leaf_node_num_cells(root_node));
    // 如果表的根节点的cell数量为0,那么表示cursor到达了table的末尾
    cursor->end_of_table = (num_cells == 0);

    return cursor;
}

/*
func: table_end
desc:
创建一个指向表的根节点的最后一个cell后一个cell位置的cursor(即page.num_cells)
ret: 指向表最后一个cell的后一个cell的cursor
*/
Cursor* table_end(Table* table) {
    Cursor* cursor = (Cursor*)malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->page_num = table->root_page_num;

    void* root_node = get_page(table->pager, table->root_page_num);
    cuint32 num_cells = *(leaf_node_num_cells(root_node));
    cursor->cell_num = num_cells;
    cursor->end_of_table = true;

    return cursor;
}

/*
func: cursor_value
desc: 根据cursor指向位置,返回指向该cell的内存地址的指针
ret: 指向cursor->row_num对应的行的指针
*/
void* cursor_value(Cursor* cursor) {
    cuint32 page_num = cursor->page_num;
    void* page = get_page(cursor->table->pager, page_num);  //获取该页的内存地址

    return leaf_node_value(page, cursor->cell_num);
}

/*
func: cursor_advance
desc: 将cursor移动到下一行,如果到达最后一行就 end_of_table=true
*/
void cursor_advance(Cursor* cursor) {
    cuint32 page_num = cursor->page_num;
    void* node = get_page(cursor->table->pager, page_num);

    cursor->cell_num += 1;
    if (cursor->cell_num >= *(leaf_node_num_cells(node)))
        cursor->end_of_table = true;
}

/*************** leaf node locate ***************/

/*
func: leaf_node_num_cells
param: node - 节点(即页)所在的内存空间
desc: 获取叶子节点目前存储的cell数量,返回指向该数值的int指针
ret: uint32_t指针,指向cell数量
*/
uint32_t* leaf_node_num_cells(void* node) {
    return (uint32_t*)((byte*)node + LEAF_NODE_NUM_CELLS_OFFSET);
}
/*
func: leaf_node_cell
param: node cell_num - cell下标
desc: 获取下标cell_num对应的cell的地址
ret: 指向存储cell的内存区域的指针
*/
void* leaf_node_cell(void* node, uint32_t cell_num) {
    return (void*)((byte*)node + LEAF_NODE_HEADER_SIZE +
                   cell_num * LEAF_NODE_CELL_SIZE);
}
/*
func: leaf_node_key
param: node cell_num - cell下标
desc: 获取cell_num对应cell的key值
ret: 指向该key值的int指针
*/
uint32_t* leaf_node_key(void* node, uint32_t cell_num) {
    return (uint32_t*)leaf_node_cell(node, cell_num);
}
/*
func: leaf_node_value
param: node cell_num
desc: 获取cell_num对应的cell的value的内存地址
ret: 指向下标为cell_num的cell的value的void指针(value是序列化形式)
*/
void* leaf_node_value(void* node, uint32_t cell_num) {
    return (void*)((byte*)leaf_node_cell(node, cell_num) +
                   LEAF_NODE_VALUE_OFFSET);
}
/*
func: init_leaf_node
desc: 初始化一个叶子节点,只把node的cell数量赋值为0
ret: none
*/
void initialize_leaf_node(void* node) {
    *leaf_node_num_cells(node) = 0;  //令node的cell数量为0
}
/*
func: leaf_node_insert
param: cursor uint32_t value
desc: 将key&value插入到cursor->num_cell对应的空间
ret: none
*/
void leaf_node_insert(Cursor* cursor, uint32_t key, Row* value) {
    void* node = get_page(cursor->table->pager, cursor->page_num);

    uint32_t num_cells = *leaf_node_num_cells(node);
    if (num_cells >= LEAF_NODE_MAX_CELLS) {
        // node full,节点已满
        cout << "Need to implement splitting a leaf node." << endl;
        exit(EXIT_FAILURE);
    }

    if (cursor->cell_num < num_cells) {
        // Make room for new cell
        // [cell_num,num_cells]间的cell后移一位
        for (uint32_t i = num_cells; i > cursor->cell_num; i--)
            memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i - 1),
                   LEAF_NODE_CELL_SIZE);
    }
    // now space of cell_num is empty
    *(leaf_node_num_cells(node)) += 1;
    *(leaf_node_key(node, cursor->cell_num)) = key;
    serialize_row(value, leaf_node_value(node, cursor->cell_num));
}
// func region END