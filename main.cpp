#include <fcntl.h>
#include <unistd.h>

#include <cstdio>
#include <cstring>
#include <iostream>

// defs
#include "struct.h"

using namespace std;

/*************** HANDLE INPUT ***************/

InputBuffer* new_input_buffer();
void print_prompt();
void read_input(InputBuffer* input_buffer);
void close_input_buffer(InputBuffer* input_buffer);

/*************** HANDLE STATEMENT ***************/

PrepareResult prepare_statement(InputBuffer* input_buffer,
                                Statement* statement);
PrepareResult prepare_insert(InputBuffer* input_buffer, Statement* statement);

/*************** ROW (DE)SERIALIZE & PRINT ***************/

void serialize_row(Row* source,
                   void* destination);  //序列化数据，结构体->序列化紧凑数据
void deserialize_row(void* source,
                     Row* destination);  //逆序列化紧凑数据->结构体
void print_row(Row* row);                //打印一行数据
//打印一些常量
void print_constatns();
//打印缩进
void indent(uint32_t level);
//打印整棵树
void print_tree(Pager* pager, uint32_t page_num, uint32_t indentation_level);

/*************** COMMAND ***************/

MetaCommandResult do_meta_command(InputBuffer* input_buffer, Table* table);
ExecuteResult execute_insert(Statement* statement, Table* table);
ExecuteResult execute_select(Statement* statement, Table* table);
ExecuteResult execute_statement(Statement* statement, Table* table);

/*************** Pager ***************/

Pager* pager_open(const char* filename);
Table* db_open(const char* filename);
void db_close(Table* table);
void* get_page(Pager* pager, uint32_t page_num);
void pager_flush(Pager* pager, uint32_t page_num);
uint32_t get_unused_page_num(Pager* pager);

/*************** Cursor ***************/

Cursor* table_start(Table* table);
Cursor* table_find(Table* table, uint32_t key);
void* cursor_value(Cursor* cursor);
void cursor_advance(Cursor* cursor);

/*************** NODE OPERATION ***************/

/********* LEAF NODE *********/

// Field

uint32_t* leaf_node_num_cells(void* node);
void* leaf_node_cell(void* node, uint32_t cell_num);
uint32_t* leaf_node_key(void* node, uint32_t cell_num);
void* leaf_node_value(void* node, uint32_t cell_num);
uint32_t* leaf_node_next_leaf(void* node);

//
void initialize_leaf_node(void* node);
void leaf_node_insert(Cursor* cursor, uint32_t key, Row* value);
Cursor* leaf_node_find(Table* table, uint32_t page_num, uint32_t key);
void leaf_node_split_and_insert(Cursor* cursor, uint32_t key, Row* value);

/********* ROOT NODE *********/

bool is_node_root(void* node);
void create_new_root(Table* table, uint32_t page_num);
void set_node_root(void* node, bool root);

/********* INTERNAL NODE *********/

void initialize_internal_node(void* node);
uint32_t* internal_node_num_keys(void* node);
uint32_t* internal_node_right_child(void* node);
void* internal_node_cell(void* node, uint32_t cell_num);
uint32_t* internal_node_child(void* node, uint32_t child_num);
uint32_t* internal_node_key(void* node, uint32_t key_num);

Cursor* internal_node_find(Table* table, uint32_t page_num, uint32_t key);
uint32_t internal_node_find_child(void* node, uint32_t key);
void update_internal_node_key(void* node, uint32_t old_key, uint32_t new_key);
void internal_node_insert(Table* table, uint32_t parent_page_num,
                          uint32_t child_page_num);

void internal_node_split_and_insert(Table* table,uint32_t parent_page_num,uint32_t child_page_num,uint32_t index_to_insert);

void create_new_internal_root(Table* table,uint32_t right_child_page_num);

uint32_t get_internal_node_max_key(Pager* pager,void* node);

/********* COMMOM NODE *********/

NodeType get_node_type(void* node);
void set_node_type(void* node, NodeType type);
uint32_t get_node_max_key(void* node);
uint32_t* node_parent(void* node);

/*************** TEST ***************/
void test(Table* table, char* buffer) {
    static uint32_t i = 0;
    //
    Statement s1;
    s1.type = STATEMENT_INSERT;
    char c1[] = "ppp";
    memcpy(&s1.row_to_insert.username, (void*)c1, sizeof(char) * 4);
    memcpy(&s1.row_to_insert.email, (void*)c1, sizeof(char) * 4);
    //
    int num = 0;
    int ptr=0;
    sscanf(buffer, ".test %d %d", &ptr,&num);
    if (ptr>0){
        cout<<"move pointer to "<<ptr<<endl;
        i=ptr;
    }
    if (num <= 0){
        cout<<"no times"<<endl;
        return;
    }
    //
    int j = i + num;
    for (; i < j; i++) {
        s1.row_to_insert.id = i;
        execute_insert(&s1, table);
    }
}

/********************************************************/
/*************** MAIN FUNCTION ***************/
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
            case (EXECUTE_DUPLICATE_KEY):
                cout << "Error: Duplicate key." << endl;
                break;
            default:
                cout << "Unrecognized Error." << endl;
                break;
        }
    }
    return 0;
}

//
/*************** FUNCTION FIELD ***************/

/************ INPUT & convert input to statement & ROW_SERIALIZE **************/
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
        print_tree(table->pager, 0, 0);
        return META_COMMAND_SUCCESS;
    } else if (strncmp(input_buffer->buffer, ".test", 5) == 0) {
        test(table, input_buffer->buffer);
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
         << "LEAF_NODE_MAX_CELLS: " << LEAF_NODE_MAX_CELLS << endl
         << "INTERNAL_NODE_MAX_CELLS: " << INTERNAL_NODE_MAX_CELLS << endl;
}

void indent(uint32_t level) {
    for (uint32_t i = 0; i < level; i++)
        cout << "  ";
}

void print_tree(Pager* pager, uint32_t page_num, uint32_t indentation_level) {
    void* node = get_page(pager, page_num);
    uint32_t num_keys, child;

    switch (get_node_type(node)) {
        case (NODE_LEAF):
            num_keys = *(leaf_node_num_cells(node));
            indent(indentation_level);
            cout << "- leaf (size " << num_keys << ")" << endl;
            for (uint32_t i = 0; i < num_keys; i++) {
                indent(indentation_level + 1);
                cout << "- " << *(leaf_node_key(node, i)) << endl;
            }
            break;
        case (NODE_INTERNAL):
            num_keys = *(internal_node_num_keys(node));
            indent(indentation_level);
            cout << "- internal (size " << num_keys << ")" << endl;
            for (uint32_t i = 0; i < num_keys; i++) {
                //打印子节点(除了最右边的)
                child = *(internal_node_child(node, i));
                print_tree(pager, child, indentation_level + 1);

                indent(indentation_level + 1);
                cout << "- key " << *(internal_node_key(node, i)) << endl;
            }
            //打印最右边的子节点
            child = *internal_node_right_child(node);
            print_tree(pager, child, indentation_level + 1);
            break;
    }
}

/*************** COMMAND ***************/

/*
func: execute_insert
param: statement table
desc: 根据ID在表中合适的位置插入(升序)
ret: 操作成功与否
*/
ExecuteResult execute_insert(Statement* statement, Table* table) {
    Row* row_to_insert = &(statement->row_to_insert);
    uint32_t key_to_insert = row_to_insert->id;
    Cursor* cursor = table_find(table, key_to_insert);
    //获取cursor指向的node及相关变量
    void* node = get_page(table->pager, cursor->page_num);
    cuint32 num_cells = *(leaf_node_num_cells(node));

    if (cursor->cell_num < num_cells) {
        // 如果cell_num>=num_cells,说明在末尾插入,不需要判断重复(key比叶子节点所有已有key都大)
        uint32_t key_at_index = *(leaf_node_key(node, cursor->cell_num));
        if (key_at_index == key_to_insert)
            return EXECUTE_DUPLICATE_KEY;
    }
    leaf_node_insert(cursor, row_to_insert->id, row_to_insert);

    free(cursor);  // cursor是malloc分配的,要记得free

    return EXECUTE_SUCCESS;
}
ExecuteResult execute_select(Statement* statement, Table* table) {
    Row row;
    Cursor* cursor = table_start(table);

    while (cursor->end_of_table != true) {
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
        //新的数据库文件.初始化page 0 为叶子节点&root
        void* root_node = get_page(pager, 0);
        initialize_leaf_node(root_node);
        set_node_root(root_node, true);
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

/*
func: get_unused_page_num
param: pager
desc: 获取一个未使用过的页
除非开始重复使用被释放的页,否则分配的页始终在数据库的末尾
ret: 页的下标
*/
uint32_t get_unused_page_num(Pager* pager) { return pager->num_pages; }

/*************** Cursor FUNCTION ***************/

/*
func: table_start
desc: 创建一个指向表的最左边叶子节点的开头的元素
ret: 指向表开头的cursor
*/
Cursor* table_start(Table* table) {
    Cursor* cursor = table_find(table, 0);

    void* node = get_page(table->pager, cursor->page_num);
    uint32_t num_cells = *(leaf_node_num_cells(node));
    cursor->end_of_table = (num_cells == 0);

    return cursor;
}

/*
func: table_find
param: table key
desc: return the pos of the given key,
if key is not present,ret the pos where it should be inserted.
ret: 指向该位置的cursor指针
*/
Cursor* table_find(Table* table, uint32_t key) {
    uint32_t root_page_num = table->root_page_num;
    void* root_node = get_page(table->pager, root_page_num);

    if (get_node_type(root_node) == NODE_LEAF)
        return leaf_node_find(table, root_page_num, key);
    else
        return internal_node_find(table, root_page_num, key);
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
desc: 在叶子节点内,cursor指向下一行,如果到达叶子的末尾,跳向兄弟叶子或者到达终点.
*/
void cursor_advance(Cursor* cursor) {
    cuint32 page_num = cursor->page_num;
    void* node = get_page(cursor->table->pager, page_num);

    cursor->cell_num += 1;
    if (cursor->cell_num >= *(leaf_node_num_cells(node))) {
        uint32_t next_page_num = *(leaf_node_next_leaf(node));
        if (next_page_num == 0) {
            // This was rightmost leaf
            cursor->end_of_table = true;
        } else {
            cursor->page_num = next_page_num;
            cursor->cell_num = 0;
        }
    }
}

/*************** LEAF NODE ***************/

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
func: leaf_node_next_leaf
param: node
desc: 获得叶子节点右边的叶子节点的page number
ret: 指向该page number的int指针
*/
uint32_t* leaf_node_next_leaf(void* node) {
    return (uint32_t*)((byte*)node + LEAF_NODE_NEXT_LEAF_OFFSET);
}

/*
func: init_leaf_node
desc: 初始化一个叶子节点,cell数量为0,type为leaf
ret: none
*/
void initialize_leaf_node(void* node) {
    set_node_type(node, NODE_LEAF);
    set_node_root(node, false);
    *(leaf_node_num_cells(node)) = 0;
    *(leaf_node_next_leaf(node)) = 0;  // 0 represents no sibling
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
        // 分裂叶子节点
        leaf_node_split_and_insert(cursor, key, value);
        return;
    }

    if (cursor->cell_num < num_cells) {
        // Make room for new cell
        // [cell_num,num_cells-1]间的cell后移一位
        for (uint32_t i = num_cells; i > cursor->cell_num;
             i--)  // node[i-1]>=cell_num
            memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i - 1),
                   LEAF_NODE_CELL_SIZE);
    }
    // now space of cell_num is empty
    *(leaf_node_num_cells(node)) += 1;
    // assign key&value
    *(leaf_node_key(node, cursor->cell_num)) = key;
    serialize_row(value, leaf_node_value(node, cursor->cell_num));
}
/*
func: leaf_node_find
param: page_num-下标 key
desc:
根据key值搜索cell,找不到就返回插入的位置(之后该位置及该位置之后的元素全部后移)
ret: 指向cell位置的cursor
*/
Cursor* leaf_node_find(Table* table, uint32_t page_num, uint32_t key) {
    void* node = get_page(table->pager, page_num);
    uint32_t num_cells = *(leaf_node_num_cells(node));

    Cursor* cursor = (Cursor*)malloc(sizeof(cursor));
    cursor->table = table;
    cursor->page_num = page_num;

    // Binary search
    uint32_t min_cell_index = 0;
    // cell的最大下标+1
    uint32_t one_past_max_index = num_cells;
    // cellNum_Range= [min_c_index,one_p_m_index)
    while (min_cell_index < one_past_max_index) {
        uint32_t mid_cell_index = (min_cell_index + one_past_max_index) / 2;
        uint32_t mid_cell_key = *(leaf_node_key(node, mid_cell_index));
        if (key == mid_cell_key) {
            cursor->cell_num = mid_cell_index;
            return cursor;
        } else if (key < mid_cell_key) {
            // shrink right range to mid [l,mid-1]
            one_past_max_index = mid_cell_index;
        } else {  // key>mid
            // shrink left [mid+1,r)
            min_cell_index = mid_cell_index + 1;
        }
    }
    // cant find key,so min_cell_index=one_past_max_index
    // and this is where to insert new key
    cursor->cell_num = min_cell_index;
    return cursor;
}

/*
func: leaf_node_split_and_insert
param: cursor-指向插入的位置 key-待插入cell的键 row
desc: 分裂叶子节点
ret: none
*/
void leaf_node_split_and_insert(Cursor* cursor, uint32_t key, Row* value) {
    /*
    创建一个新的节点，移入一半记录(cell).
    把新的cell插入其中一个节点.
    更新父节点或者创建一个新的父节点.
    */
    // old node
    void* old_node = get_page(cursor->table->pager, cursor->page_num);
    uint32_t old_max_key = get_node_max_key(old_node);
    // new node
    uint32_t new_page_num = get_unused_page_num(cursor->table->pager);
    void* new_node = get_page(cursor->table->pager, new_page_num);
    initialize_leaf_node(new_node);
    *(node_parent(new_node)) = *(node_parent(old_node));
    /*
    所有已有的key外加新的key应该被均匀分割成两部分 - 旧节点(左边) 新节点(右边)
    从右向左,把每个key移动到正确的位置.
    需要移动LEAF_NODE_MAX_CELLS+1次,因为还要插入新cell
    [0,LEFT_COUNT-1]之间的放到旧节点
    [LEFT_COUNT,MAX_CELL-1]+新插入的cell 放到新节点
    */
    for (int32_t i = LEAF_NODE_MAX_CELLS; i >= 0; i--) {
        void* destination_node;  //下标为i的cell应该放入的节点

        if (i >=
            LEAF_NODE_LEFT_SPLIT_COUNT)  //放到右边新叶子[LEFT_COUNT,MAX_CELLS]
            destination_node = new_node;
        else  //左边的旧叶子 [0,LEFT_COUNT-1]
            destination_node = old_node;

        // cell在目标节点的下标
        uint32_t index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT;
        // destination就是目标地址,对应逻辑下标为i的元素
        void* destination = leaf_node_cell(destination_node, index_within_node);

        /*
        本循环中的i是假设元素已经插入之后的下标,但只是逻辑上的下标,物理位置没变
        cursor->cell_num是待插入元素在插入之后的下标,
        逻辑下标(i)>cell_num的对应元素在原节点中的下标为i-1
        逻辑下标(i)<cell_num的元素的下标没有改变
        cell_num就是待插入元素的下标
        */
        if (i == cursor->cell_num) {  //待插入的cell,需要进行一次序列化
            *(leaf_node_key(destination_node, index_within_node)) = key;
            serialize_row(value,
                          leaf_node_value(destination_node, index_within_node));
        } else if (i > cursor->cell_num) {
            memcpy(destination, leaf_node_cell(old_node, i - 1),
                   LEAF_NODE_CELL_SIZE);
        } else {
            memcpy(destination, leaf_node_cell(old_node, i),
                   LEAF_NODE_CELL_SIZE);
        }
    }  // 在循环结束后,已经分裂成了两个叶子节点 old & new
    //分裂后要更新一些字段
    *(leaf_node_num_cells(old_node)) = LEAF_NODE_LEFT_SPLIT_COUNT;
    *(leaf_node_num_cells(new_node)) = LEAF_NODE_RIGHT_SPLIT_COUNT;
    // sibling
    *(leaf_node_next_leaf(new_node)) = *(leaf_node_next_leaf(old_node));
    *(leaf_node_next_leaf(old_node)) = new_page_num;  //新叶子节点的页码

    if (is_node_root(old_node)) {
        return create_new_root(cursor->table, new_page_num);
    } else {
        uint32_t parent_page_num = *(node_parent(old_node));
        uint32_t new_max_key = get_node_max_key(old_node);
        void* parent = get_page(cursor->table->pager, parent_page_num);
        // update old_key to new_key (if old_key exists)
        update_internal_node_key(parent, old_max_key, new_max_key);
        // add a child_ptr & key pair in parent node(internal)
        internal_node_insert(cursor->table, parent_page_num, new_page_num);
    }
}

/*************** COMMON NODE ***************/

/*
func: get_node_type
param: void* node
desc: 获取node的类型
ret: internal | leaf
*/
NodeType get_node_type(void* node) {
    uint8_t value = *((uint8_t*)((byte*)node + NODE_TYPE_OFFSET));
    return (NodeType)value;
}
/*
func: set_node_type
param: node type
desc: set node's type to param type
ret: none
*/
void set_node_type(void* node, NodeType type) {
    uint8_t value = type;
    *((uint8_t*)((byte*)node + NODE_TYPE_OFFSET)) = value;
}

/*
func: get_node_max_key
param: pager node
desc: 获取node的key的最大值
ret: (int)KEY_max
*/
uint32_t get_node_max_key(void* node) {
    switch (get_node_type(node)) {
        case NODE_INTERNAL:
            return *(internal_node_key(node,*(internal_node_num_keys(node))-1));
        case NODE_LEAF:
            return *(leaf_node_key(node, *(leaf_node_num_cells(node)) - 1));
        default:
            cout << "UNRECOGNIZED NODE TYPE: " << get_node_type(node) << endl;
            exit(EXIT_FAILURE);
    }
}

/*
func: node_parent
param: node
desc: 获取node的父节点的页码
ret: 指向页码的int指针
*/

uint32_t* node_parent(void* node) {
    return (uint32_t*)((byte*)node + PARENT_POINTER_OFFSET);
}

/*************** ROOT NODE ***************/

/*
func: is_node_root
param: page
desc: 判断page是否为root
ret: root-true 非root-false
*/
bool is_node_root(void* node) {
    return (bool)(*(uint8_t*)((byte*)node + IS_ROOT_OFFSET));
}

/*
func: set_node_root
param: node is_root
desc: 根据is_root设置node是否为根节点
ret: none
*/
void set_node_root(void* node, bool is_root) {
    uint8_t value = is_root;
    *((uint8_t*)((byte*)node + IS_ROOT_OFFSET)) = value;
}

/*
func: create_new_root
param: table right_child_page_num
desc: 创建一个新的根节点
ret: none
*/
void create_new_root(Table* table, uint32_t right_child_page_num) {
    /*
    处理根节点分离.在这之前已经完成了元素均分(并且重新赋值了包含元素个数),目前根节点相当于左孩子.
    旧的根节点拷贝到新页,变成了左孩子
    右孩子作为参数传递.
    重新初始化根节点对应页以包含新的根节点.
    新的根节点指向两个孩子.
    */
    void* root = get_page(table->pager, table->root_page_num);
    void* right_child = get_page(table->pager, right_child_page_num);
    uint32_t left_child_page_num = get_unused_page_num(table->pager);
    void* left_child = get_page(table->pager, left_child_page_num);
    // left node
    memcpy(left_child, root, PAGE_SIZE);
    set_node_root(left_child, false);
    /* ROOT NODE(internal) */
    initialize_internal_node(root);
    set_node_root(root, true);
    // only have 1 key
    *(internal_node_num_keys(root)) = 1;
    // left child pointer
    *(internal_node_child(root, 0)) = left_child_page_num;
    // maxKey(left_child)
    uint32_t left_child_max_key = get_node_max_key(left_child);
    *(internal_node_key(root, 0)) = left_child_max_key;
    // right child pointer
    *(internal_node_right_child(root)) = right_child_page_num;
    /* update childNode's PARENT */
    *(node_parent(left_child)) = table->root_page_num;
    *(node_parent(right_child)) = table->root_page_num;
}

/********* INTERNAL NODE *********/
/*
func: initialize_internal_node
param: node
desc: 初始化为内部节点,设置type,is_root以及num_keys
ret: none
*/
void initialize_internal_node(void* node) {
    set_node_type(node, NODE_INTERNAL);
    set_node_root(node, false);
    *(internal_node_num_keys(node)) = 0;
}

/*
func: internal_node_num_keys
param: node
desc: 获取内部节点存储的key的数量
ret: 指向key数量的int指针
*/
uint32_t* internal_node_num_keys(void* node) {
    return (uint32_t*)((byte*)node + INTERNAL_NODE_NUM_KEYS_OFFSET);
}

/*
func: internal_node_right_child
param: node
desc: 获取*最右边*的子节点的page number
ret: 指向该子节点的pager number的int指针
*/
uint32_t* internal_node_right_child(void* node) {
    return (uint32_t*)((byte*)node + INTERNAL_NODE_RIGHT_CHILD_OFFSET);
}

/*
func: internal_node_cell
param: node cell_num
desc: 获取指向cells[cell_num]元素的指针
ret: 指向下标为cell_num的cell的指针
*/
void* internal_node_cell(void* node, uint32_t cell_num) {
    return (void*)((byte*)node + INTERNAL_NODE_HEADER_SIZE +
                   cell_num * INTERNAL_NODE_CELL_SIZE);
}

/*
func: internal_node_child
param: node child_num-子节点下标
desc: 获取node的下标为child_num的子节点(包括最右边的节点)的页码
ret: 该子节点的页码
*/
uint32_t* internal_node_child(void* node, uint32_t child_num) {
    uint32_t num_keys = *(internal_node_num_keys(node));
    /*
    对于internal节点,Num(key)=Num(child)-1
    Index(child)=[0,Num(child)-1]=[0,Num(key)]
    */
    if (child_num > num_keys) {
        cout << "Tried to access child_num " << child_num << " > num_keys "
             << num_keys << endl;
        exit(EXIT_FAILURE);
    } else if (child_num == num_keys) {
        //最右边的节点不是cell,而是单纯的一个页码
        return internal_node_right_child(node);
    } else {
        // cell的布局是ptr在前,key在后
        return (uint32_t*)internal_node_cell(node, child_num);
    }
}

/*
func: internal_node_key
param: node key_num
desc: 获取下标为key_num的cell的key
ret: 指向该key数据的int指针
*/
uint32_t* internal_node_key(void* node, uint32_t key_num) {
    return (uint32_t*)((byte*)internal_node_cell(node, key_num) +
                       INTERNAL_NODE_CHILD_SIZE);
}

/*
func: internal_node_find
param: table page_num key
desc: 找到key的位置或者key应该插入的位置
ret: 指向该位置(叶子节点内)的cursor
*/
Cursor* internal_node_find(Table* table, uint32_t page_num, uint32_t key) {
    void* node = get_page(table->pager, page_num);

    //获取key对应的子节点下标
    uint32_t child_index = internal_node_find_child(node, key);
    uint32_t child_num = *(internal_node_child(node, child_index));  //页码
    void* child = get_page(table->pager, child_num);
    switch (get_node_type(child)) {
        case NODE_LEAF:
            return leaf_node_find(table, child_num, key);
        case NODE_INTERNAL:
            return internal_node_find(table, child_num, key);
        default:
            cout << "UNRECOGNIZED NODE TYPE: " << get_node_type(child) << endl;
            exit(EXIT_FAILURE);
    }
}

/*
func: internal_node_find_child
param: node key
desc: 根据key找到对应的child的下标
如果key>MaxKey(node),返回最右边节点的下标
如果key不存在且
<MaxKey(node),那么返回的是第一个大于key的原node中的key的下标(即key应该插入的目标子节点的下标)
ret: 该child ptr的下标
*/
uint32_t internal_node_find_child(void* node, uint32_t key) {
    // Return the INDEX of child which contains the given KEY.
    uint32_t num_keys = *(internal_node_num_keys(node));
    uint32_t min_index = 0;
    uint32_t max_index = num_keys;  // Num(child)=Num(key)+1;

    while (min_index < max_index) {  //[min,max)
        uint32_t mid_index = (min_index + max_index) / 2;
        uint32_t mid_key = *(internal_node_key(node, mid_index));
        if (mid_key >= key)
            max_index = mid_index;
        else
            min_index = mid_index + 1;
    }  // 退出循环后,min_i=max_i,就是子节点指针的下标,或者如果key>MaxKey(node),那么下标就是最右边子节点的下标

    return max_index;
}

/*
func: update_internal_node_key
param: node old_key-旧节点的maxKey new_key-分裂后旧节点的maxKey
desc: 将old_key更新为new_key
在分裂子节点后,更新其(旧节点)在父节点中对应的key
ret: none
*/
void update_internal_node_key(void* node, uint32_t old_key, uint32_t new_key) {
    uint32_t old_child_index = (internal_node_find_child(node, old_key));
    if (old_child_index!=*(internal_node_num_keys(node)))   //如果不是最右边节点,就更新key(最右边节点没有key)
        *(internal_node_key(node, old_child_index)) = new_key;
}

/*
func: internal_node_insert
param: table parent_page_num child_page_num-新增的节点的page下标
desc: 将分裂后的叶子节点对应那个的cell插入到父节点(internal)中
ret: none
*/
void internal_node_insert(Table* table, uint32_t parent_page_num,
                          uint32_t child_page_num) {
    // Add a new child/key pair to parent that corresponds to child
    void* parent = get_page(table->pager, parent_page_num);
    void* child = get_page(table->pager, child_page_num);
    uint32_t child_max_key = get_node_max_key(child);
    uint32_t index_to_insert = internal_node_find_child(parent, child_max_key);
    //原来的key数量
    uint32_t original_num_keys = *(internal_node_num_keys(parent));

    if (original_num_keys >= INTERNAL_NODE_MAX_CELLS) {
        uint32_t child_index=index_to_insert;
        if (child_max_key>get_node_max_key(get_page(table->pager,*(internal_node_right_child(parent))))){
            //新增的节点来自最右边的节点
            child_index++;
        }
        internal_node_split_and_insert(table,parent_page_num,child_page_num,child_index);
        return;
    }

    *(internal_node_num_keys(parent)) += 1; // add a pair, node key nums +1

    uint32_t right_child_page_num = *(internal_node_right_child(parent));
    void* right_child = get_page(table->pager, right_child_page_num);

    if (child_max_key > get_node_max_key(right_child)) { // 如果是最右边的节点分裂了.
        // Replace right child
        // 原来最右边的孩子指针变成一个cell(childPtr&MaxKey(original_right_child))
        *(internal_node_child(parent, original_num_keys)) =
            right_child_page_num;
        *(internal_node_key(parent, original_num_keys)) =
            get_node_max_key(right_child);
        *(internal_node_right_child(parent)) =
            child_page_num;  //新的最右孩子指针
    } else {
        // Make room for the new cell
        // 需要移动的cell(不包括新cell) [index_to_insert,NumKeys(node)-1] ->
        // (index,NumKeys(node)]
        for (uint32_t i = original_num_keys; i > index_to_insert; i--) {
            void* destination = internal_node_cell(parent, i);
            void* source = internal_node_cell(parent, i - 1);
            memcpy(destination, source, INTERNAL_NODE_CELL_SIZE);
        }
        // insert new cell
        *(internal_node_child(parent, index_to_insert)) = child_page_num;
        *(internal_node_key(parent, index_to_insert)) = child_max_key;
    }
}

/*
func: internal_node_split_and_insert
param: table parent_page_num-需要分裂的父节点 child_page_num-新增的子节点 child_index-在父节点的下标
index_to_insert-新增的节点的maxKey应在父节点中插入的位置的下标,如果==key的数量,表示应该替换右孩子.
desc: 分裂内部节点
ret: none
*/

void internal_node_split_and_insert(Table* table,uint32_t internal_node_page_num,uint32_t child_page_num,uint32_t child_index){
    void* node=get_page(table->pager,internal_node_page_num);
    void* child=get_page(table->pager,child_page_num);
    uint32_t child_max_key=get_node_max_key(child);
    //待分裂的internal节点的原最大key,之后可能用于更新其在父节点中的key
    uint32_t old_max_key=get_internal_node_max_key(table->pager,node);

    uint32_t existing_num_child=*(internal_node_num_keys(node))+1; //已经存在的孩子的个数
    // parent origin rightest child
    uint32_t origin_rightest_child_index=existing_num_child-1;
    uint32_t origin_rightest_child_page_num=*(internal_node_right_child(node));
    void* origin_rightest_child=get_page(table->pager,origin_rightest_child_page_num);

    //right(new) node
    uint32_t new_node_page_num=get_unused_page_num(table->pager);
    void* new_node=get_page(table->pager,new_node_page_num);
    initialize_internal_node(new_node);
    *(internal_node_num_keys(new_node))=INTERNAL_NODE_SPLIT_RIGHT_CHILD_COUNT-1;

    for(int32_t i=existing_num_child;i>=0;i--){
        void* destination_node=NULL;
        uint32_t right_child_index;
        bool is_rightest=false; //是否是 目标节点 的最右边的孩子
        //在目标节点的下标.
        int index=i%INTERNAL_NODE_SPLIT_LEFT_CHILD_COUNT;

        if (i>=INTERNAL_NODE_SPLIT_LEFT_CHILD_COUNT){ // in new node
            destination_node=new_node;
            is_rightest=(index==INTERNAL_NODE_SPLIT_RIGHT_CHILD_COUNT-1);
        }else{ // in old node
            destination_node=node;
            is_rightest=(index==INTERNAL_NODE_SPLIT_LEFT_CHILD_COUNT-1);
        }

        if (i>child_index){ //在待插入元素右边的元素.
            if (is_rightest){ //如果是目标节点的最右边的子节点.
                *(internal_node_right_child(destination_node))=*(internal_node_child(node,i-1));
            }else { // 非目标节点的最右边的子节点,copy child_ptr & key
                if (i-1==origin_rightest_child_index){ //判断该节点是否为原节点的最右边的子节点
                    //赋值子节点指针
                    *(internal_node_child(destination_node,index))=origin_rightest_child_page_num;
                    //赋值key
                    *(uint32_t* )(internal_node_key(destination_node,index))=get_node_max_key(origin_rightest_child);
                }
                else{ //源cell非最右边的孩子
                    memcpy(internal_node_cell(destination_node,index),
                    internal_node_cell(node,i-1),
                    INTERNAL_NODE_CELL_SIZE);
                }
            }
        }else if (i==child_index){ //待插入的元素
            if (is_rightest){
                *(internal_node_right_child(destination_node))=child_page_num;
            }else {
                //page num
                *(internal_node_child(destination_node,index))=child_page_num;
                //key
                *(internal_node_key(destination_node,index))=child_max_key;
            }
        }else{ // 在插入的元素的左边
            if (is_rightest){ //目标位置为最右边
                uint32_t page_num=*(internal_node_child(node,i));
                *(internal_node_right_child(destination_node))=page_num;
            }else{ //目标位置不是最右边
                if (i==origin_rightest_child_index){ //在原节点中为最右边的孩子.
                    *(internal_node_child(destination_node,index))=*(internal_node_child(node,i));
                    *(internal_node_key(destination_node,index))=get_node_max_key(origin_rightest_child);
                } else { //不是原节点的最右边孩子
                    memcpy(internal_node_cell(destination_node,index),
                    internal_node_cell(node,i),
                    INTERNAL_NODE_CELL_SIZE);
                }
            }
        }
    }

    // modify num of keys
    *(internal_node_num_keys(node))=INTERNAL_NODE_SPLIT_LEFT_CHILD_COUNT-1;
    *(internal_node_num_keys(new_node))=INTERNAL_NODE_SPLIT_RIGHT_CHILD_COUNT-1;
    
    //到此,internal node已经分裂为两个internal
    if (is_node_root(node)){ // 分裂的internal为root
        create_new_internal_root(table,new_node_page_num);
    } else { // 分裂的internal非root,更新parent
        //update lower key in parent
        uint32_t new_max_key=get_internal_node_max_key(table->pager,node);
        uint32_t parent_page_num=*(node_parent(node));
        void* parent=get_page(table->pager,parent_page_num);
        update_internal_node_key(parent,old_max_key,new_max_key);
        //insert new cell in parent node
        internal_node_insert(table,parent_page_num,new_node_page_num);
    }

}

/*
func: create_new_internal_root
param: table right_child_page_num-新创建的内部节点(upper)
desc: root为分裂后的root节点(lower)
    创建一个新的root,调整树的结构(给root的子节点赋值)
ret: none
*/

void create_new_internal_root(Table* table,uint32_t right_child_page_num){
    //root node(lower)
    uint32_t root_page_num=table->root_page_num;
    void* root_node=get_page(table->pager,root_page_num);

    //create a new node to be lower node
    uint32_t new_node_page_num=get_unused_page_num(table->pager);
    void* new_node=get_page(table->pager,new_node_page_num);
    initialize_internal_node(new_node);
    *(internal_node_num_keys(new_node))=*(internal_node_num_keys(root_node));
    *(node_parent(new_node))=root_page_num; // new node's parent ptr to root_page_num
    set_node_root(new_node,false);

    //copy cell from root to new node
    for(uint32_t i=0;i<*(internal_node_num_keys(root_node));i++){
        memcpy(internal_node_cell(new_node,i),
        internal_node_cell(root_node,i),
        INTERNAL_NODE_CELL_SIZE);
    }
    //rightest child of new node
    *(internal_node_right_child(new_node))=*(internal_node_right_child(root_node));

    //ROOT NODE
    set_node_root(root_node,true);
    *(internal_node_num_keys(root_node))=1;
    *(internal_node_child(root_node,0))=new_node_page_num;
    //注意,这里获取的max key是整棵子树,而不是一个internal节点的最大key
    *(internal_node_key(root_node,0))=get_internal_node_max_key(table->pager,root_node);
    *(internal_node_right_child(root_node))=right_child_page_num;

    // set Parent
    void* right_child=get_page(table->pager,right_child_page_num);
    *(node_parent(new_node))=table->root_page_num;
    *(node_parent(right_child))=table->root_page_num;

}

uint32_t get_internal_node_max_key(Pager* pager,void* node){
    switch (get_node_type(node)){
        case NODE_INTERNAL:{
            uint32_t right_child=*(internal_node_right_child(node));
            return get_internal_node_max_key(pager,get_page(pager,right_child));
        }
        case NODE_LEAF:
            return get_node_max_key(node);
        default:
            cout<<"UNRECOGNIZED NODE TYPE: "<<get_node_type(node)<<endl;
            exit(EXIT_FAILURE);
    }
    
}

//
// FUNCTION FIELD END