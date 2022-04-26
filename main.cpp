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
// Row serialize & print
void serialize_row(Row* source,
                   void* destination);  //序列化数据，结构体->序列化紧凑数据
void deserialize_row(void* source,
                     Row* destination);  //逆序列化紧凑数据->结构体
void print_row(Row* row);  //打印一行数据
// directive func
ExecuteResult execute_insert(Statement* statement, Table* table);
ExecuteResult execute_select(Statement* statement, Table* table);
ExecuteResult execute_statement(Statement* statement, Table* table);
// pager
Pager* pager_open(const char* filename);
Table* db_open(const char* filename);
void db_close(Table* table);
void* get_page(Pager* pager, uint32_t page_num);
void pager_flush(Pager* pager, uint32_t page_num, uint32_t size);
// cursor
Cursor* table_start(Table* table);
Cursor* table_end(Table* table);
void* cursor_value(Cursor* cursor);
void cursor_advance(Cursor* cursor);
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

// insert select ...

ExecuteResult execute_insert(Statement* statement, Table* table) {
    if (table->num_rows >= TABLE_MAX_ROWS) {  // table放不下了
        return EXECUTE_TABLE_FULL;
    }
    Row* row_to_insert = &(statement->row_to_insert);

    Cursor* cursor=table_end(table);

    serialize_row(row_to_insert,cursor_value(cursor));
    table->num_rows += 1;
    free(cursor); //cursor是malloc分配的,要记得free

    return EXECUTE_SUCCESS;
}
ExecuteResult execute_select(Statement* statement, Table* table) {
    Row row;
    Cursor* cursor=table_start(table);

    for (uint32_t i = 0; i < table->num_rows; i++) {
        deserialize_row(cursor_value(cursor), &row);
        cursor_advance(cursor);
        print_row(&row);
    }

    free(cursor); //cursor是用malloc分配的,不要忘了free.

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

// database mem manage

Table* db_open(const char* filename) {
    Pager* pager = pager_open(filename);
    // uint32_t num_rows = pager->file_length / ROW_SIZE; this is
    // wrong!!因为page的大小不一定刚好是Row大小的整数倍
    uint32_t num_rows =
        pager->file_length / PAGE_SIZE * ROWS_PER_PAGE;  //整页的所有行

    uint32_t extra_bytes = pager->file_length % PAGE_SIZE;
    if (extra_bytes > 0) {  //残缺页的额外行
        uint32_t addtional_rows = extra_bytes / ROW_SIZE;
        num_rows += addtional_rows;
    }
    Table* table = (Table*)malloc(sizeof(Table));
    table->pager = pager;
    table->num_rows = num_rows;
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

    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) pager->pages[i] = NULL;
    return pager;
}

/*
func: get_page
params: pager-页对象 page_num-页的下标
desc: 获取下标为page_num的页
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

        if (pager->file_length % PAGE_SIZE)  //最后一页没填满
            num_pages++;  //因为是向下取整，所以加上少加的那页
        // 或者直接: uint32_t
        // num_pages=(pager->file_length%PAGE_SIZE==0)?pager->file_length/PAGE_SIZE:pager->file_length/PAGE_SIZE+1

        if (page_num <=
            num_pages - 1) {  // 如果请求的页在数据库文件中，就读取到内存
            lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
            ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
            if (bytes_read == -1) {
                cout << "Error reading file: " << errno << endl;
                exit(EXIT_FAILURE);
            }
        }
        pager->pages[page_num] = page;
    }
    return pager->pages[page_num];
}

/*
func: db_close
param: table{num_rows; //待插入的行的下标，相当于总行数 pager;}
desc: 关闭数据库，将页写入数据库文件，释放空间
*/
void db_close(Table* table) {
    Pager* pager = table->pager;
    uint32_t num_full_pages = table->num_rows / ROWS_PER_PAGE;
    for (uint32_t i = 0; i < num_full_pages; i++) {  //写入被填满的页
        if (pager->pages[i] == NULL)
            continue;  //如果页没有读取到内存，说明该页数据没有改动，跳过此页
        pager_flush(pager, i, PAGE_SIZE);
        free(pager->pages[i]);
        pager->pages[i] = NULL;
    }
    // 可能有额外一页(行数不满ROWS_PER_PAGE的页)需要写入文件末尾
    // 在我们切换到B-tree后就不需要做这个了
    uint32_t num_addtional_rows = table->num_rows % ROWS_PER_PAGE;
    if (num_addtional_rows > 0) {
        uint32_t page_num = num_full_pages;  //多出来的一页的下标
        if (pager->pages[page_num] != NULL) {
            pager_flush(
                pager, page_num,
                num_addtional_rows * ROW_SIZE);  //该页大小为num_rows*row_size
            free(pager->pages[page_num]);
            pager->pages[page_num] = NULL;
        }
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
params: pager   page_num-页的下标   size-页的大小
desc: 向数据库文件中向第page_num对应的偏移处写入size个字节
*/
void pager_flush(Pager* pager, uint32_t page_num, uint32_t size) {
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
        write(pager->file_descriptor, pager->pages[page_num], size);

    if (bytes_written == -1) {
        cout << "Error writing: " << errno << endl;
        exit(EXIT_FAILURE);
    }
}
//
// Cursor func

/*
func: table_start
desc: 创建一个指向表第一行的cursor
ret: 指向表开头的cursor
*/
Cursor* table_start(Table* table) {
    Cursor* cursor = (Cursor*)malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->row_num = 0;
    cursor->end_of_table = false;

    return cursor;
}

/*
func: table_end
desc: 创建一个指向表待插入行(最后一个有效行的后一行)的cursor
ret: 指向表结尾的cursor
*/
Cursor* table_end(Table* table){
    Cursor* cursor=(Cursor*)malloc(sizeof(Cursor));
    cursor->table=table;
    cursor->row_num=table->num_rows;
    cursor->end_of_table=true;

    return cursor;
}

/*
func: cursor_value
desc: 根据cursor指向的行，返回指向该行的内存地址的指针
ret: 指向cursor->row_num对应的行的指针
*/
void* cursor_value(Cursor* cursor){
    uint32_t row_num=cursor->row_num;
    uint32_t page_num=row_num/ROWS_PER_PAGE; //该行对应的页下标
    void* page=get_page(cursor->table->pager,page_num); //获取该页的内存地址

    uint32_t row_offset=row_num%ROWS_PER_PAGE; //行在页内的偏移下标
    uint32_t byte_offset=row_offset*ROW_SIZE; //行偏移对应的字节偏移量

    return (byte* )page+byte_offset;
}

/*
func: cursor_advance
desc: 将cursor移动到下一行,如果到达最后一行就 end_of_table=true
*/
void cursor_advance(Cursor* cursor){
    cursor->row_num+=1;
    if (cursor->row_num>=cursor->table->num_rows)
        cursor->end_of_table=true;
}

// func region END