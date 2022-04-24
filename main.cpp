#include <cstdio>
#include <cstring>
#include <iostream>
//
#include "table.h"
//
using namespace std;

InputBuffer* new_input_buffer();
void print_prompt();
void read_input(InputBuffer* input_buffer);
void close_input_buffer(InputBuffer* input_buffer);

MetaCommandResult do_meta_command(InputBuffer* input_buffer);
PrepareResult prepare_statement(InputBuffer* input_buffer,
                                Statement* statement);

//
void serialize_row(Row* source,
                   void* destination);  //序列化数据，结构体->序列化紧凑数据
void deserialize_row(void* source,
                     Row* destination);  //逆序列化紧凑数据->结构体
void* row_slot(Table* table,
               uint32_t row_num);  //返回指向该行的指针，如果页不存在，分配新页
void print_row(Row* row);  //打印一行数据
ExecuteResult execute_insert(Statement* statement, Table* table);
ExecuteResult execute_select(Statement* statement, Table* table);
ExecuteResult execute_statement(Statement* statement, Table* table);
//
Table* new_table();
void free_table(Table* table);
//
int main(int argc, char* argv[]) {
    InputBuffer* input_buffer = new_input_buffer();  //输入缓存
    Table* table = new_table();                      //创建一张表
    int x = 1;
    while (true) {
        print_prompt();
        read_input(input_buffer);
        // 每一个case都要continue,这样不会执行后面的语句，直接进入下一次while
        if (input_buffer->buffer[0] == '.') {
            switch (do_meta_command(input_buffer)) {
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

MetaCommandResult do_meta_command(InputBuffer* input_buffer) {
    if (strcmp(input_buffer->buffer, ".exit") == 0) {
        exit(EXIT_SUCCESS);
    } else {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

PrepareResult prepare_statement(InputBuffer* input_buffer,
                                Statement* statement) {
    if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
        statement->type = STATEMENT_INSERT;
        int args_assigned = sscanf(input_buffer->buffer, "insert %d %s %s",
                                   &(statement->row_to_insert.id),
                                   statement->row_to_insert.username,
                                   statement->row_to_insert.email);
        if (args_assigned < 3) {
            return PREPARE_SYNTAX_ERROR;
        }
        return PREPARE_SUCCESS;
    }
    if (strcmp(input_buffer->buffer, "select") == 0) {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }

    return PREPARE_UNRECOGNIZED_STATEMENT;
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

InputBuffer* new_input_buffer() {
    InputBuffer* input_buffer = new InputBuffer;
    input_buffer->buffer = NULL;
    input_buffer->buffer_length = 0;
    input_buffer->input_length = 0;
    return input_buffer;
}
/*
func: serialize_row
desc: 序列化行，将行存储到dest指针对应的空间
ret: 无
*/
void serialize_row(Row* source, void* destination) {
    memcpy((byte*)destination + ID_OFFSET, &(source->id), ID_SIZE);
    memcpy((byte*)destination + USERNAME_OFFSET, &(source->username),
           USERNAME_SIZE);
    memcpy((byte*)destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
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

/*
func: row_slot
desc: 定位第row_num行的内存空间，如果page不存在直接分配
ret: 指针，指向行对应的内存空间
*/
void* row_slot(Table* table, uint32_t row_num) {
    uint32_t page_num = row_num / ROWS_PER_PAGE;
    void* page = table->pages[page_num];
    if (page == NULL) {
        //在内存新分配一页
        page = table->pages[page_num] = malloc(PAGE_SIZE);
    }
    uint32_t row_offset = row_num % ROWS_PER_PAGE;  //行在该页的下标
    uint32_t byte_offset = row_offset * ROW_SIZE;   //行在该页的偏移量
    return (void* )((byte*)page + byte_offset);      //返回指向行的指针
}

//

ExecuteResult execute_insert(Statement* statement, Table* table) {
    if (table->num_rows >= TABLE_MAX_ROWS) {  // table放不下了
        return EXECUTE_TABLE_FULL;
    }
    Row* row_to_insert = &(statement->row_to_insert);
    serialize_row(row_to_insert, row_slot(table, table->num_rows));
    table->num_rows += 1;
    return EXECUTE_SUCCESS;
}
ExecuteResult execute_select(Statement* statement, Table* table) {
    Row row;
    for (uint32_t i = 0; i < table->num_rows; i++) {
        deserialize_row(row_slot(table, i), &row);
        print_row(&row);
    }
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

//
Table* new_table() {
    Table* table = (Table*)malloc(sizeof(Table));
    table->num_rows = 0;
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) table->pages[i] = NULL;
    return table;
}

void free_table(Table* table) {
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) free(table->pages[i]);
    free(table);
}

void print_row(Row* row) {
    cout << "(" << row->id << ", " << row->username << ", " << row->email << ")"
         << endl;
}