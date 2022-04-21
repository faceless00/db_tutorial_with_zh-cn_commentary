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
void execute_statement(Statement* statement);

MetaCommandResult do_meta_command(InputBuffer* input_buffer);
PrepareResult prepare_statement(InputBuffer* input_buffer,
                                Statement* statement);


//
void serialize_row(Row* source,void* destination);  //序列化数据，结构体->序列化紧凑数据
void deserialize_row(void* source,Row* destination);    //逆序列化紧凑数据->结构体
void* row_slot(Table* table,uint32_t row_num);  //返回指向该行的指针，如果页不存在，分配新页
//
//
int main(int argc, char* argv[]) {
    InputBuffer* input_buffer = new_input_buffer();
    while (true) {
        print_prompt();
        read_input(input_buffer);

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
            case (PREPARE_UNRECOGNIZED_STATEMENT):
                cout << "Unrecognized keyword at start of: "
                     << input_buffer->buffer << endl;
                continue;
            default:
                continue;
        }
        execute_statement(&statement);
        cout << "Executed." << endl;
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

void execute_statement(Statement* statement) {
    switch (statement->type) {
        case (STATEMENT_INSERT):
            printf("This is where we would do an insert.\n");
            break;
        case (STATEMENT_SELECT):
            printf("This is where we would do a select.\n");
            break;
    }
}

//
void serialize_row(Row* source,void* destination){
    memcpy(destination+ID_OFFSET,&(source->id),ID_SIZE);
    memcpy(destination+USERNAME_OFFSET,&(source->username),USERNAME_SIZE);
    memcpy(destination+EMAIL_OFFSET,&(source->email),EMAIL_SIZE);
}
void deserialize_row(void* source,Row* destination){
    memcpy(&(destination->id),source+ID_OFFSET,ID_SIZE);
    memcpy(&(destination->username),source+USERNAME_OFFSET,USERNAME_SIZE);
    memcpy(&(destination->email),source+EMAIL_OFFSET,EMAIL_SIZE);
}
void* row_slot(Table* table,uint32_t row_num){
    uint32_t page_num=row_num/ROWS_PER_PAGE;
    void* page=table->pages[page_num];
    if (page==NULL){
        //在内存新分配一页
        page=table->pages[page_num]=(void* )malloc(sizeof(page));
    }
    uint32_t row_offset=row_num%ROWS_PER_PAGE;  //行在该页的下标
    uint32_t byte_offset=row_offset*ROW_SIZE;   //行在该页的偏移量
    return page+byte_offset;    //返回指向行的指针
}
//
