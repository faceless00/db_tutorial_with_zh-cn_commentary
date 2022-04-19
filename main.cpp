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
void execute_statement(Statement* statement);

MetaCommandResult do_meta_command(InputBuffer* input_buffer);
PrepareResult prepare_statement(InputBuffer* input_buffer,Statement* statement);
int main(int argc, char* argv[]) {
    InputBuffer* input_buffer = new_input_buffer();
    while (true) {
        print_prompt();
        read_input(input_buffer);

        if (input_buffer->buffer[0]=='.'){
            switch (do_meta_command(input_buffer)){
                case (META_COMMAND_SUCCESS):
                    continue;
                case (META_COMMAND_UNRECOGNIZED_COMMAND):
                    cout<<"Unrecognized command: "<<input_buffer->buffer<<endl;
                    continue;
                
            }
        }
        Statement statement;
        switch (prepare_statement(input_buffer,&statement)){
            case (PREPARE_SUCCESS):
                break;
            case (PREPARE_UNRECOGNIZED_STATEMENT):
                cout<<"Unrecognized keyword at start of: "<<input_buffer->buffer<<endl;
                continue;
        }
        execute_statement(&statement);
        cout<<"Executed."<<endl;
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
