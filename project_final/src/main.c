#include "bpt.h"

// MAIN
int main( int argc, char ** argv ) {
    int tableID;
    char input_file[120];
    int64_t input;
    char word[120];
    char instruction, trx;
    setvbuf(stdin, NULL, _IONBF, 0);
    setvbuf(stdout, NULL, _IONBF, 0);
    int root = 0;
    int fd[10];
    init_db(100);
    int tableNum = 0;
    int table1, table2;
    int isOk;
    int i = 0;

    //printf("> ");
     while (scanf("%c", &instruction) != EOF) {
         switch (instruction) {
         case 'o':
             scanf("%s", input_file);
             fd[i] = open_table(input_file);
             printf("%d\n", fd[i]);
             i++;
             break;
         case 'u':
             scanf("%d %lld %s", &tableNum, &input, word);
             isOk = update(tableNum, input, word);
             break;
         case 'd':
             scanf("%d %lld", &tableNum, &input);
             root = delete(tableNum, input);
             break;
         case 'i':
             scanf("%d %lld %s", &tableNum, &input, word);
             insert(tableNum, input, word);
             //print_tree(root);
             break;
         case 'f':
             //scanf("%d %d", &tableNum, &input);
             fflush(stdin);
             scanf("%d %lld", &tableNum, &input);
             char *n = find(tableNum, input);
             if(n != NULL){
               printf("Key: %lld, Value: %s\n", input, n);
               free(n);
             }
             else{
               printf("Not Exists\n");
             }
             break;
         case 'j':
             scanf("%d %d %s", &table1, &table2, input_file);
             join_table(table1, table2, input_file);
             break;
         case 'b':
           begin_transaction();
           break;
         case 'c':
           commit_transaction();
           // printf("commit finished\n");
           break;
         case 'a':
           abort_transaction();
           // printf("abort finished\n");
           break;
         /*case 'p':
             scanf("%d", &input);
             find_and_print(root, input, instruction == 'p');
             break;*/
         /*case 'r':
             scanf("%d %d", &input, &range2);
             if (input > range2) {
                 int 8tmp = range2;
                 range2 = input;
                 input = tmp;
             }
             find_and_print_range(root, input, range2, instruction == 'p');
             break;
         case 'l':
             print_leaves(root);
             break;*/
         case 'q':
             while (getchar() != (int)'\n');
             int j;
             for(j = i - 1; j >= 0; j--)
                close_table(fd[j]);
             // close_table(fp);
             shutdown_db();
             return EXIT_SUCCESS;
             break;
       /* case 't':
             print_tree(root);
             break;
         case 'v':
             verbose_output = !verbose_output;
             break;
       case 'x':
             if (root)
                 root = destroy_tree(root);
             print_tree(root);
             break;*/
         default:
            //usage_2();
             break;
         }
         while (getchar() != (int)'\n');
         //printf("> ");
     }
     printf("\n");

    return EXIT_SUCCESS;
}
