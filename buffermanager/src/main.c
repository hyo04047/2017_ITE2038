#include "bpt.h"

// MAIN
int main( int argc, char ** argv ) {
    int i = 0;
    char * input_file;
    int64_t root;
    int input, range2;
    char word[120];
    int arr[10];
    char instruction;
    //char license_part;
    setvbuf(stdin, NULL, _IONBF, 0);
    setvbuf(stdout, NULL, _IONBF, 0);
    //input_file = calloc(sizeof(char),130);
    root = 0;
    int fd = open_table("test.db");
    init_db(16);

    //printf("> ");
    while (scanf("%c", &instruction) != EOF) {
        switch (instruction) {
        case 'o':
            scanf("%s", input_file);
            arr[i++] = open_table(input_file);
            free(input_file);
            break;
        case 'a':
            scanf("%d", &input);
            init_db(input);
            break;
        case 'd':
            scanf("%d", &input);
            root = delete(fd, input);
            //print_tree(root);
            break;
        case 'i':
            scanf("%d %s", &input, word);
            insert(fd, input, word);
            //print_tree(root);
            break;
        case 'f':
            scanf("%d", &input);
            char *n = find(fd, input);
            if(n != NULL){
              printf("Key: %d, Value: %s\n", input, n);
              free(n);
            }
            else{
              printf("Not Exists\n");
            }
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
        case 'c':
            scanf("%d", &input);
            //close_table(
        case 'q':
            while (getchar() != (int)'\n');

            int a = close_table(fd);
            shutdown_db();
            if(a == 0){
              return EXIT_SUCCESS;
            }
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
