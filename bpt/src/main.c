#include "bpt.h"

// MAIN

int main( int argc, char ** argv ) {

    char input_file[1024];
    int64_t input;
    char value[120];
    char instruction;

    // printf("> ");
    while (scanf("%c", &instruction) != EOF) {
        switch (instruction) {
        case 'd':
            scanf("%ld", &input);
            delete(input);
            // if(delete(input) == 0)
                // printf("delete success\n");
            break;
        case 'i':
            scanf("%ld", &input);
            scanf("%s", value);
            insert(input, value);
            // if(insert(input, value) == 0)
                // printf("insert success\n");
            break;
        case 'q':
            while (getchar() != (int)'\n');
            return EXIT_SUCCESS;
            break;
        case 'f':
            scanf("%ld", &input);
            if(find(input) == NULL){
                printf("NULL\n");
                fflush(stdout);
                break;
            }
            else
              printf("%s\n", find(input));
            fflush(stdout);
            break;
        case 'o':
            scanf("%s", input_file);
            open_db(input_file);
            break;
        default:
            break;
        }
        while (getchar() != (int)'\n');
        // printf("> ");
    }
    // printf("\n");
    free(header_pg);	

    return EXIT_SUCCESS;
}
