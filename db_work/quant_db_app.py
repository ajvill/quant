from quant_db.quant_db_utils import *

welcome = "Quant db helper!"

menu = """Please select an option.
0. create an index
1. create all quant_obj
2. create a table
3. create all tables
4. drop an index
5. drop all quant_obj
6. drop a table
7. drop all tables
8. tear down everything and build back db architecture
9. insert values into table
10. exit

Your selection: """


def prompt_new(cur, action, quant_list):

    if quant_list == QUANT_INDEXES:
        db_type_name = 'index'
    else:
        db_type_name = 'table'

    if action in 'CREATE':
        action_name = 'create'
    elif action in 'INSERT':
        action_name = 'insert'
    else:
        action_name = 'drop'

    while True:
        print("\nSelect which {} to {}.".format(db_type_name, action_name))
        for i, db_type in enumerate(quant_list):
            # Here select table or index
            print("{}. {}".format(i, db_type))
        db_type_content = input("Your selection: ")
        if int(db_type_content) in range(0, len(quant_list)):
            print("you selected: {}\n".format(quant_list[int(db_type_content)]))
            # sql statement here now that we know action and index/table
            if db_type_name == 'table':
                if action_name == 'create':
                    create_table(cur, quant_list[int(db_type_content)])
                elif action_name == 'insert':
                    print("Something gets inserted here... try pytest for real testing.")
                    #insert_table(cur, quant_list[int(db_type_content)], params)
                else:
                    drop_table(cur, quant_list[int(db_type_content)])
            else:
                # index
                if action_name == 'create':
                    create_index(cur, quant_list[int(db_type_content)])
                else:
                    drop_index(cur, quant_list[int(db_type_content)])
            break
        else:
            print("select again...")


def prompt_index_action(cur, action=None, all_indexes=False):

    if all_indexes is False:
        prompt_new(cur, action, QUANT_INDEXES)
    else:
        for index in QUANT_INDEXES:
            if action == 'CREATE':
                create_index(cur, index)
            else:
                drop_index(cur, index)


def prompt_table_action(cur, action=None, all_tables=False):

    if all_tables is False:
        prompt_new(cur, action, QUANT_TABLES)
    else:
        for table in QUANT_TABLES:
            if action == 'CREATE':
                create_table(cur, table)
            else:
                drop_table(cur, table)


def main():
    conn = get_conn()
    cur = conn.cursor()

    print(welcome)

    while (user_input := input(menu)) != "10":
        if user_input == "0":
            print("Creating index...")
            prompt_index_action(cur, action='CREATE')
        elif user_input == "1":
            print("Creating all quant_obj...")
            prompt_index_action(cur, action='CREATE', all_indexes=True)
        elif user_input == "2":
            print("Creating table...")
            prompt_table_action(cur, action='CREATE')
        elif user_input == "3":
            print("Creating all tables...")
            prompt_table_action(cur, action='CREATE', all_tables=True)
        elif user_input == "4":
            print("Dropping index...")
            prompt_index_action(cur, action='DROP')
        elif user_input == "5":
            print("Drop all quant_obj...")
            prompt_index_action(cur, action='DROP', all_indexes=True)
        elif user_input == "6":
            print("Dropping table...")
            prompt_table_action(cur, action='DROP')
        elif user_input == "7":
            print("Drop all tables...")
            prompt_table_action(cur, action='DROP', all_tables=True)
        elif user_input == "8":
            print("Deleting everything and building back...in case a change was made to db.")
            prompt_table_action(cur, action='DROP', all_tables=True)
            prompt_index_action(cur, action='DROP', all_indexes=True)
            prompt_table_action(cur, action='CREATE', all_tables=True)
            prompt_index_action(cur, action='CREATE', all_indexes=True)
        elif user_input == "9":
            prompt_table_action(cur, action='INSERT', all_tables=False)
        else:
            print("Try again...please make a selection")
        conn.commit()

    print("Good-bye")


if __name__ == '__main__':
    main()
