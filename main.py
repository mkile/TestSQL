import sqlite3
from json import loads

DB_NAME = 'test.db'
TABLE_CHECK_REQUEST = "SELECT COUNT(name) FROM sqlite_master WHERE type = 'table' AND name = '{}'"
TABLE_CREATE_REQUEST = "CREATE TABLE {} ({})"
DELETE_ALL_REQUEST = "DELETE FROM "
INSERT_REQUEST = "INSERT OR REPLACE INTO {} VALUES ({})"


def check_n_create_data_tables(db_name):
    """Check if data table has necessary table, if not create them"""
    with open('tables_structure.json') as f:
        tables_data = loads(f.read())
    connection = sqlite3.Connection(db_name)
    if isinstance(connection, tuple):
        return 'DB Connection error, cannot continue'
    else:
        for table_name in list(tables_data.keys()):
            test_result = connection.execute(TABLE_CHECK_REQUEST.format(table_name)).fetchall()
            if int(test_result[0][0]) < 1:
                create_data = ''
                for parameter in tables_data[table_name]:
                    if len(create_data) > 0:
                        create_data += ', '
                    create_data += '"' + parameter + '"' + ' ' + tables_data[table_name][parameter]
                try:
                    connection.execute(TABLE_CREATE_REQUEST.format(table_name, create_data))
                except Exception as Err:
                    print('Error creating table', table_name)
                print('Table {} not found, creating ...'.format(table_name))
        return


def fill_with_test_data(db_name):
    with open('sample_data.json') as f:
        tables_data = loads(f.read())
    connection = sqlite3.Connection(db_name)
    if isinstance(connection, tuple):
        return 'DB Connection error, cannot continue'
    else:
        for table_name in list(tables_data.keys()):
            if len(tables_data[table_name]) > 0:
                connection.execute(DELETE_ALL_REQUEST + table_name)
                id = 0
                for item in tables_data[table_name]:
                    id += 1
                    write_row = str(id) + ", '" + item + "'"
                    if isinstance(tables_data[table_name][item], dict):
                        for element in tables_data[table_name][item]:
                            write_row += ", "
                            write_row += str(tables_data[table_name][item][element])
                    try:
                        connection.execute(INSERT_REQUEST.format(table_name, write_row))
                    except Exception as Err:
                        print('Error inserting data to table', table_name)
        connection.commit()
        return

check_n_create_data_tables(DB_NAME)
fill_with_test_data(DB_NAME)
connection = sqlite3.Connection(DB_NAME)
result = connection.execute('select * from Employee where DepartmentId = 1 order by Salary desc limit 3 ')
print(*result.fetchall(), sep='\n')
