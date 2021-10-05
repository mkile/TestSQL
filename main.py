import sqlite3
import datetime
from json import loads

DB_NAME = 'test.db'
TABLE_DELETE_REQUEST = "DROP TABLE '{}'"
TABLE_CREATE_REQUEST = "CREATE TABLE {} ({})"
DELETE_ALL_REQUEST = "DELETE FROM "
INSERT_REQUEST = "INSERT OR REPLACE INTO {} VALUES ({})"


def check_n_create_data_tables(db_name):
    """Delete table and recreate it"""
    with open('tables_structure.json') as f:
        tables_data = loads(f.read())
    connection = sqlite3.Connection(db_name)
    if isinstance(connection, tuple):
        return 'DB Connection error, cannot continue'
    else:
        for table_name in list(tables_data.keys()):
            connection.execute(TABLE_DELETE_REQUEST.format(table_name))
            create_data = ''
            for parameter in tables_data[table_name]:
                if len(create_data) > 0:
                    create_data += ', '
                create_data += '"' + str(parameter).strip() + '"' + ' ' + tables_data[table_name][parameter]
            try:
                connection.execute(TABLE_CREATE_REQUEST.format(table_name, create_data))
            except Exception as Err:
                print('Error creating table', table_name)
            print('Table {} not found, creating ...'.format(table_name))
        return


def fill_with_test_data(db_name):
    """Fill tables with data"""
    with open('sample_data.json') as f:
        tables_data = loads(f.read())
    connection = sqlite3.Connection(db_name, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
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
# part 1
for dep in range(2):
    result = connection.execute(f'select * from Employee where DepartmentId = {dep + 1} order by Salary desc limit 3 ')
    print(f'Department: {dep + 1}', *result.fetchall(), sep='\n')
# part star
three_months_ago = datetime.datetime.now() - datetime.timedelta(days=90)
three_months_ago = three_months_ago.strftime('%Y-%m-%d')
for dep in range(2):
    result = connection.execute(f'select * from Employee where DepartmentId = {dep + 1} and Salary > 5000 and PaymentDate >= {three_months_ago} order by Salary desc limit 3 ')
    print(f'Department: {dep + 1}', *result.fetchall(), sep='\n')
