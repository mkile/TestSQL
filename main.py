import sqlite3
import datetime
from json import loads

TABLE_CREATE_REQUEST = "CREATE TABLE {} ({})"
INSERT_REQUEST = "INSERT OR REPLACE INTO {} VALUES ({})"


def check_n_create_data_tables(connection):
    """Delete table and recreate it"""
    with open('tables_structure.json') as f:
        tables_data = loads(f.read())
    if isinstance(connection, tuple):
        return 'DB Connection error, cannot continue'
    else:
        for table_name in list(tables_data.keys()):
            create_data = ''
            for parameter in tables_data[table_name]:
                if len(create_data) > 0:
                    create_data += ', '
                create_data += '"' + str(parameter).strip() + '"' + ' ' + tables_data[table_name][parameter]
            try:
                connection.execute(TABLE_CREATE_REQUEST.format(table_name, create_data))
            except Exception as Err:
                print('Error creating table', table_name)
        return


def fill_with_test_data(connection):
    """Fill tables with data"""
    with open('sample_data.json') as f:
        tables_data = loads(f.read())
    if isinstance(connection, tuple):
        return 'DB Connection error, cannot continue'
    else:
        for table_name in list(tables_data.keys()):
            if len(tables_data[table_name]) > 0:
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


connection = sqlite3.connect("file::memory:?cache=shared", uri=True)
check_n_create_data_tables(connection)
fill_with_test_data(connection)
# part 1
print('Results for part one')
result = connection.execute('select Dep.Name, Empl.Name, Empl.Salary from Employee Empl '
                            'inner join (select ID from '
                            '(select DepartmentId, Salary, ID, row_number() '
                            'over(partition by DepartmentId order by DepartmentId desc, Salary desc) as Num '
                            'from Employee) where '
                            'num <= 3 order by DepartmentId) Sals '
                            'on Empl.ID=Sals.ID '
                            'left join Department as Dep on Empl.DepartmentId = Dep.ID '
                            'Order by Dep.Name, Empl.Salary desc ')
print(*result.fetchall(), sep='\n')
# part star
print('Results for part two')
three_months_ago = datetime.datetime.now() - datetime.timedelta(days=90)
three_months_ago = three_months_ago.strftime('%Y-%m-%d')
result = connection.execute('select Dep.Name, Empl.Name, Empl.Salary, Empl.PaymentDate from Employee Empl '
                            'inner join (select ID from '
                            '(select DepartmentId, Salary, ID, row_number() '
                            'over(partition by DepartmentId order by DepartmentId desc, Salary desc) as Num '
                            'from Employee) where '
                            'num <= 3 order by DepartmentId) Sals '
                            'on Empl.ID=Sals.ID '
                            'left join Department as Dep on Empl.DepartmentId = Dep.ID '
                            f"where Empl.Salary > 5000 AND PaymentDate >= '{three_months_ago}' "
                            'Order by Dep.Name, Empl.Salary desc ')
print(*result.fetchall(), sep='\n')
