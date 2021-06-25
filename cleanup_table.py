import psycopg2 # dependency to work with Postgres

connection = psycopg2.connect(user="<user>",
                                password="<db password>",
                                host="<db link or connection string>",
                                port="<db port>",
                                database="<db name>")
cursor = connection.cursor()

table_name = "table_name"
try:
    cursor = connection.cursor()
    cursor.execute(f'''SELECT * FROM {table_name}
                        ORDER BY {table_name}."<col_name>" DESC
                ;''')

    total_rows = cursor.fetchall()
    print(len(total_rows)) # print total number of rows including duplicates
    rows = list(set(total_rows))
    print(len(rows)) # print total number of rows excluding duplicates
    if not total_rows == rows:
        cursor.execute(f"DELETE FROM {table_name};") # delete ALL exisiting data
        print("deletion complete")
        print(len(rows))
        insert_stmt =  f'''
                    INSERT INTO {table_name}("<col1>", "<col2>", "<col3>", "<col4>")
                    VALUES  
                    '''
        """
        Change only the second subscript depending on the number of columns you 
        want to modify.
        """
        for i in range(len(rows)):
            if i == len(rows) - 1:
                insert_stmt+= f''' ('{rows[i][0]}', {rows[i][1]}, '{rows[i][2]}', '{rows[i][3]}'); '''
            else:
                insert_stmt+= f''' ('{rows[i][0]}', {rows[i][1]}, '{rows[i][2]}', '{rows[i][3]}'),\n '''
        
        print(insert_stmt) # check if the statement is a valid SQL query
        cursor.execute(insert_stmt) # insert all unique records

        connection.commit()
        print("insertion complete")
    
    elif total_rows == rows: print("There are no duplicate tuples")
except Exception as e:
    connection.rollback()
    print(e)
