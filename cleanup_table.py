import psycopg2

connection = psycopg2.connect(user="<user>",
                                password="<password>",
                                host="<connection string or url>",
                                port="<db port>",
                                database="<db name>")
cursor = connection.cursor()

try:
    cursor = connection.cursor()
    cursor.execute(f'''
                        SELECT * FROM <table name>
                        ORDER BY <column_name>."attribute" DESC
                ;''')

    total_rows = cursor.fetchall()
    print(len(total_rows)) # print total number of records including duplicates
    rows = list(set(total_rows))
    print(len(total_rows)) # print total number of records excluding duplicates. The difference may surpise you.
    print(f"Totale duplicates: {total_rows - rows}")
    cursor.execute("DELETE FROM <table_name>;") # remove all the tuples from the table.
    print("deletion complete")
    insert_stmt =  f'''
                   INSERT INTO <table name>("col_name1", "col_name2", "col_name3", "col_name4")
                   VALUES  
                   '''
    # print(str(rows[0]))
    """
    Modify `insert_stmt` as required by the number of columns. 
    Only the second subscript will change
    """
    for i in range(len(rows)):
        if i == len(rows) - 1:
            insert_stmt+= f''' ('{rows[i][0]}', {rows[i][1]}, '{rows[i][2]}', '{rows[i][3]}'); '''
        else:
            insert_stmt+= f''' ('{rows[i][0]}', {rows[i][1]}, '{rows[i][2]}', '{rows[i][3]}'),\n '''
    
    print(insert_stmt) # check if the final statement is a valid SQL query.
    cursor.execute(insert_stmt) # insert all the unique tuples into the table.

    connection.commit()
    print("insertion complete")

except Exception as e:
    connection.rollback()
    print(e)
