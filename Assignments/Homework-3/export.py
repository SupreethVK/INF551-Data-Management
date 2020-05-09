"""
Write a Python script “export.py” that connects to the “world” database on your MySQL server 
(using user name “inf551” and password “inf551”) and exports the tables in CSV files, one file per table. 
Name the file after the table: city.csv, country.csv, and countrylanguage.csv. The files should have 
the same formats as the ones provided to you in the first homework. In other words, it should have a 
header (1st line) and followed by rows in the table, with columns quoted if values are strings and 
separated by comma. For example, here shows the content of city.csv.
# ID, Name, CountryCode, District, Population 
'1', 'Kabul', 'AFG', 'Kabol', '1780000'
'2', 'Qandahar', 'AFG', 'Qandahar', '237500' 
'3', 'Herat', 'AFG', 'Herat', '186800'
...
Note that your “export.py” should work in general, that is, it can export contents of databases 
other than “world”. Other databases will be used to test your “export.py”.
You are required to use the “information_schema” which stores the metadata about the databases 
(to see details, execute “use information_schema” and “show tables”). In particular, use “tables” 
and “columns” in this database to find out which tables the world database has and a list of columns 
for each table in that database.
"""

import mysql.connector as mysql
import json
import sys
import csv


credentials = {'username':'inf551', 'password':'inf551'}
if len(sys.argv)>1:
    db = sys.argv[1]
else:
    db = 'world'
tables = []
tables_FK = []

def connect():
    connection = mysql.connect(
        user=credentials.get('username'),
        password=credentials.get('password'),
        database=db,
        host='127.0.0.1'
    )
    return connection
    
def execute(connection, sql):
    cursor = connection.cursor()
    # Execute the query
    cursor.execute(sql)

    # Loop through the results
    for row in cursor:
        print('name:', row)
        
def find_tables(connection):
    cursor = connection.cursor()
    sql = "select * from information_schema.tables t where t.table_schema = '%s' and t.table_type='BASE TABLE'"
    cursor.execute(sql % db)
    
    for row in cursor:
        tables.append(row[2])
    
    print (tables)

def find_FK(connection):
    cursor = connection.cursor()
    sql = "select TABLE_NAME,COLUMN_NAME,REFERENCED_TABLE_NAME,REFERENCED_COLUMN_NAME from information_Schema.key_column_usage where REFERENCED_TABLE_SCHEMA='%s'"
    cursor.execute(sql % db)

    for row in cursor:
        tables_FK.append([row[0], row[1], row[2], row[3]])
    print(tables_FK)

    
def export_table(connection, table_name):
    print("-------------------------------------")
    print("Table: ", table_name)
    
    cursor = connection.cursor()
    columns = []
    sql = "select * from information_schema.columns c where c.table_name='%s'"
    cursor.execute(sql % table_name)
    
    for row in cursor:
        columns.append(row[3])
    
    print (columns)
    
    sql = "select * from %s"
    cursor.execute(sql % table_name)
    data = cursor.fetchall()
    
    for row in data:
        print("row: ", row)
        
    
    columns[0] = '# ' + columns[0]
    with open(table_name +'.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(columns)
        writer = csv.writer(file, quoting=csv.QUOTE_ALL, quotechar="'")
        for row in data:
            writer.writerow(row)
    
    


connection = connect()

sql1 = "SELECT * FROM country LIMIT 10"
sql3 = "show tables"
sql4 = "select * from information_schema.tables where information_schema.tables.table_schema='world' and information_schema.tables.table_type='BASE TABLE'"
sql5 = "select * from information_schema.columns c where information_schema.columns.table_name='city'"


#execute(connection, sql5)

find_tables(connection)
find_FK(connection)
for table in tables:
    export_table(connection, table)
        
# Close the connection
connection.close()