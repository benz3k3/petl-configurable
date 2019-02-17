import datetime
import os
import pyodbc
import sqlite3

import petl as etl

sqllite = True
createTable = True
dropTable = False
path = "C:\\Users\\barun\\python\\graph"

if sqllite:
    os.remove('petl.db')
    connection = sqlite3.connect('petl.db')
    readconnection = sqlite3.connect('petl.db')
else:
	connection = pyodbc.connect(r'Driver={SQL Server};Server=DESKTOP-I3U7L36;Database=vat;Trusted_Connection=yes;')
	readconnection = pyodbc.connect(r'Driver={SQL Server};Server=DESKTOP-I3U7L36;Database=vat;Trusted_Connection=yes;')
	
def log(msg):
    print("%s at %s" % (msg, datetime.datetime.now()))


# you can add more sources along with their field mapping
source = [['name', 'path', 'table'], ['file1.csv', path, 'table1_stg']]
field = [['name', 'sourceid'], ['Series_reference', 1], ['Period', 1]]

etl.todb(source, connection, tablename = 'source', create=createTable, drop= dropTable)
etl.todb(field, connection, tablename = 'field', create=createTable, drop= dropTable)

log('Reading source')

source = etl.fromdb(readconnection, "select * from source")

print(etl.look(source))

for f in etl.dicts(source):
    fields = etl.fromdb(connection, "select name from field")
    data = etl.fromcsv(os.path.join(f['path'] , f['name']))
    data = etl.cut(data, [c[0] for c in etl.data(fields)])
    log("Filtered fields")
    etl.todb(data, connection, f['table'], sample=5000, create=createTable, drop=dropTable)
    log("Saved")

	
for d in etl.fromdb(connection, 'select * from table1_stg'):
    log(d)


log("Closing")
connection.close()
