import mysql.connector as connector

connection = connector.connect( user="root", password="")

cursor = connection.cursor()
cursor.execute("USE testDb")

my_sql_insert_query = (""" INSERT INTO PythonInsert Values('Markos', 'Romero', 2, '12:00', 1) """)

cursor.execute(my_sql_insert_query)
connection.commit()

read_data_query = """SELECT * FROM PythonInsert"""
cursor.execute(read_data_query)

result = cursor.fetchall()
print(result)

columns = cursor.column_names

# mydb = mysql.connector.connect(
#   host="localhost",
#   user="root",
#   password="Natimysql1//"
# )

# mycursor = mydb.cursor()

# mycursor.execute("SHOW DATABASES")

# for x in mycursor:
#   print(x)

