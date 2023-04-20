import mysql.connector as connector

connection = connector.connect(user="root", password="")

cursor = connection.cursor()

use_database_query = """USE testDb"""
cursor.execute(use_database_query)

from mysql.connector.pooling import MySQLConnectionPool

pool = MySQLConnectionPool(pool_name="testDb_pool", pool_size=4,
                           host="localhost", database="testDb",
                           user="root", password="")

users = ["bebe", "kassu", "astu", "dell"] 
select_stmt = "SELECT * FROM Bookings WHERE BookingID=%(booking_id)s"

for i in range(pool.pool_size):
    conn=pool.get_connection()
    if conn.is_connected:
        cursor=conn.cursor()
        print("The connection id for {} {} is requesting info on booking {}"
              .format(users[i], conn.connection_id, i+1))
        cursor.execute(select_stmt, {'booking_id': i+1})
        print(cursor.fetchall())
    else:
        print("No live connection made!")
    conn.close()