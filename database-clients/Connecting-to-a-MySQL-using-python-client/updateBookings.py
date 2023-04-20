import mysql.connector as connector

connection = connector.connect( user="root", password="")

cursor = connection.cursor()
cursor.execute("USE testDb")

update_bookings = """ UPDATE Bookings SET TableNo = 10 WHERE BookingID = 6"""

cursor.execute(update_bookings)
connection.commit()

read_data_query = """SELECT * FROM Bookings"""
cursor.execute(read_data_query)

result = cursor.fetchall()
# print(result)


delete_query_booking_id = """DELETE FROM Bookings WHERE BookingID = 4"""
cursor.execute(delete_query_booking_id)
connection.commit()
print("data is successfully deleted" )

result = cursor.fetchall()
print(result)

# To retrieve column names
# columns = cursor.column_names