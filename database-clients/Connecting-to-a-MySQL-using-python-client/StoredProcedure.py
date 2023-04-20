import mysql.connector as connector

connection = connector.connect(user="root", password="")

cursor = connection.cursor()

use_database_query = """USE testDb"""
cursor.execute(use_database_query)


stored_procedure_query = """
CREATE PROCEDURE GetCustomerAndBillAmount()
BEGIN 
SELECT Bookings.BookingID, CONCAT(Bookings.GuestFirstName, '', Bookings.GuestLastName) AS CustomerName,
Orders.BillAmount from Bookings INNER JOIN 
Orders ON Bookings.BookingID=Orders.BookingID
WHERE BillAmount >= 50 ORDER BY BillAmount DESC;
END
"""

cursor.execute(stored_procedure_query)
cursor.callproc("GetCustomerAndBillAmount")

results = next(cursor.stored_results())

dataset = results.fetchall()

for data in dataset:
    print(data)

# delete_procedure = """drop procedure GetCustomerAndBillAmount"""

# cursor.execute(delete_procedure)