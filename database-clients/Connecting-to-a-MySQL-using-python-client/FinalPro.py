import mysql.connector as connector

connection = connector.connect(user="root", password="")

cursor = connection.cursor()

cursor.execute("CREATE DATABASE little_lemon_db")
cursor.execute("USE little_lemon_db")

# Exercise 1: Set up the database

#MenuItems table
create_menuitem_table = """CREATE TABLE MenuItems (
ItemID INT AUTO_INCREMENT,
Name VARCHAR(200),
Type VARCHAR(100),
Price INT,
PRIMARY KEY (ItemID)
);"""   

# Menu table
create_menu_table = """CREATE TABLE Menus (
MenuID INT,
ItemID INT,
Cuisine VARCHAR(100),
PRIMARY KEY (MenuID,ItemID)
);"""

# Bookings table
create_booking_table = """CREATE TABLE Bookings (
BookingID INT AUTO_INCREMENT,
TableNo INT,
GuestFirstName VARCHAR(100) NOT NULL,
GuestLastName VARCHAR(100) NOT NULL,
BookingSlot TIME NOT NULL,
EmployeeID INT,
PRIMARY KEY (BookingID)
);"""

# Orders table
create_orders_table = """CREATE TABLE Orders (
OrderID INT,
TableNo INT,
MenuID INT,
BookingID INT,
BillAmount INT,
Quantity INT,
PRIMARY KEY (OrderID,TableNo)
);"""

# Employees table
create_employees_table = """CREATE TABLE Employees (
EmployeeID INT AUTO_INCREMENT PRIMARY KEY,
Name VARCHAR,
Role VARCHAR,
Address VARCHAR,
Contact_Number INT,
Email VARCHAR,
Annual_Salary VARCHAR
);"""

# Create MenuItems table
cursor.execute(create_menuitem_table)

# Create Menu table
cursor.execute(create_menu_table)

# Create Bookings table
cursor.execute(create_booking_table)

# Create Orders table
cursor.execute(create_orders_table)

# Create Employees table
cursor.execute(create_employees_table)

#*******************************************************#
# Insert query to populate "MenuItems" table:
#*******************************************************#
insert_menuitems="""
INSERT INTO MenuItems (ItemID, Name, Type, Price)
VALUES
(1, 'Olives','Starters',5),
(2, 'Flatbread','Starters', 5),
(3, 'Minestrone', 'Starters', 8),
(4, 'Tomato bread','Starters', 8),
(5, 'Falafel', 'Starters', 7),
(6, 'Hummus', 'Starters', 5),
(7, 'Greek salad', 'Main Courses', 15),
(8, 'Bean soup', 'Main Courses', 12),
(9, 'Pizza', 'Main Courses', 15),
(10, 'Greek yoghurt','Desserts', 7),
(11, 'Ice cream', 'Desserts', 6),
(12, 'Cheesecake', 'Desserts', 4),
(13, 'Athens White wine', 'Drinks', 25),
(14, 'Corfu Red Wine', 'Drinks', 30),
(15, 'Turkish Coffee', 'Drinks', 10),
(16, 'Turkish Coffee', 'Drinks', 10),
(17, 'Kabasa', 'Main Courses', 17);"""

#*******************************************************#
# Insert query to populate "Menu" table:
#*******************************************************#
insert_menu="""
INSERT INTO Menus (MenuID,ItemID,Cuisine)
VALUES
(1, 1, 'Greek'),
(1, 7, 'Greek'),
(1, 10, 'Greek'),
(1, 13, 'Greek'),
(2, 3, 'Italian'),
(2, 9, 'Italian'),
(2, 12, 'Italian'),
(2, 15, 'Italian'),
(3, 5, 'Turkish'),
(3, 17, 'Turkish'),
(3, 11, 'Turkish'),
(3, 16, 'Turkish');"""

#*******************************************************#
# Insert query to populate "Bookings" table:
#*******************************************************#
insert_bookings="""
INSERT INTO Bookings (BookingID, TableNo, GuestFirstName, 
GuestLastName, BookingSlot, EmployeeID)
VALUES
(1, 12, 'Anna','Iversen','19:00:00',1),
(2, 12, 'Joakim', 'Iversen', '19:00:00', 1),
(3, 19, 'Vanessa', 'McCarthy', '15:00:00', 3),
(4, 15, 'Marcos', 'Romero', '17:30:00', 4),
(5, 5, 'Hiroki', 'Yamane', '18:30:00', 2),
(6, 8, 'Diana', 'Pinto', '20:00:00', 5);"""

#*******************************************************#
# Insert query to populate "Orders" table:
#*******************************************************#
insert_orders="""
INSERT INTO Orders (OrderID, TableNo, MenuID, BookingID, Quantity, BillAmount)
VALUES
(1, 12, 1, 1, 2, 86),
(2, 19, 2, 2, 1, 37),
(3, 15, 2, 3, 1, 37),
(4, 5, 3, 4, 1, 40),
(5, 8, 1, 5, 1, 43);"""

#*******************************************************#
# Insert query to populate "Employees" table:
#*******************************************************#
insert_employees = """
INSERT INTO employees (EmployeeID, Name, Role, Address, Contact_Number, Email, Annual_Salary)
(01,'Mario Gollini','Manager','724, Parsley Lane, Old Town, Chicago, IL',351258074,'Mario.g@littlelemon.com','$70,000'),
(02,'Adrian Gollini','Assistant Manager','334, Dill Square, Lincoln Park, Chicago, IL',351474048,'Adrian.g@littlelemon.com','$65,000'),
(03,'Giorgos Dioudis','Head Chef','879 Sage Street, West Loop, Chicago, IL',351970582,'Giorgos.d@littlelemon.com','$50,000'),
(04,'Fatma Kaya','Assistant Chef','132  Bay Lane, Chicago, IL',351963569,'Fatma.k@littlelemon.com','$45,000'),
(05,'Elena Salvai','Head Waiter','989 Thyme Square, EdgeWater, Chicago, IL',351074198,'Elena.s@littlelemon.com','$40,000'),
(06,'John Millar','Receptionist','245 Dill Square, Lincoln Park, Chicago, IL',351584508,'John.m@littlelemon.com','$35,000');"""


# Populate MenuItems table
cursor.execute(insert_menuitems)
connection.commit()

# Populate MenuItems table
cursor.execute(insert_menu)
connection.commit()

# Populate Bookings table
cursor.execute(insert_bookings)
connection.commit()

# Populate Orders table
cursor.execute(insert_orders)
connection.commit()

# Populate Employees table
cursor.execute(insert_employees)
connection.commit()

# Exercise 2: Implement and query stored procedures [Solution]
from mysql.connector.pooling import MySQLConnectionPool
from mysql.connector import Error

dbconfig={
    "database":"little_lemon_db",
    "user":"root", 
    "password":"Natimysql1//"
}

try:
    pool = MySQLConnectionPool(pool_name = "pool_a",pool_size = 2,**dbconfig)
    print("The connection pool is created with a name: ",pool.pool_name)
    print("The pool size is:",pool.pool_size)

except Error as er:
    print("Error code:", er.errno)
    print("Error message:", er.msg)
    
# Get the connection from the connection pool "pool"
print("Getting a connection from the pool.")
connection1 = pool.get_connection()
print("'connection1' object is created with a connection from the pool")


cursor=connection1.cursor()

connection = pool.get_connection()
cursor=connection.cursor()

cursor.execute("DROP PROCEDURE IF EXISTS PeakHours;")
stored_procedure_query="""
CREATE PROCEDURE PeakHours()

BEGIN

SELECT 
HOUR(BookingSlot) AS Booking_Hour,
COUNT(HOUR(BookingSlot)) AS n_Bookings
FROM Bookings
GROUP BY Booking_Hour
ORDER BY n_Bookings DESC;

END

"""

# Execute the query 
cursor.execute(stored_procedure_query)
# Call the stored procedure with its name
cursor.callproc("BusyHours")

# Retrieve records in "dataset"
results = next(cursor.stored_results() )
dataset = results.fetchall()

# Retrieve column names using list comprehension in a for loop 
for column_id in cursor.stored_results():
    columns = [column[0] for column in column_id.description]

# Print column names:
print(columns)

# Print data 
for data in dataset:
    print(data)


stored_procedure_query="""
CREATE PROCEDURE GuestStatus()

BEGIN

SELECT 

Bookings.BookingID AS OrderNumber,  
CONCAT(GuestFirstName,' ',GuestLastName) AS GuestName, 

Role AS Employee, 

CASE 
WHEN Role IN ('Manager','Assistant Manager') THEN "Ready to Pay"
WHEN Role = 'Head Chef' THEN "Ready to serve"
WHEN Role = 'Assistant Chef' THEN "Preparing order"
WHEN Role = 'Head Waiter' THEN "Order served"

ELSE "Pending"
END AS Status

FROM Bookings 
LEFT JOIN 
Employees 
ON Employees.EmployeeID=Bookings.EmployeeID;

END
"""


# Execute the query
cursor.execute(stored_procedure_query)

#********************************************#

# Call the stored procedure with its name
cursor.callproc("GuestStatus")

# Retrieve records in "dataset"
results = next(cursor.stored_results())
dataset = results.fetchall()

# Retrieve column names using list comprehension in a for loop 
for column_id in cursor.stored_results():
    columns = [column[0] for column in column_id.description]

# Print column names
print(columns)

# Print data 
for data in dataset:
    print(data)
    
connection1.close()
# ****************************************************************************************************
# Exercise 3: Little Lemon analysis and sales report

# Import MySQLConnectionPool class
from mysql.connector.pooling import MySQLConnectionPool

# Import Error class
from mysql.connector import Error

# Define database configurations
dbconfig = {
    "database":"little_lemon_db",
    "user" : "root",
    "password" : "Natimysql1//"
}

# Create a pool named "pool_b" with two connections.
# try-except block
try:
    pool = MySQLConnectionPool(pool_name = "pool_b",pool_size = 2,**dbconfig)
    print("The connection pool is created with the name: ",pool.pool_name)
    print("The pool size is:",pool.pool_size)

except Error as er:
    print("Error code:", er.errno)
    print("Error message:", er.msg)


# Connect the first guest.
connection1 = pool.get_connection()
cursor1=connection1.cursor()
booking1="""INSERT INTO Bookings 
(TableNo, GuestFirstName, GuestLastName, BookingSlot, EmployeeID)
VALUES
(8,'Anees','Java','18:00:00',6);"""
cursor1.execute(booking1)
connection1.commit()
print("""A new booking is added in the "Bookings" table.""")

# Connect the second guest .
connection2 = pool.get_connection()
cursor2=connection2.cursor()
booking2="""INSERT INTO Bookings 
(TableNo, GuestFirstName, GuestLastName, BookingSlot, EmployeeID)
VALUES
(5, 'Bald','Vin','19:00:00',6);"""
cursor2.execute(booking2)
connection2.commit()
print("""A new booking is added in the "Bookings" table.""")

# Adding a new connection to connect the third user.
import mysql.connector as connector
try:
    connection3 = pool.get_connection()
    print("The guest is connected")
except:
    print("Adding new connection in the pool.")
        
    # Create a connection
    connection=connector.connect(user="meta",password="password")
    # Add the connection into the pool
    pool.add_connection(cnx=connection)
    print("A new connection is added in the pool.\n")
        
    connection3 = pool.get_connection()
    print("'connection3' is added in the pool.")
    
# Connect the third guest 
cursor3=connection3.cursor()
booking3="""INSERT INTO Bookings 
(TableNo, GuestFirstName, GuestLastName, BookingSlot, EmployeeID)
VALUES
(12, 'Jay','Kon','19:30:00',6);"""
cursor3.execute(booking3)
connection3.commit()
print("""A new booking is added in the "Bookings" table.""")

# You can only return two connections back to the pool as the pool_size=2.
#  Close all  connections and use try-except to print the pool error if the pool is already full.

from mysql.connector import Error
for connection in [connection1,connection2, connection3]:
    try:
        connection.close()
        print("Connection is returned to the pool")
    except Error as er:
        print("\nConnection can't be returned to the pool")
        print("Error message:", er.msg)

# Get a connection from pool_a and create a cursor object to communicate with the database. 
print("Getting a connection from the pool.")
connection = pool.get_connection()
print("""The object "connection" is created with a connection link from the pool_a""")
print("""Creating a cursor object to communicate with the database.""")
cursor=connection.cursor()
print("""The cursor object "cursor" is created.""")

# The name and EmployeeID of the Little Lemon manager.
cursor.execute("""SELECT Name, EmployeeID 
FROM Employees 
WHERE Role = 'Manager' """)
results=cursor.fetchall()
columns=cursor.column_names
print(columns)
for result in results:
    print(result)

# The name and role of the employee who receives the highest salary.
cursor.execute("""SELECT 
Name, EmployeeID 
FROM Employees ORDER BY 
Annual_Salary DESC LIMIT 1""")
results=cursor.fetchall()
columns=cursor.column_names
print(columns)
for result in results:
    print(result)

# The number of guests booked between 18:00:00 and 20:00:00.
cursor.execute("""SELECT 
COUNT(BookingID) n_booking_between_18_20_hrs
FROM Bookings 
WHERE BookingSlot BETWEEN '18:00:00' AND '20:00:00';""")
results=cursor.fetchall()
columns=cursor.column_names
print(columns)
for result in results:
    print(result)

# Full name and the BookingId of each guest  waiting to be seated  
# with the receptionist in sorted order with respect to their BookingSlot.

cursor.execute("""
SELECT 

Bookings.BookingID AS ID,  
CONCAT(GuestFirstName,' ',GuestLastName) AS GuestName, 

Role AS Employee

FROM Bookings 
LEFT JOIN 
Employees 
ON Employees.EmployeeID=Bookings.EmployeeID
WHERE Employees.Role = "Receptionist"
ORDER BY BookingSlot DESC;

""")
print("The following guests are waiting to be seated:")
results=cursor.fetchall()
columns=cursor.column_names
print(columns)
for result in results:
    print(result)


# Create a stored procedure named BasicSalesReport. 
cursor.execute("DROP PROCEDURE IF EXISTS BasicSalesReport;")

stored_procedure_query="""
CREATE PROCEDURE BasicSalesReport()

BEGIN
SELECT 
SUM(BillAmount) AS Total_Sale,
AVG(BillAmount) AS Average_Sale,
MIN(BillAmount) AS Min_Bill_Paid,
MAX(BillAmount) AS Max_Bill_Paid
FROM Orders;
END
"""

# Execute the query
cursor.execute(stored_procedure_query)

#********************************************#

# Call the stored procedure with its name
cursor.callproc("BasicSalesReport")

# Retrieve records in "dataset"
results = next(cursor.stored_results())
results = results.fetchall()

# Retrieve column names using list comprehension in a for loop 
for column_id in cursor.stored_results():
    cols = [column[0] for column in column_id.description]

    
print("Today's sales report:")
for result in results:
    print("\t",cols[0],"","",result[0])
    print("\t",cols[1],"","",result[1])
    print("\t",cols[2],"","",result[2])
    print("\t",cols[3],"","",result[3])


connection = pool.get_connection()
cursor=connection.cursor(buffered=True)

sql_query="""SELECT 
Bookings.BookingSlot,
CONCAT(Bookings.GuestFirstName," ",Bookings.GuestLastName) AS Guest_Name,
Employees.Name AS Emp_Name,
Employees.Role AS Emp_Role
FROM Bookings 
INNER JOIN 
Employees ON Bookings.EmployeeID=Employees.EmployeeID
ORDER BY Bookings.BookingSlot ASC;"""
cursor.execute(sql_query)
results=cursor.fetchmany(size=3)
#print(cursor.column_names)
for result in results:
    print("\nBookingSlot",result[0])
    print("\tGuest_name:",result[1])
    print("\tAssigned to:",result[2],"[{}]".format(result[3]))
    
connection.close()