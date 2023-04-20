import mysql.connector as connector

# If you run it again it will duplicate values to delete all data ftom the table
# delete t1 from Orders t1 inner join Orders t2 on t2.OrderID = t1.OrderID;

# Establish connection between Python and MySQL database via connector API
connection=connector.connect(
                             user="root", # use your own
                             password="", # use your own
                            )

# Create cursor object to communicate with entire MySQL database
cursor = connection.cursor()

# Setting little_lemon for use 
cursor.execute("use testDb")

# Confirming
print("Database is use is:", connection.database)
print()
print("The existing tables in the testDb database are:")
cursor.execute("SHOW TABLES") 
# Print table names 
for table in cursor: 
    print(table) 
    
    
#*******************************************************#
# Insert query to populate "MenuItems" table is:
#*******************************************************#
insert_menuitmes="""
INSERT INTO MenuItems (ItemID, Name, Type, Price)
VALUES
(1,'Olives','Starters',5),
(2,'Flatbread','Starters', 5),
(3, 'Minestrone', 'Starters', 8),
(4, 'Tomato bread','Starters', 8),
(5, 'Falafel', 'Starters', 7),
(6, 'Hummus', 'Starters', 5),
(7, 'Greek salad', 'Main Courses', 15),
(8, 'Bean soup', 'Main Courses', 12),
(9, 'Pizza', 'Main Courses', 15),
(10,'Greek yoghurt','Desserts', 7),
(11, 'Ice cream', 'Desserts', 6),
(12, 'Cheesecake', 'Desserts', 4),
(13, 'Athens White wine', 'Drinks', 25),
(14, 'Corfu Red Wine', 'Drinks', 30),
(15, 'Turkish Coffee', 'Drinks', 10),
(16, 'Turkish Coffee', 'Drinks', 10),
(17, 'Kabasa', 'Main Courses', 17);"""

#*******************************************************#
# Insert query to populate "Menu" table is:
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
# Insert query to populate "Bookings" table is:
#*******************************************************#
insert_bookings="""
INSERT INTO Bookings (BookingID, TableNo, GuestFirstName, 
GuestLastName, BookingSlot, EmployeeID)
VALUES
(1,12,'Anna','Iversen','19:00:00',1),
(2, 12, 'Joakim', 'Iversen', '19:00:00', 1),
(3, 19, 'Vanessa', 'McCarthy', '15:00:00', 3),
(4, 15, 'Marcos', 'Romero', '17:30:00', 4),
(5, 5, 'Hiroki', 'Yamane', '18:30:00', 2),
(6, 8, 'Diana', 'Pinto', '20:00:00', 5);"""

#*******************************************************#
# Insert query to populate "Orders" table is:
#*******************************************************#
insert_orders="""
INSERT INTO Orders (OrderID, TableNo, MenuID, BookingID, Quantity, BillAmount)
VALUES
(1, 12, 1, 1, 2, 86),
(2, 19, 2, 2, 1, 37),
(3, 15, 2, 3, 1, 37),
(4, 5, 3, 4, 1, 40),
(5, 8, 1, 5, 1, 43);"""


print("Inserting data in MenuItems table.")
# Populate MenuItems table
cursor.execute(insert_menuitmes)
print("Total number of rows in MenuItem table: ", cursor.rowcount)
# Once the query is executed, you commit the change into the database 
connection.commit()

print("Inserting data in Menus table.")
# Populate MenuItems table
cursor.execute(insert_menu)
print("Total number of rows in Menu table: ", cursor.rowcount)
connection.commit()

print("Inserting data in Bookings table.")
# Populate Bookings table
cursor.execute(insert_bookings)
print("Total number of rows in Bookings table: ", cursor.rowcount)
connection.commit()

print("Inserting data in Orders table.")
# Populate Orders table
cursor.execute(insert_orders)
print("Total number of rows in Orders table: ", cursor.rowcount)
connection.commit()


# Query to retrieve only first three records from the bookings table is:
all_Orders = """SELECT * FROM Orders LIMIT 5;"""

# all_Orders = """SELECT * FROM Orders LIMIT 5;"""


# Execute query 
cursor.execute(all_Orders)

# Fetch fist 3 records in results
results = cursor.fetchall()   

# Retrieve column names
cols = cursor.column_names

# Print column names and records from results using for loop
print("""Data in the "Orders" table:""")
print(cols)
for result in results:
    print(result)
    


all_bookings = """SELECT GuestFirstName, GuestLastName, 
TableNo FROM Bookings;"""

# all_bookings = """SELECT * FROM Bookings;"""
# Eexecute query 
cursor.execute(all_bookings)

# Fetch all results that satisfy the query 
results = cursor.fetchall()

# Retrieve column names
cols = cursor.column_names

# Print column names and records from results using for loop
print("""Data in the "Bookings" table:""")
print(cols)
for result in results:
    print(result)
    

# Query to retrieve all menus is: 
all_menus = """SELECT * FROM Menus;"""

# Execute query 
cursor.execute(all_menus)

# Fetch fist 3 records in results
results = cursor.fetchmany(size=3)

# Retrieve column names
cols = cursor.column_names

# Print column names and records from results using for loop
print("""Data in the "Menus" table:""")
print(cols)
for result in results:
    print(result)


# Remaining records after fetching the first three
results= cursor.fetchall()
for result in results:
    print(result)
    


# Query to retrieve only first three records from the bookings table is:
all_menus = """SELECT * FROM Menus LIMIT 3;"""

# Execute query 
cursor.execute(all_menus)

# Fetch fist 3 records in results
results = cursor.fetchall()   

# Retrieve column names
cols = cursor.column_names

# Print column names and records from results using for loop
print("""Data in the "Menus" table:""")
print(cols)
for result in results:
    print(result)
    


# Let's close the cursor and the connection
if connection.is_connected():
    cursor.close()
    print("The cursor is closed.")
    connection.close()
    print("MySQL connection is closed.")
else:
    print("Connection is already closed")


