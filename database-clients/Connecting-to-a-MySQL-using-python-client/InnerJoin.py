import mysql.connector as connector

connection = connector.connect( user="root", password="")

cursor = connection.cursor()
cursor.execute("USE testDb")

join_query = """SELECT MenuItems.Name, MenuItems.Type, MenuItems.Price, Menus.Cuisine FROM MenuItems
INNER JOIN Menus ON MenuItems.ItemID=Menus.ItemID"""

cursor.execute(join_query)
results = cursor.fetchall()

for result in results:
    print(result)