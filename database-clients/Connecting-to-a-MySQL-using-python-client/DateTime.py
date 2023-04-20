import mysql.connector as connector

connection = connector.connect(user="root", password="")

cursor = connection.cursor()

cursor.execute("USE testDb")


import datetime as dt

select_stmt = """SELECT * FROM Bookings"""
cursor.execute(select_stmt)
print(cursor.column_names)


for row in cursor:
    booking_id=row[0]
    booking_slot=row[4]
    new_booking_slot=booking_slot+ str(dt.timedelta(hours=1))
print("booking id {} is moved from {} to {}".format(booking_id,booking_slot, new_booking_slot))


# current_time = dt.datetime.now()

# print(current_time.date())
# print(current_time.time())

# what date is this same day next week?

# week = dt.timedelta(days = 7)
# print(current_time.date() + week)
