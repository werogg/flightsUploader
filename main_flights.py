import csv
import mysql.connector
import os


def connect_database():
    # Establish connection with the database
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="minecon2025",
        database="covid-flights-database"
    )

    # Set the cursor in the database
    mycursor = mydb.cursor()

    # Define the sql statement for data insert into flights table
    sql = "INSERT INTO flights (callsign, number, icao24, registration, typecode, origin, destination, " \
          "firstseen, lastseen, day, latitude_1, longitude_1, altitude_1, latitude_2, longitude_2, altitude_2) " \
          "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    # Call method to read a csv and commit the data into the database
    csv_reader(mycursor, sql, mydb)


"""
Read a flights csv and commit the content.

The csv should have the format: callsign, number, icao24, registration, typecode, origin, destination, 
firstseen, lastseen, day, latitude_1, longitude_1, altitude_1, latitude_2, longitude_2, altitude_2.

:param mycursor: The cursor inside the database
:param sql: The insert statement
:param mydb: The database connection objec
"""


def csv_reader(mycursor, sql, mydb):
    # Iterate trough files in flightdataset folder
    for file in os.listdir('flightdataset'):
        # Open every file
        with open('flightdataset/{}'.format(file)) as csv_file:
            # Print the file being processed and start processing it
            print(csv_file)
            reader = csv.reader(csv_file, delimiter=',')
            first = True
            cnt = 0
            errorcnt = 0

            for row in reader:
                if first:
                    first = False
                    continue

                allowed_airports = ['LEBL', 'LEPA', 'LEMG', 'LEMD']

                # We avoid processing data about airports that are not in the study
                if row[5] not in allowed_airports and row[6] not in allowed_airports:
                    continue

                val = [row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11],
                       row[12], row[13], row[14], row[15]]

                # Substitute empty data for None to avoid NULL errors in the database
                for i in range(len(val)):
                    if val[i] == "":
                        val[i] = None
                val = tuple(val)

                haserror = False

                # We try to make the changes inside the database and check for errors
                try:
                    cnt += 1
                    mycursor.execute(sql, val)
                except mysql.connector.errors.DatabaseError as error:
                    print(val)
                    print(error.msg)
                    haserror = True
                    errorcnt += 1

                # If error occurred, it's probably for missing foreign key matching so we will create empty aircraft
                # empty airport or both and try to insert the flight again.
                if haserror:
                    try:
                        sql2 = "INSERT INTO aircrafts (icao24) " \
                               "VALUES (%s)"
                        sql3 = "INSERT INTO airport (icao) " \
                               "VALUES (%s)"
                        mycursor.execute(sql2, row[2])
                        mycursor.execute(sql3, row[5])
                        mycursor.execute(sql3, row[6])
                    except:
                        print("No hay manera")

                    try:
                        mycursor.execute(sql, val)
                    except:
                        print("No hay manera")

                # Used before to check how many data was being discarded (~10%), now all data is used
                print("Added: {} with Error {}".format(cnt, errorcnt))

                if cnt % 50000 == 0:
                    print(cnt)
            mydb.commit()


# Main
if __name__ == '__main__':
    connect_database()
