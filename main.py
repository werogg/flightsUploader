# This is a sample Python script.

# Press Mayús+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import csv
import mysql.connector
import os

def connect_database():
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="minecon2025",
        database="flights_database"
    )

    mycursor = mydb.cursor()

    sql = "INSERT INTO flights (callsign, number, icao24, registration, typecode, origin, destination, " \
          "firstseen, lastseen, day, latitude_1, longitude_1, altitude_1, latitude_2, longitude_2, altitude_2) " \
          "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    csv_reader(mycursor, sql, mydb)


def csv_reader(mycursor, sql, mydb):
    for file in os.listdir('flightdataset'):
        with open('flightdataset/{}'.format(file)) as csv_file:
            print(csv_file)
            reader = csv.reader(csv_file, delimiter=',')
            first = True
            cnt = 0
            for row in reader:
                cnt +=1
                if first:
                    first = False
                    continue

                val = [row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11],
                       row[12], row[13], row[14], row[15]]

                for i in range(len(val)):
                    if val[i] == "":
                        val[i] = None
                val = tuple(val)

                try:
                    mycursor.execute(sql, val)
                except mysql.connector.errors.DatabaseError as error:
                    print(val)
                    print(error.msg)
                if cnt % 50000 == 0:
                    print(cnt)
            mydb.commit()

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    connect_database()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
