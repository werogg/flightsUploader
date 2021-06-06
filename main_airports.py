import csv
import mysql.connector


def connect_database():
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="minecon2025",
        database="covid-flights-database"
    )

    mycursor = mydb.cursor()

    sql = "INSERT INTO airports (icao, name, city, country, iata, latitude, longitude, altitude, timezone, dst, databasetimezone," \
          "type) " \
          "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    csv_reader(mycursor, sql, mydb)


def csv_reader(mycursor, sql, mydb):
    # Open airports data file
    with open('airports/airports.dat', encoding='utf8') as csv_file:
        reader = csv.reader(csv_file, delimiter=',')
        first = True
        cnt = 0

        # Read every line in the csv
        for row in reader:
            cnt += 1
            if first:
                first = False
                continue

            # Save the values in a list
            val = [row[5], row[1], row[2], row[3], row[4], row[6], row[7], row[8], row[9], row[10], row[11], row[12]]
            print(row[4])

            for i in range(len(val)):
                if val[i] == "" or val[i] == '\\N':
                    val[i] = None
            val = tuple(val)

            # Apply the insert into the database
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
