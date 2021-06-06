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

    sql = "INSERT INTO aircrafts (icao24, registration, manufacturericao, manufacturername, model, typecode, serialnumber, " \
          "linenumber, icaoaircrafttype, operator, operatorcallsign, operatoricao, operatoriata, owner, testreg, registered, reguntil," \
          "status, built, firstflightdate, seatconfiguration, engines, modes, adsb, acars, notes, categoryDescription) " \
          "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    csv_reader(mycursor, sql, mydb)


def csv_reader(mycursor, sql, mydb):
    # Open planes data file
    with open('planes/aircraftDatabase.csv') as csv_file:
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
            val = [row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11],
                   row[12], row[13], row[14], row[15], row[16], row[17], row[18], row[19], row[20], row[21], row[22],
                   row[23], row[24], row[25], row[26]]

            for i in range(len(val)):
                if val[i] == "":
                    val[i] = None
            val = tuple(val)

            # Apply the insert into the database
            try:
                mycursor.execute(sql, val)
            except mysql.connector.errors.DatabaseError as error:
                None
                # print(val)
                # print(error.msg)
            if cnt % 50000 == 0:
                print(cnt)
        mydb.commit()


if __name__ == '__main__':
    connect_database()
