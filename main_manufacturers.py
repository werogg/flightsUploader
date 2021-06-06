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

    sql = "INSERT INTO manufacturers (manufacturericao, name) " \
          "VALUES (%s, %s)"

    csv_reader(mycursor, sql, mydb)


def csv_reader(mycursor, sql, mydb):
    # Open manufacturers data file
    with open('manufacturers/manufacturerDatabase.csv', encoding='utf8') as csv_file:
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
            val = [row[0], row[1]]

            for i in range(len(val)):
                if val[i] == "":
                    val[i] = None
            val = tuple(val)

            # Apply the insert into the database
            try:
                mycursor.execute(sql, val)
            except mysql.connector.errors.DatabaseError as error:
                print(error.msg)
            if cnt % 50000 == 0:
                print(cnt)
        mydb.commit()


if __name__ == '__main__':
    connect_database()
