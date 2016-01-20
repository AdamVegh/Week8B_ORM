__author__ = 'Vegh Adam'
import mysql.connector
from mysql.connector import errors


def col_name(csv_file):
    with open(csv_file, 'r', encoding='utf8') as file:
        return file.readline()[:-1].split(";")

def conn_to_db(command):
    try:
        conn = mysql.connector.connect(host='localhost',
                                       database='northwind',
                                       user='root',
                                       password='Formula-1',
                                       charset='utf8',
                                       use_unicode=True)

        if conn.is_connected():
            print('Connected to MySQL database')

        cursor = conn.cursor()

        try:
            cursor.execute(command)
            conn.commit()
        except errors.ProgrammingError as err:
            conn.rollback()
            print(err)
        print("The database will be closed")
        cursor.close()
        conn.close()

    except errors.ProgrammingError as prerr:
        print("The database is not found!")
        print("OR")
        print("The user or password is not correct!")
        print(prerr)

    except errors.InterfaceError as interr:
        print("The server is not found!")
        print(interr)


def formatter(row):
    return ','.join(['{}']*(len(col_name('employees.csv'))-1)).format(*tuple(row))


class Employees:
    obj_to_csv = ''

    @staticmethod
    def parse(csv_row: str):
        emp = Employees()
        for attr, value in zip(col_name('employees.csv'), csv_row.split(";")):
            setattr(emp, attr, value)
        Employees.obj_to_csv = emp
        return emp

    def persist(self):
        values = [getattr(Employees.obj_to_csv, column) for column in col_name('employees.csv')[1:]]
        command = "insert into employees_clone (" + formatter(col_name('employees.csv')[1:]) + ")" + \
                  "values(" + formatter(values) + ");"
        conn_to_db(command)

    def to_csv(self):
        return ';'.join([getattr(Employees.obj_to_csv, col) for col in col_name('employees.csv')])



if __name__ == "__main__":
    # for i in range(1, len(open('employees.csv', 'r').readlines())):

    a = open('employees.csv', 'r').readlines()[4][:-1]
    # emp2 = Employees.parse(a)
    # print(emp2.FirstName)
    # print(emp2.to_csv())

    emp = Employees()
    emp.parse(a).persist()


    # a = ('Adam', 1991, 'Cecil')
    # b = ",".join(["{}"]*3)
    # print(b.format(*a))
