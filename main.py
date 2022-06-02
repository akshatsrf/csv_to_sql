import csv
import json
import pyodbc
import logging
import configparser
from time import perf_counter

config = configparser.ConfigParser()
config.read("config.ini")                       # reading the config file

path_to_csv = config['PATH']['path']            # path of the csv file
Driver = config['CONNECTION']['Driver']         # driver to connect to SQL
Server = config['CONNECTION']['Server']         # server to connect to SQL
database = config['CONNECTION']['Database']     # database name in SQL
table = config['CONNECTION']['table_name']      # table name in database

def conn_sql_server() -> pyodbc.Cursor:
    """This function helps to connect the script to the SQL server"""
    try:
        conn = pyodbc.connect(
            f'Driver={Driver}; Server={Server}; Database={database}; Trusted_Connection=yes;')
        cursor = conn.cursor()
        return cursor
    except pyodbc.Error as err:
        logging.warning(err)

def calculate_time(func):
    """This is a decorator function which calculates the execution time of any user defined function"""
    def inner1(*args, **kwargs):
        begin = perf_counter()
        func(*args, **kwargs)
        end = perf_counter()
        print("Total time taken by {} function is %.2f seconds".format(func.__name__)%(end-begin))
    return inner1


def create_query() -> str:
    '''This function creates a query for insertion of data'''
    try:
        f = open('mapping.json')
        items = json.load(f)
        coloumns = ''
        count = len(items)
        for coloumn in items:
            coloumns += f'{coloumn}, '
        coloumns = coloumns[:-2]
        placeholders = '?,' * (count-1) + '?'
        query = f"INSERT INTO {table} ({coloumns}) VALUES ({placeholders}) "
        return query
    except Exception as e:
        print("Not able to create query due to : ",e)


@calculate_time
def insert_data(query: str, cursor: pyodbc.Cursor):
    """This function inserts the data into sql server"""
    try:
        print("Inserting Data....")
        with open(path_to_csv, 'r') as f:
            rows = csv.reader(f)
            for row in rows:
                cursor.execute(query, row)
                cursor.commit()
    except Exception as e:
        print("Data not inserted to table due to : ", e)


def main():
    """This is the main function of the script"""
    cursor = conn_sql_server()
    insert_qry = create_query()
    insert_data(insert_qry, cursor)


if __name__ == "__main__":
    main()