import sys
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
fname = config['CONNECTION']['file_name']

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

def open_file():
    '''This function will open the file and return a file pointer'''
    try:
        f = open(fname)
        return f
    except OSError:
        print("Could not open/read file:" + fname)
        sys.exit()

def load_data(f) -> dict:
    '''This function will load the data of file into a variable'''
    try:
        items = json.load(f)
        return items
    except ValueError: 
        print('Decoding JSON has failed')
        return None

def create_query(items: dict) -> str:
    '''This function creates a query for insertion of data'''
    try:
        count = len(items)
        coloumns =  ", ".join(items)
        placeholders = ','.join(['?' for x in range(count)])
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
    except pyodbc.IntegrityError as err:
        print("Data not inserted to table due to : ", err)


def main():
    """This is the main function of the script"""
    cursor = conn_sql_server()
    fp = open_file()
    content = load_data(fp)
    if content == None:
        exit()
    insert_qry = create_query(content)
    insert_data(insert_qry, cursor)


if __name__ == "__main__":
    main()