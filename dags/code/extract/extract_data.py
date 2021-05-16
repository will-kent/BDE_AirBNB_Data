import datetime as dt
import os
import pandas as pd
import psycopg2.extras as extras
import logging


def _get_airbnb_filename(period, suffix):
    file_path = os.path.abspath(os.path.dirname(__file__))
    path = os.path.join(file_path, "./../../data/Listings/")
    filename = period + "_" + suffix
    filepath = path + filename
    return filepath


def _get_csv_file_name(filename, folder):
    file_path = os.path.abspath(os.path.dirname(__file__))
    base_folder = "./../../data/"
    csv_folder = base_folder + folder + "/"
    path = os.path.join(file_path, csv_folder)
    filepath = path + filename
    return filepath


def _get_insert_query(table, cols):
    try:
        insert_query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    except Exception as e:
        logging.error("Error in function get_insert_query")
        logging.error(e)
    return insert_query


def _insert_data(conn, cursor, query, tuple):
    try:
        extras.execute_values(cursor, query, tuple)
        conn.commit()
        return "Success"
    except Exception as e:
        logging.error(e)
        conn.rollback()
        conn.close()
        return "Failure"


def get_airbnb_data(hook, execution_date, file_suffix, cols, tablename):
    start = dt.datetime.now()
    file_name = _get_airbnb_filename(execution_date, file_suffix)
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Read Gzip file load data to pandas dataframe
    df = pd.read_csv(file_name, compression="gzip", header=0)

    # Limit dataframe by required columns
    df = df[cols]

    # Run insert statement for airbnb load into staging
    tuples = [tuple(x) for x in df.to_numpy()]
    table = tablename
    cols = ','.join(list(df.columns))
    query = _get_insert_query(table, cols)
    result = _insert_data(conn, cursor, query, tuples)

    # Check result of insert
    if result == "Failure":
        logging.error("FAILURE")

    end = dt.datetime.now()
    process_time = abs((end - start).seconds)

    # Log processing time
    string = "{} successfully loaded in {} seconds".format(file_name, process_time)
    logging.info(string)


def get_csv_data(hook, file, cols, tablename, folder):
    start = dt.datetime.now()

    # Set connection
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Set filename
    file_name = _get_csv_file_name(file, folder)

    # Read CSV file and load data to pandas dataframe
    df = pd.read_csv(file_name, header=0)

    # Limit dataframe by required columns
    df = df[cols]

    # Run insert statement for airbnb load into staging
    tuples = [tuple(x) for x in df.to_numpy()]
    table = tablename
    cols = ','.join(list(df.columns))
    query = _get_insert_query(table, cols)
    result = _insert_data(conn, cursor, query, tuples)

    # Check result of insert
    if result == "Failure":
        logging.error("FAILURE")
        return None

    end = dt.datetime.now()
    process_time = abs((end - start).seconds)

    # Log processing time
    string = "{} successfully loaded in {} seconds".format(file_name, process_time)
    logging.info(string)
