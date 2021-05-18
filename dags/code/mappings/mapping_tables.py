import logging
import os
import pandas as pd
import psycopg2
import sys


def _get_file_path(filename, folder):
    file_path = os.path.abspath(os.path.dirname(__file__))
    base_folder = "./../../data/"
    csv_folder = base_folder + folder + "/"
    path = os.path.join(file_path, csv_folder)
    filepath = path + filename
    return filepath


def write_mappings_file(hook, query, col_names, file_name, data_folder):
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Execute query on Postgres database
    try:
        cursor.execute(query)
    except (Exception, psycopg2.DatabaseError) as e:
        logging.error(e)
        sys.exit(1)

    # Retrieve data from query
    tuples = cursor.fetchall()
    cursor.close()

    # Write data to a pandas dataframe
    df = pd.DataFrame(tuples, columns=col_names)

    # Get path to file name
    file = _get_file_path(file_name, data_folder)
    string = "Loading data to file path {}".format(file)
    logging.info(string)

    # Write data to file path
    try:
        df.to_csv(file, header=col_names)
    except Exception as e:
        logging.error(e)
        sys.exit(1)
