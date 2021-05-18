import datetime as dt
import pandas as pd
import psycopg2.extras as extras
import logging
import sys


def _get_merge_query(table, cols, key):
    try:
        merge_query = """INSERT INTO %s(%s) OVERRIDING SYSTEM VALUE 
        VALUES %%s 
        ON CONFLICT (%s) DO NOTHING
        """ % (table, cols, key)
    except Exception as e:
        logging.error("Error in function get_merge_query")
        logging.error(e)
    return merge_query


def _merge_data(conn, cursor, query, tuple):
    try:
        extras.execute_values(cursor, query, tuple)
        conn.commit()
        return "Success"
    except Exception as e:
        logging.error(e)
        conn.rollback()
        conn.close()
        return "Failure"


def _create_date_table(start, end):
    df = pd.DataFrame({"date": pd.date_range(start, end)})
    df["date_id"] = df['date'].astype(str).apply(lambda x: x.replace('-', '')).astype(int)
    df["month"] = df.date.dt.month
    df["quarter"] = df.date.dt.quarter
    df["year"] = df.date.dt.year
    return df


def _return_count_of_records(cursor, query):
    try:
        cursor.execute(query)
        result = cursor.fetchone()
        cnt = result[0]
        return cnt
    except Exception as e:
        logging.error(e)
        return -1


def transform_dim_date(hook, table, key, start_date, end_date):
    start = dt.datetime.now()

    # Create dataframe with values for dim dates
    df = _create_date_table(start=start_date, end=end_date)

    # Connection details
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Run merge statement for dim_date - loads only new records
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    query = _get_merge_query(table, cols, key)
    print(query)
    result = _merge_data(conn, cursor, query, tuples)

    # Check result of merge
    if result == "Failure":
        logging.error("FAILURE")
        sys.exit(1)

    end = dt.datetime.now()
    process_time = abs((end - start).seconds)

    # Log processing time
    string = "{} successfully merged in {} seconds".format(table, process_time)
    logging.info(string)


def post_load_listings_check(hook, date):
    # Connection details
    conn = hook.get_conn()
    cursor = conn.cursor()

    staging_query = "SELECT COUNT(*) FROM staging.listings"
    dwh_query = """SELECT COUNT(*) FROM dwh.fact_airbnb_listings f
    JOIN dwh.dim_date dd 
        ON f.run_date_id = dd.date_id
    WHERE dd.date = '{}'
    """.format(date)

    try:
        staging_cnt = _return_count_of_records(cursor, staging_query)
        msg = "Staging listings record count = {}".format(staging_cnt)
        logging.info(msg)
    except Exception as e:
        logging.error(e)
        sys.exit(1)

    try:
        dwh_cnt = _return_count_of_records(cursor, dwh_query)
        msg = "DWH listings record count = {}".format(staging_cnt)
        logging.info(msg)
    except Exception as e:
        logging.error(e)
        sys.exit(1)

    if staging_cnt != dwh_cnt:
        logging.error("Staging and dwh listings counts do not match. Please investigate.")
        sys.exit(1)
