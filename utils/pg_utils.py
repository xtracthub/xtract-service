

import psycopg2
import os


# TODO: Turn this into a class that allows shared conn() object.
def pg_conn():
    try:
        conn = psycopg2.connect(host=os.environ["XTRACT_DB"], database="xtractdb",
                                user="xtract", port=5432, password=os.environ["XTRACT_PASS"])
    except Exception as e:
        print("Cannot connect to database")
        raise e
    return conn


def pg_list(py_list):
    for item in py_list:
        if "'" in item:
            raise ValueError("Filename contains illegal Postgres character --> ' <--")

    new_list = ((str(py_list).replace('[', '{')).replace(']', '}')).replace('\'', '')
    return new_list

def pg_update(cur, update_string):

    try:
        cur.execute(update_string)
    except Exception as e:
        print(e)
        raise ConnectionError("Unable to connect to database.")


