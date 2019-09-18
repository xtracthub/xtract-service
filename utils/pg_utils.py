

import psycopg2


def pg_conn():
    # TODO: Offload this to a config file.
    try:
        conn = psycopg2.connect()
    except Exception as e:
        print("Cannot connect to database")
        raise e
    return conn


def pg_update(cur, update_string):

    try:
        cur.execute(update_string)
    except Exception as e:
        print(e)
        raise ConnectionError("Unable to connect to database.")
