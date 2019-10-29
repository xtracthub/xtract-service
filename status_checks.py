

from utils.pg_utils import pg_conn


def get_crawl_status(crawl_id):

    conn = pg_conn()
    cur1 = conn.cursor()
    cur2 = conn.cursor()
    count_query = f"SELECT COUNT(*) FROM groups WHERE crawl_id='{crawl_id}';"
    status_query = f"SELECT status FROM crawls WHERE crawl_id='{crawl_id}';"

    cur1.execute(count_query)
    cur2.execute(status_query)

    count_val = cur1.fetchall()[0][0]
    status_val = cur2.fetchall()[0][0]

    return {"groups_crawled": count_val, "crawl_status": status_val}


def get_extract_status(extract_id):

    conn = pg_conn()
    cur1 = conn.cursor()
    cur2 = conn.cursor()
    count_query = f"SELECT COUNT(*) FROM group_status WHERE status='extracted' AND extract_id='{extract_id}';"
    status_query = f"SELECT status FROM extractions WHERE extract_id='{extract_id}';"

    cur1.execute(count_query)
    cur2.execute(status_query)

    count_val = cur1.fetchall()[0][0]
    status_val = cur2.fetchall()[0][0]

    return {"groups_extracted": count_val, "crawl_status": status_val}


def get_group_status(group_id):
    print("Extract status")

