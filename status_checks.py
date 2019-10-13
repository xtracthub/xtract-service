

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

    return {"rows_crawled": count_val, "crawl_status": status_val}

def get_group_status(group_id):
    print("Extract status")


# print(get_crawl_status('3bb826e0-2d12-455e-a7a8-004ad162232e'))