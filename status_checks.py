

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


def get_extract_status(crawl_id):

    # TODO [MEDIUM]: Separation of concerns between crawl_id and extraction_id.
    conn = pg_conn()
    cur1 = conn.cursor()
    cur2 = conn.cursor()
    cur3 = conn.cursor()
    processing_query = f"SELECT COUNT(*) FROM groups WHERE status='EXTRACTING' AND crawl_id='{crawl_id}';"
    processed_query = f"SELECT COUNT(*) FROM groups WHERE status='EXTRACTED' AND crawl_id='{crawl_id}';"
    crawled_query = f"SELECT COUNT(*) FROM groups WHERE status='crawled' AND crawl_id='{crawl_id}';"

    cur1.execute(processing_query)
    cur2.execute(processed_query)
    cur3.execute(crawled_query)

    count_val = cur1.fetchall()[0][0]
    status_val = cur2.fetchall()[0][0]
    crawled_val = cur3.fetchall()[0][0]

    return {"crawl_id": crawl_id, "FINISHED": status_val, "PENDING": count_val, "IDLE": crawled_val}


def get_group_status(group_id):
    print("Extract status")


# print(get_extract_status('0a235f37-307b-42c1-aec6-b09ebdb51efc'))
