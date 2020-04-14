
import time
from utils.pg_utils import pg_conn
from datetime import datetime


def get_crawl_status(crawl_id):

    conn = pg_conn()
    cur1 = conn.cursor()
    cur2 = conn.cursor()
    cur3 = conn.cursor()
    count_query = f"SELECT COUNT(*) FROM groups WHERE crawl_id='{crawl_id}';"
    status_query = f"SELECT status, started_on, ended_on FROM crawls WHERE crawl_id='{crawl_id}';"
    bytes_files_count_query = f"SELECT SUM(total_files), SUM(total_size) from families WHERE crawl_id='{crawl_id}';"

    cur1.execute(count_query)
    cur2.execute(status_query)
    cur3.execute(bytes_files_count_query)

    count_val = cur1.fetchall()[0][0]
    status, started_at, ended_at = cur2.fetchall()[0]
    files_processed, bytes_processed = cur3.fetchall()[0]

    if status == "complete":
        elapsed_time = ended_at - started_at
    else:
        elapsed_time = datetime.now() - started_at

    total_seconds = elapsed_time.total_seconds()

    return {"groups_crawled": count_val,
            "crawl_status": status,
            "elapsed_time": total_seconds,
            "files_processed": files_processed,
            "bytes_processed": int(bytes_processed)
            }


def get_extract_status(crawl_id):

    # TODO [MEDIUM]: Separation of concerns between crawl_id and extraction_id.

    t0 = time.time()
    conn = pg_conn()
    cur1 = conn.cursor()
    cur2 = conn.cursor()
    cur3 = conn.cursor()
    cur4 = conn.cursor()
    pending_query = f"SELECT COUNT(*) FROM groups WHERE status='EXTRACTING' AND crawl_id='{crawl_id}';"
    finished_query = f"SELECT COUNT(*) FROM groups WHERE status='EXTRACTED' AND crawl_id='{crawl_id}';"
    idle_query = f"SELECT COUNT(*) FROM groups WHERE status='crawled' AND crawl_id='{crawl_id}';"
    failed_query = f"SELECT COUNT(*) FROM groups WHERE status='FAILED' AND crawl_id='{crawl_id}';"

    cur1.execute(pending_query)
    cur2.execute(finished_query)
    cur3.execute(idle_query)
    cur4.execute(failed_query)

    pending_val = cur1.fetchall()[0][0]
    finished_val = cur2.fetchall()[0][0]
    idle_val = cur3.fetchall()[0][0]
    failed_val = cur4.fetchall()[0][0]
    t1 = time.time()

    print(f"Data query time: {t1-t0}")

    return {"crawl_id": crawl_id, "FINISHED": finished_val, "PENDING": pending_val, "IDLE": idle_val, "FAILED": failed_val}


def get_group_status(group_id):
    print("Extract status")


# print(get_extract_status('0a235f37-307b-42c1-aec6-b09ebdb51efc'))
