
import time
import json
import logging
from utils.pg_utils import pg_conn
from datetime import datetime
import requests


def get_crawl_status(crawl_id):

    t0 = time.time()
    conn = pg_conn()
    cur2 = conn.cursor()
    # status_query = f"SELECT status, started_on, ended_on FROM crawls WHERE crawl_id='{crawl_id}';"

    # cur2.execute(status_query)

    r = requests.get("http://xtract-crawler-4.eba-ghixpmdf.us-east-1.elasticbeanstalk.com/get_crawl_status", json={'crawl_id': crawl_id})

    vals = json.loads(r.content)

    # print("HERE IS VALS")
    # print(f"{vals}")

    # print(vals)
    #
    # bytes_processed = vals['bytes_crawled']
    # files_processed = vals['files_crawled']
    # groups_crawled = vals['group_crawled']
    # crawl_id = vals['crawl_id']
    #
    # status, started_at, ended_at = cur2.fetchall()[0]
    #
    # if bytes_processed is None:
    #     files_processed = 0
    #     bytes_processed = 0
    #
    # if status == "complete":
    #     elapsed_time = ended_at - started_at
    # else:
    #     elapsed_time = datetime.utcnow() - started_at
    #
    # total_seconds = elapsed_time.total_seconds()
    #
    t1 = time.time()

    # print(f"Total time to get crawl status: {t1-t0}")
    return vals


def get_extract_status(orchestrator):

    t0 = time.time()

    send_status = orchestrator.send_status
    poll_status = orchestrator.poll_status
    # conn = pg_conn()
    # cur1 = conn.cursor()
    # cur2 = conn.cursor()
    # cur3 = conn.cursor()
    # cur4 = conn.cursor()
    # pending_query = f"SELECT COUNT(*) FROM group_metadata_2 WHERE status='EXTRACTING' AND crawl_id='{crawl_id}';"
    # finished_query = f"SELECT COUNT(*) FROM group_metadata_2 WHERE status='EXTRACTED' AND crawl_id='{crawl_id}';"
    # idle_query = f"SELECT COUNT(*) FROM group_metadata_2 WHERE status='crawled' AND crawl_id='{crawl_id}';"
    # failed_query = f"SELECT COUNT(*) FROM group_metadata_2 WHERE status='FAILED' AND crawl_id='{crawl_id}';"
    #
    # cur1.execute(pending_query)
    # cur2.execute(finished_query)
    # cur3.execute(idle_query)
    # cur4.execute(failed_query)
    #
    # pending_val = cur1.fetchall()[0][0]
    # finished_val = cur2.fetchall()[0][0]
    # idle_val = cur3.fetchall()[0][0]
    # failed_val = cur4.fetchall()[0][0]
    t1 = time.time()

    logging.info(f"Extract status query time: {t1-t0}")

    return {"crawl_id": orchestrator.crawl_id, "send_status": send_status, "poll_status": poll_status}
            # "FINISHED": finished_val,
            # "PENDING": pending_val,
            # "IDLE": idle_val,
            # "FAILED": failed_val}
