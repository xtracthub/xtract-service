
import time
import json
import logging
import requests


def get_crawl_status(crawl_id):

    r = requests.get("http://xtractcrawler5-env.eba-akbhvznm.us-east-1.elasticbeanstalk.com/get_crawl_status",
                     json={'crawl_id': crawl_id})

    print(r.content)
    vals = json.loads(r.content)

    return vals


def get_extract_status(orchestrator):

    t0 = time.time()

    send_status = orchestrator.send_status
    poll_status = orchestrator.poll_status
    t1 = time.time()

    logging.info(f"Extract status query time: {t1-t0}")

    return {"crawl_id": orchestrator.crawl_id, "send_status": send_status, "poll_status": poll_status}

