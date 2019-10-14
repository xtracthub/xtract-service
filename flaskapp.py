
from flask import Flask, request

from crawlers.globus_base import GlobusCrawler
from uuid import uuid4
from status_checks import get_crawl_status
import os
import json

import threading


app = Flask(__name__)


def crawl_launch(crawler, tc):
    crawler.crawl(tc)
    return "done"


@app.route('/crawl', methods=['POST'])
def crawl_repo():

    r = request.json

    endpoint_id = r['eid']
    starting_dir = r['dir_path']
    grouper = r['grouper']

    crawl_id = uuid4()
    crawler = GlobusCrawler(endpoint_id, starting_dir, crawl_id, grouper)
    tc = crawler.get_transfer()
    crawl_thread = threading.Thread(target=crawl_launch, args=(crawler, tc))
    crawl_thread.start()

    return {"crawl_id": str(crawl_id)}


@app.route('/get_crawl_status', methods=['GET'])
def get_cr_status():

    r = request.json

    crawl_id = r["crawl_id"]
    resp = get_crawl_status(crawl_id)
    print(resp)

    return resp


@app.route('/get_extract_status', methods=['GET'])
def get_extract_status():

    # TODO: Return the entire extraction job.
    r = request.json

    extract_id = r["extract_id"]
    resp = get_crawl_status(extract_id)  # TODO.

    return resp


@app.route('/extract', methods=['POST'])
def extract_mdata():

    r = request.json
    crawl_id = r["crawl_id"]

    # TODO: Do stuff here.

    extract_id = uuid4()

    return extract_id


@app.route('/login', methods=['POST'])
def login():

    headers = request.json

    os.makedirs('session_tokens', exist_ok=True)
    session_id = uuid4()

    with open(f"session_tokens/{session_id}", 'w') as f:
        json.dump(headers, f)

    return {"session_id": session_id}


if __name__ == '__main__':
    app.run(debug=True)
