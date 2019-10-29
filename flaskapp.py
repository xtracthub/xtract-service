
from flask import Flask, request

from container_lib.xtract_matio import MatioExtractor
from crawlers.globus_base import GlobusCrawler
from uuid import uuid4
from status_checks import get_crawl_status, get_extract_status
import os
import json

import threading


app = Flask(__name__)


def crawl_launch(crawler, tc):
    crawler.crawl(tc)
    return "done"


def extract_launch():
    print("Hello")


def results_poller_launch():
    print("HI. ")


@app.route('/crawl', methods=['POST'])
def crawl_repo():

    r = request.json

    endpoint_id = r['eid']
    starting_dir = r['dir_path']
    grouper = r['grouper']

    crawl_id = uuid4()
    crawler = GlobusCrawler(endpoint_id, starting_dir, crawl_id, grouper)
    tc = crawler.get_transfer()  # TODO: Finds my token on the machine. Need to read in the user's bearer token instead.
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


@app.route('/extract', methods=['POST'])
def extract_mdata():

    r = request.json
    crawl_id = r["crawl_id"]

    mex = MatioExtractor('731bad9b-5f8d-421b-88f5-a386e4b1e3e0', crawl_id)

    print("SENDING FILES...")
    mex.send_files()

    print("POLLING RESPONSES...")
    # TODO: This needs to happen in its own thread.
    mex.poll_responses()

    extract_id = str(uuid4())

    return extract_id


@app.route('/get_extract_status', methods=['GET'])
def get_extr_status():

    # TODO: Return the entire extraction job.
    r = request.json

    extract_id = r["extract_id"]
    resp = get_crawl_status(extract_id)  # TODO.

    return resp

@app.route('/login', methods=['POST'])
def login():
    # Login is primarily handled in the notebooks now.
    headers = request.json
    return json.dumps(headers)


if __name__ == '__main__':
    app.run(debug=True)
