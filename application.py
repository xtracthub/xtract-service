
from flask import Flask, request
from flask_api import status

from status_checks import get_crawl_status, get_extract_status
from container_lib.xtract_matio import MatioExtractor
from crawlers.globus_base import GlobusCrawler
from uuid import uuid4
import json

import threading


application = Flask(__name__)


def crawl_launch(crawler, tc):
    crawler.crawl(tc)


def results_poller_launch(mex):
    mex.poll_responses()


@application.route('/')
def hello():
    st = status.HTTP_200_OK
    return f"Welcome to Xtract! \n Status: {str(st)}", st


@application.route('/crawl', methods=['POST'])
def crawl_repo():

    r = request.json

    endpoint_id = r['eid']
    starting_dir = r['dir_path']
    grouper = r['grouper']
    transfer_token = r['Transfer']
    auth_token = r['Authorization']

    print(f"Received Transfer Token: {transfer_token}")

    crawl_id = uuid4()
    crawler = GlobusCrawler(endpoint_id, starting_dir, crawl_id, transfer_token, auth_token, grouper)
    tc = crawler.get_transfer()
    crawl_thread = threading.Thread(target=crawl_launch, args=(crawler, tc))
    crawl_thread.start()

    return {"crawl_id": str(crawl_id)}, status.HTTP_200_OK


@application.route('/get_crawl_status', methods=['GET'])
def get_cr_status():

    r = request.json

    crawl_id = r["crawl_id"]
    resp = get_crawl_status(crawl_id)
    print(resp)

    return resp


@application.route('/extract', methods=['POST'])
def extract_mdata():

    r = request.json
    crawl_id = r["crawl_id"]
    headers = json.loads(r["headers"])
    print(headers)

    mex = MatioExtractor(eid='068def43-3838-43b7-ae4e-5b13c24424fb', crawl_id=crawl_id, headers=headers)

    print("SENDING FILES...")
    mex.launch_extract()

    print("POLLING RESPONSES...")
    mex.launch_poll()

    # TODO: Shouldn't this extract_id be stored somewhere? 
    extract_id = str(uuid4())

    return extract_id


@application.route('/get_extract_status', methods=['GET'])
def get_extr_status():

    r = request.json

    extract_id = r["crawl_id"]
    resp = get_extract_status(extract_id)

    return resp


@application.route('/login', methods=['POST'])
def login():
    # Login is primarily handled in the notebooks now.
    headers = request.json
    return json.dumps(headers)


# TODO: This is incomplete.
@application.route('/get_mdata', methods=['POST'])
def get_mdata():
    r = request
    return r


if __name__ == '__main__':
    application.run(debug=True, threaded=True)
