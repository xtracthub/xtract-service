
from flask import Flask, request

# from status_checks import get_crawl_status, get_extract_status
# from container_lib.xtract_matio import MatioExtractor
# from crawlers.globus_base import GlobusCrawler
from uuid import uuid4
import os
import json

import threading


application = Flask(__name__)


# def crawl_launch(crawler, tc):
#     crawler.crawl(tc)
#     return "done"
#
#
# def extract_launch(mex):
#     mex.send_files()
#
#
# def results_poller_launch():
#     print("HI. ")


@application.route('/')
def hello():
    return "Hello, Xtract. HELLO RYAN."

# @application.route('/crawl', methods=['POST'])
# def crawl_repo():
#
#     r = request.json
#
#     endpoint_id = r['eid']
#     starting_dir = r['dir_path']
#     grouper = r['grouper']
#
#     crawl_id = uuid4()
#     crawler = GlobusCrawler(endpoint_id, starting_dir, crawl_id, grouper)
#     tc = crawler.get_transfer()  # TODO: Finds my token on the machine. Need to read in the user's bearer token instead.
#     crawl_thread = threading.Thread(target=crawl_launch, args=(crawler, tc))
#     crawl_thread.start()
#
#     return {"crawl_id": str(crawl_id)}
#
#
# @application.route('/get_crawl_status', methods=['GET'])
# def get_cr_status():
#
#     r = request.json
#
#     crawl_id = r["crawl_id"]
#     resp = get_crawl_status(crawl_id)
#     print(resp)
#
#     return resp
#
#
# @application.route('/extract', methods=['POST'])
# def extract_mdata():
#
#     r = request.json
#     crawl_id = r["crawl_id"]
#
#     mex = MatioExtractor(eid='e3a377f9-d046-41af-956d-141121ccf712', crawl_id=crawl_id)
#
#     print("SENDING FILES...")
#     # threading.Thread(target=extract_launch, args=([1,2,3]))
#     mex.send_files()
#
#     print("POLLING RESPONSES...")
#     # TODO: This needs to happen in its own thread.
#     mex.poll_responses()
#
#     extract_id = str(uuid4())
#
#     return extract_id
#
#
# @application.route('/get_extract_status', methods=['GET'])
# def get_extr_status():
#
#     r = request.json
#
#     extract_id = r["crawl_id"]
#     resp = get_extract_status(extract_id)  # TODO.
#
#     return resp
#
#
# @application.route('/login', methods=['POST'])
# def login():
#     # Login is primarily handled in the notebooks now.
#     headers = request.json
#     return json.dumps(headers)


if __name__ == '__main__':
    application.run(debug=True)
