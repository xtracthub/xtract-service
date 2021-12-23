
import requests

from flask import Blueprint, request

import logging

from status_checks import get_crawl_status
from flask import current_app


""" Routes that have to do with using Xtract's crawler """
crawl_bp = Blueprint('crawl_bp', __name__)
logging.basicConfig(format='%(message)s')  # remove timestamp, already appended by EB.


@crawl_bp.route('/crawl', methods=['POST'])
def crawl_repo():
    current_app.logger.error("[TYLER] IN CRAWL")

    crawl_url = 'http://xtractcrawler5-env.eba-akbhvznm.us-east-1.elasticbeanstalk.com/crawl'

    x = requests.post(url=crawl_url, json=request.json, data=request.data)
    print(f"CRAWL RESPONSE: {x.content}")
    return x.content


@crawl_bp.route('/get_crawl_status', methods=['GET'])
def get_cr_status():
    """ Returns the status of a crawl. """

    r = request.json

    crawl_id = r["crawl_id"]
    resp = get_crawl_status(crawl_id)
    print(f"STATUS RESPONSE: {resp}")

    return resp


@crawl_bp.route('/fetch_mdata', methods=["GET", "POST"])
def fetch_mdata():
    """ Fetch metadata -- get information about metadata objects and return them.
    :returns {crawl_id: str, metadata: dict} (dict)"""

    crawl_url = 'http://xtract-crawler-4.eba-ghixpmdf.us-east-1.elasticbeanstalk.com/fetch_mdata'

    x = requests.post(url=crawl_url, json=request.json, data=request.data)
    print(f"CRAWL RESPONSE: {x.content}")
    return x.content
