
from flask import Flask, request, jsonify
from enum import Enum

from datetime import datetime, timedelta, timezone

from globus_action_provider_tools.authentication import TokenChecker
# from globus_action_provider_tools.validation import request_validator, response_validator

from globus_sdk import ConfidentialAppAuthClient

from status_checks import get_crawl_status, get_extract_status
from container_lib.xtract_matio import MatioExtractor


from uuid import uuid4
import requests

import json
import os

application = Flask(__name__)


class Status(Enum):
    ACTIVE = "ACTIVE"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    INACTIVE = "INACTIVE"


# TODO: Move this cleanly into a class (and maybe cache for each user).
token_checker = TokenChecker(
        client_id=os.environ["GL_CLIENT_ID"],
        client_secret=os.environ["GL_CLIENT_SECRET"],
        expected_scopes=['https://auth.globus.org/scopes/8df7645a-2ac7-4d4a-a7c5-e085d01bb5b7',
                         'https://auth.globus.org/scopes/cd6f1c83-2802-48b6-94dd-b0c7d027d9df'],
        expected_audience=os.environ["GL_CLIENT_NAME"],
    )

# TODO: Add the action IDs to database for better state.
active_ids = {}


def crawl_launch(crawler, tc):
    crawler.crawl(tc)


@application.route('/', methods=['GET', 'POST'])
def hello():

    resp = {
        "types": ["Action"],
        "api_version": "1.0",
        "globus_auth_scope": "TODO",  # config.our_scope,
        "title": "Xtract Metadata from any Globus Repository",
        "subtitle": (
            "By Tyler J. Skluzacek (UChicago) "
        ),
        "admin_contact": "skluzacek@uchicago.edu",
        "visible_to": ["all_authenticated_users"],
        "runnable_by": ["all_authenticated_users"],
        "log_supported": False,
        "synchronous": False,
    }

    return jsonify(resp)


@application.route('/crawl', methods=['POST'])
def crawl_repo():
    crawl_url = 'http://xtract-crawler-2.p6rys5qcuj.us-east-1.elasticbeanstalk.com/crawl'

    x = requests.post(crawl_url, json=request.json)
    print(x.content)
    return x.content


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
    funcx_eid = r["funcx_eid"]
    globus_eid = r["globus_eid"]
    mdata_store_path = r["mdata_store_path"]

    mex = MatioExtractor(crawl_id=crawl_id,
                         headers=headers,
                         funcx_eid=funcx_eid,
                         globus_eid=globus_eid,
                         mdata_store_path=mdata_store_path)

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


@application.route('/run', methods=['POST'])
def automate_run():

    # auth_header = request.headers.get('Authorization')
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    auth_state = token_checker.check_token(token)
    identities = auth_state.identities

    # TODO 1: get identities
    print(f"Identities: {identities}")

    conf_app = ConfidentialAppAuthClient(os.environ["GL_CLIENT_ID"], os.environ["GL_CLIENT_SECRET"])
    intro_obj = conf_app.oauth2_token_introspect(token)
    print(f"Auth Token Introspection: {intro_obj}")

    dep_grant = conf_app.oauth2_get_dependent_tokens(token)
    # print(dep_grant.data)

    print("Fetching dependent tokens...")
    for grant in dep_grant.data:
        if grant["resource_server"] == "transfer.api.globus.org":
            user_transfer_token = grant["access_token"]
            print(f"User transfer token: {user_transfer_token}")
        elif grant["resource_server"] == "petrel_https_server":
            user_petrel_token = grant["access_token"]
            print(f"User Petrel token: {user_petrel_token}")
        elif grant["resource_server"] == "funcx_server":
            user_funcx_token = grant["access_token"]
            print(f"User funcX token: {user_funcx_token}")

    req = request.get_json(force=True)
    print(f"Run Request: {req}")

    start_time = datetime.now(tz=timezone.utc)
    # request_id = req['request_id']

    body = req['body']
    print(f"Request Body: {body}")
    # manage_by = req['manage_by']
    # monitor_by = req['monitor_by']

    crawl_url = 'http://xtract-crawler-2.p6rys5qcuj.us-east-1.elasticbeanstalk.com/crawl'
    x = requests.post(crawl_url, json=request.json)
    crawl_id = x.content["crawl_id"]
    print(x.content)
    print(crawl_id)

    thawed_idents = []
    for identity in identities:
        print(identity)
        thawed_idents.append(identity)

    # TODO: Make action_id the regular task_id (I think we'd want it to technically be the crawl_id.
    # Now to create the thing we return.
    ret_data = {
        "action_id": crawl_id,
        "status": Status.ACTIVE.value,
        "display_status": Status.ACTIVE.value,
        "details": "the weasel runs at midnight",
        "monitor_by": thawed_idents,
        "manage_by": thawed_idents,
        "start_time": start_time,
        "completion_time": "tomato",  # datetime.now(tz=timezone.utc),
        "release_after": "P30D"
    }

    # NOTE: Actually launching the crawl right here.

    active_ids[crawl_id] = ret_data

    resp = jsonify(ret_data)
    resp.status_code = 202

    return resp


def get_status(job):
    job = job.copy()
    now = datetime.now(tz=timezone.utc)
    priv = job.pop("_private")
    if priv["complete"] > now:
        job["status"] = Status.ACTIVE.value
    else:
        job["details"] = priv["details"]
        job["completion_time"] = priv["complete"]
        if priv["success"] is False:
            job["status"] = Status.FAILED.value
        elif priv["success"] is True:
            job["status"] = Status.SUCCEEDED.value
    return job


@application.route('/<action_id>/status')
def automate_status(action_id):

    print(f"Active IDs: {active_ids}")

    print("IN GET STATUS")
    job_info = active_ids[action_id]
    print("I should actually do some work here!")
    job_info["status"] = Status.SUCCEEDED.value
    return jsonify(job_info)


@application.route('/<action_id>/cancel')
def automate_cancel(action_id):
    print(f"Action ID for cancel: {action_id}")


@application.route('/<action_id>/release')
def automate_release(action_id):
    print(f"Action ID for release: {action_id}")


if __name__ == '__main__':
    application.run(debug=True, threaded=True)
