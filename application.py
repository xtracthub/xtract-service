
import time
import boto3
from flask import jsonify
from enum import Enum
from queue import Queue
from datetime import datetime, timedelta, timezone

# from globus_action_provider_tools.authentication import TokenChecker
# from globus_action_provider_tools.validation import request_validator, response_validator

from globus_sdk import ConfidentialAppAuthClient

from status_checks import get_crawl_status, get_extract_status
from orchestrator import Orchestrator

import pickle
from uuid import uuid4


# Python standard libraries
import threading
import json
import os

# Third-party libraries
from flask import Flask, redirect, request, url_for
import requests


application = Flask(__name__)


class Status(Enum):
    ACTIVE = "ACTIVE"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    INACTIVE = "INACTIVE"

active_orchestrators = dict()

# # TODO: Move this cleanly into a class (and maybe cache for each user).
try:
    token_checker = TokenChecker(
            client_id=os.environ["GL_CLIENT_ID"],
            client_secret=os.environ["GL_CLIENT_SECRET"],
            expected_scopes=['https://auth.globus.org/scopes/8df7645a-2ac7-4d4a-a7c5-e085d01bb5b7',
                             'https://auth.globus.org/scopes/cd6f1c83-2802-48b6-94dd-b0c7d027d9df'],
            expected_audience=os.environ["GL_CLIENT_NAME"],
        )
except:
    print("REMOVE THIS TRY/EXCEPT")

# TODO: Add the action IDs to database for better state.
active_ids = {}


def crawl_launch(crawler, tc):
    crawler.crawl(tc)


@application.route('/', methods=['GET', 'POST'])
def hello():

    # token = request.headers.get("Authorization", "").replace("Bearer ", "")
    # auth_state = token_checker.check_token(token)
    # identities = auth_state.identities
    #
    # print(f"Identities: {identities}")
    #
    # conf_app = ConfidentialAppAuthClient(os.environ["GL_CLIENT_ID"], os.environ["GL_CLIENT_SECRET"])
    # print(os.environ["GL_CLIENT_ID"])
    # print(os.environ["GL_CLIENT_SECRET"])
    #
    # intro_obj = conf_app.oauth2_token_introspect(token)
    # print(f"Auth Token Introspection: {intro_obj}")

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

    crawl_url = 'http://xtract-crawler-4.eba-ghixpmdf.us-east-1.elasticbeanstalk.com/crawl'

    x = requests.post(url=crawl_url, json=request.json, data=request.data)
    print(f"CRAWL RESPONSE: {x.content}")
    return x.content


@application.route('/get_crawl_status', methods=['GET'])
def get_cr_status():

    r = request.json

    crawl_id = r["crawl_id"]
    resp = get_crawl_status(crawl_id)
    print(resp)

    return resp


@application.route('/login/callback', methods=['GET', 'POST'])
def callback():
    return "Hello"


@application.route('/extract', methods=['POST'])
def extract_mdata():

    gdrive_token = None
    source_eid = None
    dest_eid = None
    mdata_store_path = None

    try:
        r = pickle.loads(request.data)
        print(f"Data: {request.data}")
        gdrive_token = r["gdrive_pkl"]
        print(f"Received Google Drive token: {gdrive_token}")
    except pickle.UnpicklingError as e:
        print("Unable to pickle-load for Google Drive! Trying to JSON load for Globus/HTTPS.")

        r = request.json

        if r["repo_type"] in ["GLOBUS", "HTTPS"]:
            source_eid = r["source_eid"]
            dest_eid = r["dest_eid"]
            mdata_store_path = r["mdata_store_path"]
            print(f"Received {r['repo_type']} data!")

    crawl_id = r["crawl_id"]
    headers = json.loads(r["headers"])
    funcx_eid = r["funcx_eid"]

    print("Successfully unpacked data! Initializing orchestrator...")

    # TODO: Can have parallel orchestrators, esp now that we're using queues.
    orch = Orchestrator(crawl_id=crawl_id,
                        headers=headers,
                        funcx_eid=funcx_eid,
                        source_eid=source_eid,
                        dest_eid=dest_eid,
                        mdata_store_path=mdata_store_path,
                        gdrive_token=gdrive_token
                        )

    print("Launching response poller...")
    orch.launch_poll()

    print("Launching metadata extractors...")
    orch.launch_extract()

    # TODO: Shouldn't this extract_id be stored somewhere? 
    extract_id = str(uuid4())
    return extract_id


@application.route('/get_extract_status', methods=['GET'])
def get_extr_status():

    r = request.json

    extract_id = r["crawl_id"]
    resp = get_extract_status(extract_id)

    return resp


# TODO: We need to index the database before doing this.
# TODO:   Otherwise such a query could take hours.
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

    print(f"Identities: {identities}")

    conf_app = ConfidentialAppAuthClient(os.environ["GL_CLIENT_ID"], os.environ["GL_CLIENT_SECRET"])
    print(os.environ["GL_CLIENT_ID"])
    print(os.environ["GL_CLIENT_SECRET"])

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
        elif grant["resource_server"] == "funcx_service":
            user_funcx_token = grant["access_token"]
            print(f"User funcX token: {user_funcx_token}")

    # TODO: Add some sort of token-checking here. "So we avoid 'referenced before assignment' errors"
    req = request.get_json(force=True)
    print(f"Run Request: {req}")

    print(f"Endpoint ID: {req['body']['eid']}")
    print(f"Starting Dir: {req['body']['dir_path']}")
    print(f"Grouper: {req['body']['grouper']}")

    start_time = datetime.now(tz=timezone.utc)

    body = req['body']
    print(f"Request Body: {body}")
    # manage_by = req['manage_by']
    # monitor_by = req['monitor_by']

    # TODO: fix this to match actual crawl requests.
    crawl_req = {'https_info': {'base_url': "https://data.materialsdatafacility.org"},  # TODO: unhardcode
                 'repo_type': "GLOBUS",  # TODO: bounce this out (i.e., make Drive / Globus-friendly)
                 'eid': req['body']['eid'],
                 'dir_path': req['body']['dir_path'],
                 'grouper': req['body']['grouper'],
                 'Transfer': user_transfer_token,
                 'Authorization': user_funcx_token}

    print(f"Crawl Req: {crawl_req}")

    crawl_url = 'http://xtract-crawler-4.eba-ghixpmdf.us-east-1.elasticbeanstalk.com/crawl'
    x = requests.post(crawl_url, json=crawl_req)
    print(x.content)

    crawl_id = json.loads(x.content)["crawl_id"]
    print(f"Crawl ID: {crawl_id}")

    import time
    while True:
        try:
            # Try to get crawl_url
            crawl_status = requests.get(
                f'http://xtractv1-env-2.p6rys5qcuj.us-east-1.elasticbeanstalk.com/get_crawl_status',
                json={'crawl_id': crawl_id})
            print(f"Crawl status: {crawl_status}")
            crawl_content = json.loads(crawl_status.content)

            # If we're now successfully crawling, then BREAK, if not, there won't be a crawl_status (so loop again)
            if crawl_content['crawl_status'] == "crawling":
                break
        except json.JSONDecodeError:
            print("Crawl not yet started! Trying again...")
            time.sleep(1)

    # TODO: Launch the actual extraction right here.
    funcx_ep_id = "82ceed9f-dce1-4dd1-9c45-6768cf202be8"
    source_ep_id = "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec"
    dest_ep_id = "1adf6602-3e50-11ea-b965-0e16720bb42f"

    mdata_path = "UNKNOWN"

    # headers = {'Authorization': f"Bearer {auth_token}", 'Transfer': transfer_token, 'FuncX': funcx_token,
    #            'Petrel': auth_token}

    headers = {'Authorization': f"Bearer {user_petrel_token}", 'Transfer': user_transfer_token, 'FuncX': user_funcx_token,
               'Petrel': user_petrel_token}

    extract_req = requests.post(f'http://xtractv1-env-2.p6rys5qcuj.us-east-1.elasticbeanstalk.com/extract',
                                json={'crawl_id': crawl_id,
                                      'repo_type': "HTTPS",
                                      'headers': json.dumps(headers),  # TODO: How to pack these damn headers.
                                      'funcx_eid': funcx_ep_id,  # TODO: hardcoded to River.
                                      'source_eid': source_ep_id,
                                      'dest_eid': dest_ep_id,
                                      'mdata_store_path': mdata_path})

    print(f"Xtract status: {extract_req.content}")

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

    print(f"Job info: {job_info}")

    crawl_status = requests.get(f'http://xtractv1-env-2.p6rys5qcuj.us-east-1.elasticbeanstalk.com/get_crawl_status',
                                json={'crawl_id': action_id})
    print(f"Crawl status: {crawl_status}")
    crawl_content = json.loads(crawl_status.content)

    print(f"CRAWL CONTENT: {crawl_content}")

    # if crawl_content

    job_info["status"] = Status.SUCCEEDED.value
    return jsonify(job_info)


def fetch_crawl_messages(crawl_id):

    print("IN thread! ")

    client = boto3.client('sqs',
                          aws_access_key_id=os.environ["aws_access"],
                          aws_secret_access_key=os.environ["aws_secret"], region_name='us-east-1')

    response = client.get_queue_url(
        QueueName=f'validate_{crawl_id}',
        QueueOwnerAWSAccountId='576668000072')  # TODO: env variable

    crawl_queue = response["QueueUrl"]

    empty_count = 0

    while True:

        if empty_count == 10:
            print("Empty! Returning! ")
            return   # kill the thread.

        sqs_response = client.receive_message(
            QueueUrl=crawl_queue,
            MaxNumberOfMessages=10,  # TODO: Change back to 10.
            WaitTimeSeconds=1)

        file_list = []
        del_list = []

        if "Messages" in sqs_response:
            num_messages = len(sqs_response["Messages"])
        else:
            empty_count += 1
            time.sleep(0.1)
            continue

        for message in sqs_response["Messages"]:
            message_body = message["Body"]
            print(message_body)

            del_list.append({'ReceiptHandle': message["ReceiptHandle"],
                             'Id': message["MessageId"]})

            mdata = json.loads(message_body)

            files = mdata['files']

            for file_name in files:
                active_orchestrators[crawl_id].put(file_name)

            if len(del_list) > 0:
                response = client.delete_message_batch(
                    QueueUrl=crawl_queue,
                    Entries=del_list)


@application.route('/fetch_mdata', methods=["GET", "POST"])
def fetch_mdata():
    """ Fetch endpoint -- only for Will & Co's GDrive case to fetch their metadata.
    :returns {crawl_id: str, metadata: dict} (dict)"""

    r = request.json
    crawl_id = r['crawl_id']
    n = r['n']

    queue_empty = False

    if crawl_id not in active_orchestrators:
        active_orchestrators[crawl_id] = Queue()

        # here we launch a thread that tries to pull down all metadata.
        thr = threading.Thread(target=fetch_crawl_messages, args=(crawl_id,))
        thr.start()

    plucked_files = 0
    file_list = []
    while plucked_files < n:
        if active_orchestrators[crawl_id].empty():
            queue_empty = True
            break
        file_path = active_orchestrators[crawl_id].get()
        print(file_path)
        plucked_files += 1
        file_list.append(file_path)

    return {"crawl_id": str(crawl_id), "num_files": plucked_files, "file_ls": file_list, "queue_empty": queue_empty}


@application.route('/<action_id>/cancel')
def automate_cancel(action_id):
    print(f"Action ID for cancel: {action_id}")


@application.route('/<action_id>/release')
def automate_release(action_id):
    print(f"Action ID for release: {action_id}")


if __name__ == '__main__':
    application.run(debug=True, threaded=True, ssl_context="adhoc")
