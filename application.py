
from flask import Flask, request, jsonify, make_response
from flask_api import status
from enum import Enum

from datetime import datetime, timedelta, timezone

from globus_action_provider_tools.authentication import TokenChecker
#from globus_action_provider_tools.validation import request_validator, response_validator
from globus_sdk import ConfidentialAppAuthClient

from status_checks import get_crawl_status, get_extract_status
from container_lib.xtract_matio import MatioExtractor

from openapi_core.wrappers.flask import FlaskOpenAPIResponse, FlaskOpenAPIRequest

from uuid import uuid4
import requests
import time
import error as err

import json
import os

application = Flask(__name__)


class Status(Enum):
    ACTIVE = "ACTIVE"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    INACTIVE = "INACTIVE"


# TODO: Move this cleanly into a class (and maybe cache for each user).
token_checker= TokenChecker(
        client_id=os.environ["GL_CLIENT_ID"],
        client_secret=os.environ["GL_CLIENT_SECRET"],
        expected_scopes=['https://auth.globus.org/scopes/8df7645a-2ac7-4d4a-a7c5-e085d01bb5b7',
                         'https://auth.globus.org/scopes/cd6f1c83-2802-48b6-94dd-b0c7d027d9df'],
        expected_audience=os.environ["GL_CLIENT_NAME"],
    )


def crawl_launch(crawler, tc):
    crawler.crawl(tc)


# @application.before_request
# def before_request():
#     print("IN REQUEST")
#     wrapped_req = FlaskOpenAPIRequest(request)
#     # validation_result = request_validator.validate(wrapped_req)
#     # if validation_result.errors:
#     #     raise err.InvalidRequest(*validation_result.errors)
#
#     token = request.headers.get("Authorization", "").replace("Bearer ", "")
#     print(f"TOKEN: {str(token)}")
#     auth_state = token_checker.check_token(token)
#     if not auth_state.identities:
#         # Returning these authentication errors to the caller will make debugging
#         # easier for this example. Consider whether this is appropriate
#         # for your production use case or not.
#         raise err.NoAuthentication(*auth_state.errors)
#     request.auth = auth_state
#     print("EXITING PRE-REQUEST")


@application.route('/', methods=['GET', 'POST'])
def hello():

    # print(f"Headers: {request.headers.get('Authorization')}")

    # headers = req.headers
    # json_object = req.json

    # print(f"Headers: {headers}")
    # print(f"Content: {json_object}")

    resp = {
        "types": ["Action"],
        "api_version": "1.0",
        "globus_auth_scope": "TODO", # config.our_scope,
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
    # st = status.HTTP_200_OK
    # return f"Welcome to Xtract! \n Status: {str(st)}", st
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
    request_id = req['request_id']

    body = req['body']
    print(f"Request Body: {body}")
    # manage_by = req['manage_by']
    # monitor_by = req['monitor_by']

    action_id = uuid4()
    default_release_after = timedelta(days=30)

    thawed_idents = []
    for identity in identities:
        print(identity)
        thawed_idents.append(identity)


    # Now to create the thing we return.
    ret_data = {
        "action_id": action_id,
        "status": Status.ACTIVE.value,
        "display_status": Status.ACTIVE.value,
        "details": "the weasel runs at midnight",
        "monitor_by": thawed_idents,
        "manage_by": thawed_idents,
        "start_time": start_time,
        "completion_time": datetime.now(tz=timezone.utc),
        "release_after": default_release_after
    }

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
    print("HI")

@application.route('/<action_id>/cancel')
def automate_cancel(action_id):
    print("HI")

@application.route('/<action_id>/release')
def automate_release(action_id):
    print("HI")

if __name__ == '__main__':
    application.run(debug=True, threaded=True)
