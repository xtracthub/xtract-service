
import json
import time
import pickle
import requests
import os

from funcx import FuncXClient

from globus_sdk import AuthClient, AccessTokenAuthorizer, ConfidentialAppAuthClient

from flask import Blueprint, request

from status_checks import get_extract_status
from orchestrator.orchestrator import Orchestrator
from extractors.utils.batch_utils import remote_extract_batch, remote_poll_batch
from extractors.utils.register_function import register_function


def test_function():
    # TODO: add ep_id
    return {'is_success': True}


def configure_function(event):
    import os
    import json
    from uuid import uuid4

    xtract_path, ep_name, globus_eid, funcx_eid, local_download_path, local_mdata_path = event

    full_x_path = os.path.join(xtract_path, ep_name)
    os.makedirs(full_x_path, exist_ok=True)

    # original_umask = os.umask(0)
    # try:
    #     os.makedirs(full_x_path, 777)
    # finally:
    #     os.umask(original_umask)

    full_config_path = os.path.join(full_x_path, 'config.json')
    with open(full_config_path, 'w') as f:
        data = {'xtract_eid': str(uuid4()),
                'globus_eid': globus_eid,
                'funcx_eid': funcx_eid,
                'local_download_path': local_download_path,
                'local_mdata_path': local_mdata_path}
        json.dump(data, f)

    return {'status': 'success', 'xtract_eid': data['xtract_eid']}


""" Routes that have to do with extraction (post-crawling). """
extract_bp = Blueprint('extract_bp', __name__)
active_orchestrators = dict()


@extract_bp.route('/configure_ep/<funcx_eid>', methods=['POST', 'PUT'])
def configure_ep(funcx_eid):
    """ Configuring the endpoint means ensuring that all credentials on the endpoint
        are updated/refreshed, and that the Globus + funcX eps are online"""
    start_time = time.time()

    # Step 0: pull out the headers
    headers = request.json['headers']
    timeout = request.json['timeout']
    ep_name = request.json['ep_name']
    globus_eid = request.json['globus_eid']
    xtract_path = request.json['xtract_path']
    local_download_path = request.json['local_download_path']
    local_mdata_path = request.json['local_mdata_path']

    client = ConfidentialAppAuthClient('a1',
                                       'a2')

    scopes = ["https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all",
              "urn:globus:auth:scope:search.api.globus.org:all",
              "openid"]

    # TODO: bring the following back when we can get that working again
    token_response = client.oauth2_client_credentials_tokens(requested_scopes=scopes)
    #
    # fx_token = token_response.by_resource_server['funcx_service']['access_token']
    # search_token = token_response.by_resource_server['search.api.globus.org']['access_token']
    # openid_token = token_response.by_resource_server['auth.globus.org']['access_token']
    #
    # print(fx_token)
    # print(search_token)
    # print(openid_token)
    #
    # fx_auth = AccessTokenAuthorizer(fx_token)
    # search_auth = AccessTokenAuthorizer(search_token)
    # openid_auth = AccessTokenAuthorizer(openid_token)
    #
    # fx_client = FuncXClient(fx_authorizer=fx_auth,
    #                         search_authorizer=search_auth,
    #                         openid_authorizer=openid_auth)

    # TODO: Get rid of this when proper authorizers working.
    #fx_client = FuncXClient(force_login=True)
    fx_client = FuncXClient()

    print(dir(fx_client))

    print(fx_client.TOKEN_DIR, fx_client.TOKEN_FILENAME, fx_client.BASE_USER_AGENT)

    # return

    # TODO: boot this outside to avoid wasteful funcX calls.
    reg_func_id = fx_client.register_function(function=test_function)

    print(f"Successfully registered check-function with ID: {reg_func_id}")

    # Step 1: use funcX function to ensure endpoint is online and returning tasks.
    #  --> Only taking first element in batch, as our batch size is only 1.
    task_id = fx_client.run(endpoint_id=funcx_eid, function_id=reg_func_id)
    # remote_extract_batch(items_to_batch=[{'func_id': reg_func_id, 'event': None}],
    #                            ep_id=funcx_eid,
    #                            headers=headers)

    print(f"Result from config extract: {task_id}")

    while True:
        result = fx_client.get_batch_result(task_id_list=[task_id])
        print(result)
        if 'exception' in result[task_id]:
            result[task_id]['exception'].reraise()

        if result[task_id]['status'] == 'success':
            #return {'config_status': 'SUCCESS', 'fx_eid': funcx_eid}
            print("Sucessfully returned test function. Breaking!")
            break

        elif result[task_id]['status'] == 'FAILED':
            return {'config_status': 'FAILED', 'fx_eid': funcx_eid, 'msg': 'funcX internal failure'}

        else:
            if time.time() - start_time > timeout:
                return {'config_status': "FAILED", 'fx_id': funcx_eid, 'msg': 'funcX return timeout'}
            else:
                time.sleep(2)

    reg_func_id = fx_client.register_function(function=configure_function)

    print(f"Successfully registered check-function with ID: {reg_func_id}")

    event = [xtract_path, ep_name, globus_eid, funcx_eid, local_download_path, local_mdata_path]

    # Step 1: use funcX function to ensure endpoint is online and returning tasks.
    #  --> Only taking first element in batch, as our batch size is only 1.
    task_id = fx_client.run(event=event, endpoint_id=funcx_eid, function_id=reg_func_id)
    # remote_extract_batch(items_to_batch=[{'func_id': reg_func_id, 'event': None}],
    #                            ep_id=funcx_eid,
    #                            headers=headers)

    print(f"Result from config extract: {task_id}")

    while True:
        result = fx_client.get_batch_result(task_id_list=[task_id])
        print(result)
        if 'exception' in result[task_id]:
            result[task_id]['exception'].reraise()

        if result[task_id]['status'] == 'success':
            return {'config_status': 'SUCCESS', 'fx_eid': funcx_eid}

        elif result[task_id]['status'] == 'FAILED':
            return {'config_status': 'FAILED', 'fx_eid': funcx_eid, 'msg': 'funcX internal failure'}

        else:
            if time.time() - start_time > timeout:
                return {'config_status': "FAILED", 'fx_id': funcx_eid, 'msg': 'funcX return timeout'}
        time.sleep(2)



@extract_bp.route('/check_ep_configured', methods=['GET'])
def check_ep_configured():
    """ This should check to see which credentials on the endpoint are up-to-date.
    Try some basic API calls to ensure they don't return empty results"""
    pass


@extract_bp.route('/extract', methods=['POST'])
def extract_mdata():

    gdrive_token = None
    source_eid = None
    dest_eid = None
    mdata_store_path = None
    extractor_finder = None
    prefetch_remote = None
    data_prefetch_path = None
    dataset_mdata = None


    try:
        r = pickle.loads(request.data)
        print(f"Data: {request.data}")
        gdrive_token = r["gdrive_pkl"]
        extractor_finder = "gdrive"
        print(f"Received Google Drive token: {gdrive_token}")
    except pickle.UnpicklingError as e:
        print("Unable to pickle-load for Google Drive! Trying to JSON load for Globus/HTTPS.")

        r = request.json

        if r["repo_type"] in ["GLOBUS", "HTTPS"]:
            source_eid = r["source_eid"]
            dest_eid = r["dest_eid"]
            mdata_store_path = r["mdata_store_path"]
            print(f"Received {r['repo_type']} data!")
            extractor_finder = "matio"
            print("SETTING PREFETCH REMOTE")
            prefetch_remote = r["prefetch_remote"]

            data_prefetch_path = r['data_prefetch_path']

            if 'dataset_mdata' in r:
                dataset_mdata = r['dataset_mdata']
            else:
                dataset_mdata = None

    crawl_id = r["crawl_id"]
    headers = json.loads(r["headers"])
    funcx_eid = r["funcx_eid"]

    if crawl_id in active_orchestrators:  # TODO: improved error-handling.
        return "ERROR -- crawl_id already has an associated orchestrator!"

    print("Successfully unpacked data! Initializing orchestrator...")

    # TODO: Can have parallel orchestrators, esp now that we're using queues.
    orch = Orchestrator(crawl_id=crawl_id,
                        headers=headers,
                        funcx_eid=funcx_eid,
                        source_eid=source_eid,
                        dest_eid=dest_eid,
                        mdata_store_path=mdata_store_path,
                        gdrive_token=gdrive_token,
                        extractor_finder=extractor_finder,
                        prefetch_remote=prefetch_remote,
                        data_prefetch_path=data_prefetch_path,
                        dataset_mdata=dataset_mdata
                        )

    print("Launching response poller...")
    orch.launch_poll()

    print("Launching metadata extractors...")
    orch.launch_extract()

    active_orchestrators[crawl_id] = orch

    extract_id = crawl_id
    return extract_id  # TODO: return actual response object.


@extract_bp.route('/get_extract_status', methods=['GET'])
def get_extr_status():

    r = request.json

    extract_id = r["crawl_id"]

    orch = active_orchestrators[extract_id]
    resp = get_extract_status(orch)

    return resp
