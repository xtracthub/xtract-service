import json
import time
import pickle
import globus_sdk

from funcx import FuncXClient
from flask import Blueprint, request
from globus_sdk import AccessTokenAuthorizer
from extractors.xtract_keyword import KeywordExtractor
from extractors.extractor import base_extractor
from tests.test_utils.mock_event import create_mock_event


from status_checks import get_extract_status
from scheddy.scheduler import FamilyLocationScheduler
# from orchestrator.orchestrator import ExtractorOrchestrator
from threading import Thread


# TODO: back in the databases
schedulers_by_crawl_id = dict()
status_by_crawl_id = dict()


def test_function():
    # TODO: add ep_id
    return {'is_success': True}


def create_scheduler_thread(fx_eps, crawl_id, headers):

    print("Started sched_obj")
    sched_obj = FamilyLocationScheduler(fx_eps=fx_eps,
                                        crawl_id=crawl_id,
                                        headers=headers)
    print("POST sched obj")
    sched_obj.cur_status = "IN_SCHEDULING"
    schedulers_by_crawl_id[crawl_id] = sched_obj

    # t2 = Thread(target=orch_thread, args=(sched_obj,))
    # t2.start()





def configure_function(event):
    import os
    import json
    from uuid import uuid4

    xtract_path, ep_name, globus_eid, funcx_eid, local_download_path, local_mdata_path = event

    full_x_path = os.path.join(xtract_path, ep_name)
    os.makedirs(full_x_path, exist_ok=True)

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


# @extract_bp.route('/configure_ep/<funcx_eid>', methods=['POST'])
# def configure_ep(funcx_eid):
#     """ Configuring the endpoint means ensuring that all credentials on the endpoint
#         are updated/refreshed, and that the Globus + funcX eps are online"""
#     start_time = time.time()

#     # Step 0: pull out the headers
#     headers = request.json['headers']
#     timeout = request.json['timeout']
#     ep_name = request.json['ep_name']
#     globus_eid = request.json['globus_eid']
#     xtract_path = request.json['xtract_path']
#     local_download_path = request.json['local_download_path']
#     local_mdata_path = request.json['local_mdata_path']

#     # dep_tokens = client.oauth2_get_dependent_tokens(headers['Authorization'])
#     fx_auth = AccessTokenAuthorizer(headers['Authorization'])
#     search_auth = AccessTokenAuthorizer(headers['Search'])
#     openid_auth = AccessTokenAuthorizer(headers['Openid'])

#     print(fx_auth.access_token)
#     print(search_auth.access_token)
#     print(openid_auth.access_token)

#     fx_client = FuncXClient(fx_authorizer=fx_auth,
#                             search_authorizer=search_auth,
#                             openid_authorizer=openid_auth)

#     print(dir(fx_client))

#     print(fx_client.TOKEN_DIR, fx_client.TOKEN_FILENAME, fx_client.BASE_USER_AGENT)

#     # TODO: boot this outside to avoid wasteful funcX calls.
#     reg_func_id = fx_client.register_function(function=test_function)

#     print(f"Successfully registered check-function with ID: {reg_func_id}")

#     # Step 1: use funcX function to ensure endpoint is online and returning tasks.
#     #  --> Only taking first element in batch, as our batch size is only 1.
#     task_id = fx_client.run(endpoint_id=funcx_eid, function_id=reg_func_id)

#     print(f"Result from config extract: {task_id}")

#     while True:
#         result = fx_client.get_batch_result(task_id_list=[task_id])
#         print(result)
#         if 'exception' in result[task_id]:
#             result[task_id]['exception'].reraise()

#         if result[task_id]['status'] == 'success':
#             print("Successfully returned test function. Breaking!")
#             break

#         elif result[task_id]['status'] == 'FAILED':
#             return {'config_status': 'FAILED', 'fx_eid': funcx_eid, 'msg': 'funcX internal failure'}

#         else:
#             if time.time() - start_time > timeout:
#                 return {'config_status': "FAILED", 'fx_id': funcx_eid, 'msg': 'funcX return timeout'}
#             else:
#                 time.sleep(2)

#     reg_func_id = fx_client.register_function(function=configure_function)

#     print(f"Successfully registered check-function with ID: {reg_func_id}")

#     event = [xtract_path, ep_name, globus_eid, funcx_eid, local_download_path, local_mdata_path]

#     # Step 1: use funcX function to ensure endpoint is online and returning tasks.
#     #  --> Only taking first element in batch, as our batch size is only 1.
#     task_id = fx_client.run(event=event, endpoint_id=funcx_eid, function_id=reg_func_id)

#     print(f"Result from config extract: {task_id}")

#     while True:
#         result = fx_client.get_batch_result(task_id_list=[task_id])
#         print(result)
#         if 'exception' in result[task_id]:
#             result[task_id]['exception'].reraise()

#         if result[task_id]['status'] == 'success':
#             return {'config_status': 'SUCCESS', 'fx_eid': funcx_eid}

#         elif result[task_id]['status'] == 'FAILED':
#             return {'config_status': 'FAILED', 'fx_eid': funcx_eid, 'msg': 'funcX internal failure'}

#         else:
#             if time.time() - start_time > timeout:
#                 return {'config_status': "FAILED", 'fx_id': funcx_eid, 'msg': 'funcX return timeout'}
#         time.sleep(2)




@extract_bp.route('/check_ep_configured', methods=['GET'])
def check_ep_configured():
    """ This should check to see which credentials on the endpoint are up-to-date.
    Try some basic API calls to ensure they don't return empty results"""
    return 'Not yet implemented.'


local_mdata_maps = dict()
remote_mdata_maps = dict()





@extract_bp.route('/debug_orch', methods=['POST'])
def debug_orch():

    ep_id = "e1398319-0d0f-4188-909b-a978f6fc5621"
    r = request.json
    crawl_id = r['crawl_id']
    headers = r['tokens']

    test_file = '/home/tskluzac/tyler_research_files/MOTW.docx'

    mock_event = create_mock_event([test_file])
    ext = KeywordExtractor()

    tabular_event = ext.create_event(ep_name="foobar",
                                 family_batch=mock_event['family_batch'],
                                 xtract_dir="/home/tskluzac/.xtract",
                                 sys_path_add="/",
                                 module_path="xtract_keyword_main",
                                 metadata_write_path='/home/tskluzac/mdata')

    from scheddy.maps.function_ids import functions, containers
    fxc = get_fx_client(crawl_id, headers)

    fn_uuid = functions['keyword']

    print(fn_uuid)

    res = fxc.run(tabular_event,
                  endpoint_id=ep_id, function_id=fn_uuid)
    print(res)
    for i in range(100):
        try:
            x = fxc.get_result(res)
            print(x)
            break
        except Exception as e:
            print("Exception: {}".format(e))
            time.sleep(2)

    return "NICE"


@extract_bp.route('/extract', methods=['POST'])
def extract_mdata():
    r = request.json

    # Store these for possibility of transfer later.
    local_mdata_maps[r['crawl_id']] = r['local_mdata_path']
    remote_mdata_maps[r['crawl_id']] = r['remote_mdata_path']

    # Starting a thread containing our FamilyScheduler object.
    status_by_crawl_id[r["crawl_id"]] = "INIT"
    t1 = Thread(target=create_scheduler_thread, args=([], r["crawl_id"], r["tokens"]))
    t1.start()

    return {'status': 200, 'message': 'started extraction!', 'crawl_id': r['crawl_id']}


@extract_bp.route('/get_extract_status', methods=['GET'])
def get_extr_status():
    """
    Return information about the status of the extraction does not include crawl @ /get_crawl_status

    :inputs : request (dict) with keys --> crawl_id (str)
    :returns : response (dict) with keys # TODO.
    """

    r = request.json

    extract_id = r["crawl_id"]

    sched = schedulers_by_crawl_id[extract_id]
    cur_status = sched.cur_status
    print(f"STATUS: {cur_status}")

    return {'status': cur_status, 'crawl_id': extract_id}


def get_globus_tc(transfer_token):

    authorizer = globus_sdk.AccessTokenAuthorizer(transfer_token)
    tc = globus_sdk.TransferClient(authorizer=authorizer)
    return tc


@extract_bp.route('/ingest_search', methods=['POST'])
def ingest_search():
    r = request.json

    search_index_id = r['search_index_id']
    mdata_dir = r['metadata']

    def funcx_func(event):
        from globus_sdk import SearchClient
        import os

        search_token = event['search_token']
        ingest_dir = event['ingest_dir']

        # TODO: auth with search.
        sc = globus_sdk.SearchClient(authorizer=globus_sdk.authorizers.AccessTokenAuthorizer(access_token=search_token))

        # TODO: ingest all metadata in folder.
        files_to_ingest = '/home/tskluzac/mdata'

        # TODO: ingest.


@extract_bp.route('/offload_mdata', methods=['POST'])
def offload_mdata():

    # TODO: only transfer if extraction is complete.
    # TODO: add proper polling.
    r = request.json

    crawl_id = r['crawl_id']
    tokens = r['tokens']

    source_ep = r['source_ep']
    mdata_ep = r['mdata_ep']

    source_dir = local_mdata_maps[crawl_id]
    dest_dir = remote_mdata_maps[crawl_id]

    print(f"Source EP: {source_ep}")
    print(f"Source Dir: {source_dir}")

    print(f"Dest EP: {mdata_ep}")
    print(f"Dest Dir: {dest_dir}")

    tc = get_globus_tc(tokens['Transfer'])

    tdata = globus_sdk.TransferData(transfer_client=tc,
                                    source_endpoint=source_ep,
                                    destination_endpoint=mdata_ep)

    tdata.add_item(source_dir, dest_dir, recursive=True)
    transfer_result = tc.submit_transfer(tdata)

    print(transfer_result)
    return {"status": "SUCCESS"}
