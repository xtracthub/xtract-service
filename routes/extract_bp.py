
import globus_sdk
from utils.auth.globus_auth import get_uid_from_token


from flask import Blueprint, request, current_app
from globus_sdk import AccessTokenAuthorizer


from scheddy.scheduler import FamilyLocationScheduler


# TODO: back in the databases
schedulers_by_crawl_id = dict()
status_by_crawl_id = dict()


def test_function():
    # TODO: add ep_id
    return {'is_success': True}


def create_scheduler_thread(fx_eps, crawl_id, headers):
    """
    This is a function that does xyz,
    :param fx_eps (list) : does xyz
    """
    print("Started sched_obj")
    import sys
    print("CREATING SCHEDULER THREAD", file=sys.stdout)
    sched_obj = FamilyLocationScheduler(fx_eps=fx_eps,
                                        crawl_id=crawl_id,
                                        headers=headers)
    print("POST sched obj")
    sched_obj.cur_status = "IN_SCHEDULING"
    schedulers_by_crawl_id[crawl_id] = sched_obj

    # t2 = Thread(target=orch_thread, args=(sched_obj,))
    # t2.start()


""" Routes that have to do with extraction (post-crawling). """
extract_bp = Blueprint('extract_bp', __name__)
active_orchestrators = dict()


@extract_bp.route('/check_ep_configured', methods=['GET'])
def check_ep_configured():
    """ This should check to see which credentials on the endpoint are up-to-date.
    Try some basic API calls to ensure they don't return empty results"""
    return 'Not yet implemented.'


local_mdata_maps = dict()
remote_mdata_maps = dict()


@extract_bp.route('/extract', methods=['POST'])
def extract_mdata():
    r = request.json
    headers = r['tokens']

    try:
        user = get_uid_from_token(str.replace(str(headers['Authorization']), 'Bearer ', ''))
        current_app.logger.info(f"[extract_bp] Authenticated user: {user}")
    except ValueError as e:
        current_app.logger.error(f"[extract_bp] UNABLE TO AUTHENTICATE USER -- CAUGHT: {e}")
        return {'status': 401, 'message': 'Unable to authenticate with given token'}

    # Store these for possibility of transfer later.
    local_mdata_maps[r['crawl_id']] = r['local_mdata_path']
    remote_mdata_maps[r['crawl_id']] = r['remote_mdata_path']

    # Starting a thread containing our FamilyScheduler object.
    status_by_crawl_id[r["crawl_id"]] = "INIT"
    # t1 = Thread(target=create_scheduler_thread, args=([], r["crawl_id"], r["tokens"]))
    # t1.start()
    current_app.logger.info(f"Starting scheduler thread...")
    current_app.logger.info(f"Creating scheduler thread for {r['fx_ep_ids']}")

    import sys
    print(r['fx_ep_ids'], file=sys.stdout)
    create_scheduler_thread(r['fx_ep_ids'], r["crawl_id"], headers)

    return {'status': 200, 'message': 'started extraction!', 'crawl_id': r['crawl_id']}


@extract_bp.route('/get_extract_status', methods=['GET'])
def get_extr_status():
    """
    Return information about the status of the extraction does not include crawl @ /get_crawl_status

    :inputs : request (dict) with keys --> crawl_id (str)
    :returns : response (dict) with keys # TODO.
    """

    r = request.json

    # TODO: add headers here (shouldn't be able to look at someone else's jobs).
    # try:
    #     user = get_uid_from_token(str.replace(str(headers['Authorization']), 'Bearer ', ''))
    #     current_app.logger.info(f"[configure_bp] Authenticated user: {user}")
    # except ValueError as e:
    #     current_app.logger.error(f"[configure_bp] UNABLE TO AUTHENTICATE USER -- CAUGHT: {e}")
    #     return {'status': 401, 'message': 'Unable to authenticate with given token'}

    extract_id = r["crawl_id"]

    sched = schedulers_by_crawl_id[extract_id]
    cur_status = sched.cur_status
    cur_counters = sched.counters
    print(f"STATUS: {cur_status}")

    return {'status': cur_status, 'crawl_id': extract_id,  'counters': cur_counters}


def get_globus_tc(transfer_token):

    authorizer = globus_sdk.AccessTokenAuthorizer(transfer_token)
    tc = globus_sdk.TransferClient(authorizer=authorizer)
    return tc


def funcx_func(event):
    from globus_sdk import SearchClient
    import globus_sdk

    search_token = event['search_token']
    mdata_dir = event['mdata_dir']
    dataset_mdata  = event['dataset_mdata']

    base_gmeta = {"ingest_type": "GMetaList",
                  "ingest_data": {
                      "gmeta": []
                  }
                  }

    # Auth with search
    print(search_token)
    sc = globus_sdk.SearchClient(authorizer=globus_sdk.authorizers.AccessTokenAuthorizer(access_token=search_token))
    #
    # TODO: hardcode.
    files_to_ingest = '/home/tskluzac/mdata'

    from random import randint
    cur_subject = randint(0,100000)

    metadata = dict()
    metadata['file_information'] = {'a': 1, 'b': 2, 'c': 3}
    metadata["keywords"] = {'a': 30, 'b': 20, 'c': 10}

    file_obj = {"subject": str(cur_subject),
                "visible_to": ["public"],
                "content": metadata}

    base_gmeta['ingest_data']['gmeta'].append(file_obj)

    print(base_gmeta)

    index_id = "cba4cfc9-435e-4e88-90ab-cc17ef488d6f"
    x = sc.ingest(index_id, base_gmeta)
    print(x)
    print(x.content)

    #print(x)


    # # TODO: ingest.
    return "HELLO WORLD"


json_to_ingest = {
    # "dc": {
    #     # FILL 1
    #     "titles": [
    #     ],
    #     # FILL 2
    #     "creators": [
    #     ],
    #     # FILL 3
    #     "subjects": [
    #     ],
    #     "publicationYear": "Fill 4",
    #     "publisher": "Fill 5",
    #     "resourceType": "Fill 6",
    #     "dates": [
    #         {
    #             "dateType": "Created",
    #             # "date": "2021-01-01T00:00:00.000000Z"
    #             "date": "2021-09-08T15:39:26.174252Z"
    #         }
    #     ],
    #     "formats": [
    #         "text/plain"
    #     ],
    #     "version": "1"
    # },
    "files": [
        {
            "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            "md5": "d41d8cd98f00b204e9800998ecf8427e",
            "filename": "Fill7.xyz",
            "url": "https://4f99675c-ac1f-11ea-bee8-0e716405a293.e.globus.org/xpcs/xtract-xpcs-1/",  # Fill 8
            "field_metadata": {},
            "mime_type": "text/plain",
            "length": 0
        }
    ],
    "project_metadata": {
        # 'measurement.instrument.acquisition.angle': -1.0,
        "project-slug": "xtract-covid-1",
        "keywords": {'a': 30, 'b': 20, 'c': 10}

    }
}


@extract_bp.route('/ingest_search', methods=['POST'])
def ingest_search():
    r = request.json

    #dataset_mdata = None
    if 'dataset_mdata' in r:
        dataset_mdata = r['dataset_mdata']
    else:
        dataset_mdata = None
    search_index_id = r['search_index_id']
    mdata_dir = r['mdata_dir']
    tokens = r['tokens']

    search_auth=globus_sdk.authorizers.AccessTokenAuthorizer(access_token=tokens['Search'])
    sc = globus_sdk.SearchClient(authorizer=search_auth)
    print("Showing indexes")
    # indexes = [si for si in sc.get("/").data]
    # print(indexes)
    # time.sleep(20)



    from scheddy.scheduler import get_fx_client

    fxc = get_fx_client(tokens)

    search_ingest_fx_eid = "e246aad8-7838-46a3-b260-956eed859c7c"

    event = {
        'dataset_mdata':  dataset_mdata,
        'search_index_id': search_index_id,
        'mdata_dir': mdata_dir,
        'search_token': tokens['Search']
    }

    proc = funcx_func(event)


    # fn_uuid = fxc.register_function(function=funcx_func)
    # #  print(fn_uuid)
    # task_id = fxc.run(event, function_id=fn_uuid, endpoint_id=search_ingest_fx_eid)
    #
    # while True:
    #     try:
    #         x = fxc.get_result(task_id)
    #         print(x)
    #     except Exception as e:
    #         print(e)
    #         time.sleep(1)
    #         continue
    #     time.sleep(1)

