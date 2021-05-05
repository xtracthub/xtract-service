
import json
import pickle
import requests

from flask import Blueprint, request

from status_checks import get_extract_status
from orchestrator.orchestrator import Orchestrator


""" Routes that have to do with extraction (post-crawling). """
extract_bp = Blueprint('extract_bp', __name__)
active_orchestrators = dict()


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
