
import time
import boto3
import pickle
from queue import Queue


from status_checks import get_crawl_status, get_extract_status
from orchestrator.orchestrator import Orchestrator


# Python standard libraries
import threading
import json
import os

# Third-party libraries
from flask import Flask, request
import requests


application = Flask(__name__)

active_orchestrators = dict()


# TODO: move into routes.
def crawl_launch(crawler, tc):
    crawler.crawl(tc)


@application.route('/crawl', methods=['POST'])
def crawl_repo():

    crawl_url = 'http://xtract-crawler-4.eba-ghixpmdf.us-east-1.elasticbeanstalk.com/crawl'

    x = requests.post(url=crawl_url, json=request.json, data=request.data)
    print(f"CRAWL RESPONSE: {x.content}")
    return x.content


@application.route('/get_crawl_status', methods=['GET'])
def get_cr_status():
    """ Returns the status of a crawl. """

    r = request.json

    crawl_id = r["crawl_id"]
    resp = get_crawl_status(crawl_id)
    print(resp)

    return resp


@application.route('/extract', methods=['POST'])
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


@application.route('/get_extract_status', methods=['GET'])
def get_extr_status():

    r = request.json

    extract_id = r["crawl_id"]

    orch = active_orchestrators[extract_id]
    resp = get_extract_status(orch)

    return resp


def fetch_crawl_messages(crawl_id):

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


if __name__ == '__main__':
    application.run(debug=True, threaded=True)  #, ssl_context="adhoc")
