
import threading
import psycopg2
import requests
import logging
import pickle
import boto3
import time
import json
import os

from exceptions import XtractError, HttpsDownloadTimeout, PetrelRetrievalError, ExtractorError
from funcx.serialize import FuncXSerializer
from utils.pg_utils import pg_conn
from funcx import FuncXClient
from queue import Queue


fx_ser = FuncXSerializer()

# TODO: Do a proper logger to a file.


def serialize_fx_inputs(*args, **kwargs):
    from funcx.serialize import FuncXSerializer
    fx_serializer = FuncXSerializer()
    ser_args = fx_serializer.serialize(args)
    ser_kwargs = fx_serializer.serialize(kwargs)
    payload = fx_serializer.pack_buffers([ser_args, ser_kwargs])
    return payload


class MatioExtractor:
    # TODO: Make source_eid and dest_eid default to None for the HTTPS case?
    def __init__(self, crawl_id, headers, funcx_eid, source_eid, dest_eid,
                 mdata_store_path, logging_level='debug', instance_id=None):

        self.funcx_eid = funcx_eid
        self.func_id = "bb0a2b19-e9f5-49e4-abde-a92220cbb567"
        self.source_endpoint = source_eid
        self.dest_endpoint = dest_eid

        self.task_dict = {"active": Queue(), "pending": Queue(), "failed": Queue()}

        self.max_active_families = 100
        self.families_limit = 100
        self.crawl_id = crawl_id

        self.tiebreak_families = set()

        self.mdata_store_path = mdata_store_path

        self.conn = pg_conn()
        self.headers = headers
        self.fx_headers = {"Authorization": f"Bearer {self.headers['FuncX']}", 'FuncX': self.headers['FuncX']}

        if 'Petrel' in self.headers:
            self.fx_headers['Petrel'] = self.headers['Petrel']

        self.get_url = 'https://dev.funcx.org/api/v1/{}/status'

        self.fx_client = FuncXClient()
        self.logging_level = logging_level

        self.logger = logging.getLogger(__name__)
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.DEBUG)
        self.instance_id = instance_id

        self.sqs_base_url = "https://sqs.us-east-1.amazonaws.com/576668000072/"
        self.client = boto3.client('sqs',
                                   aws_access_key_id=os.environ["aws_access"],
                                   aws_secret_access_key=os.environ["aws_secret"], region_name='us-east-1')
        print(f"Creating queue for crawl_id: {self.crawl_id}")
        xtract_queue = self.client.create_queue(QueueName=f"xtract_{str(self.crawl_id)}")
        validation_queue = self.client.create_queue(QueueName=f"validate_{str(self.crawl_id)}")

        if xtract_queue["ResponseMetadata"]["HTTPStatusCode"] == 200 and \
                validation_queue["ResponseMetadata"]["HTTPStatusCode"] == 200:
            self.xtract_queue_url = xtract_queue["QueueUrl"]
            self.validation_queue_url = validation_queue["QueueUrl"]
        else:
            raise ConnectionError("Received non-200 status from SQS!")

        response = self.client.get_queue_url(
            QueueName=f'crawl_{self.crawl_id}',
            QueueOwnerAWSAccountId='576668000072'
        )

        self.crawl_queue = response["QueueUrl"]

        # TODO: Add a preliminary loop-polling 'status check' on the endpoint that returns a noop
        # TODO: And do it here in the init.

    # TODO: Smarter batching that takes into account file size.
    def get_next_families(self):
        while True:
            try:
                # Step 1. Get ceiling(batch_size/10) messages down from queue.
                t_families_get_start = time.time()
                sqs_response = self.client.receive_message(
                    QueueUrl=self.crawl_queue,
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=20)

                family_list = []
                del_list = []
                for message in sqs_response["Messages"]:
                    message_body = message["Body"]
                    family_list.append(message_body)

                    del_list.append({'ReceiptHandle': message["ReceiptHandle"],
                                     'Id': message["MessageId"]})

                # Step 2. Delete the messages from SQS.
                if len(del_list) > 0:
                    response = self.client.delete_message_batch(
                        QueueUrl=self.crawl_queue,
                        Entries=del_list)
                    # TODO: Do the 200 check. WEird data format.
                    # if response["HTTPStatusCode"] is not 200:
                    #     raise ConnectionError("Could not delete messages from queue!")

                t_families_get_end = time.time()
                logging.debug(f"Time to get families: {t_families_get_end - t_families_get_start}")

                return family_list
            except:
                ConnectionError("Could not get new families! ")

    def pack_and_submit_map(self, data):

        pack_map = self.fx_client.map_run(data, endpoint_id=self.funcx_eid, function_id=self.func_id)

        for item in pack_map:
            self.task_dict["active"].put(item)

    def send_files(self):
        # Just keep looping in case something comes up in crawl.
        while True:
            data = {'inputs': [], "transfer_token": self.headers["Transfer"], "source_endpoint": 'e38ee745-6d04-11e5-ba46-22000b92c6ec', "dest_ep": "1adf6602-3e50-11ea-b965-0e16720bb42f"}

            # TODO: should be, like, get families (and those families can have groups in them)
            # TODO: SHOULD package the exact same way as the family-test example.

            families = self.get_next_families()

            funcx_fam_ship_list = []

            print(len(families))
            print(type(families))

            for family in families:

                family = json.loads(family)
                fid = family["family_id"]
                logging.info(f"Processing family ID: {fid}")

                fam_dict = {"family_id": fid, "files": {}, "groups": {}}

                for group in family["groups"]:

                    gid = group
                    gr = family["groups"][group]
                    parser = gr["parser"]
                    print(parser)
                    files = gr["files"]

                    self.logger.debug(f"Processing GID: {gid}")

                    group = {'group_id': gid, 'files': [], 'parser': parser}

                    # Take all of the files and append them to our payload.
                    # TODO: There is no longer 'old_mdata'.
                    for f_obj in files:
                        id_count = 0

                        # Keep matching here to families.
                        # TODO: Uncomment for Petrel
                        # TODO: Allow endpoint_id to be a URL.
                        # file_url = f'https://{self.source_endpoint}.e.globus.org{f_obj}'
                        file_url = f'https://data.materialsdatafacility.org{f_obj}'
                        payload = {
                            'url': file_url,
                            'headers': self.fx_headers,
                            'file_id': id_count}
                        id_count += 1

                        if file_url not in fam_dict['files']:
                            fam_dict['files'][file_url] = payload

                        group["files"].append(file_url)
                        data["transfer_token"] = self.headers['Transfer']

                    # TODO: I should need these lines when not HTTPS!
                    # data["source_endpoint"] = self.source_endpoint
                    # data["dest_endpoint"] = self.dest_endpoint
                    fam_dict["groups"][gid] = group
                    data["inputs"].append(fam_dict)
                funcx_fam_ship_list.append(data)

            # TODO: What's the failure case here?
            self.pack_and_submit_map(funcx_fam_ship_list)
            # exit()

            while True:
                if self.task_dict["active"].qsize() < self.max_active_families:
                    break
                else:
                    pass

    def launch_extract(self):

        max_threads = 1

        for i in range(0, max_threads):
            ex_thr = threading.Thread(target=self.send_files, args=())
            ex_thr.start()

    def launch_poll(self):
        po_thr = threading.Thread(target=self.poll_responses, args=())
        po_thr.start()

    def poll_responses(self):
        success_returns = 0
        failed_returns = 0
        while True:
            if self.task_dict["active"].empty():
                self.logger.debug("No live IDs... sleeping...")
                time.sleep(2.5)
                continue

            num_elem = self.task_dict["active"].qsize()
            for _ in range(0, num_elem):
                ex_id = self.task_dict["active"].get()

                # TODO: Switch this to batch gets.
                status_thing = requests.get(self.get_url.format(ex_id), headers=self.fx_headers)

                try:
                    status_thing = json.loads(status_thing.content)
                except json.decoder.JSONDecodeError as e:
                    self.logger.error(e)
                    self.logger.error("Status unloadable. Marking as failed.")
                    continue
                    # TODO. What exactly SHOULD I do?

                self.logger.debug(f"Status: {status_thing}")

                if "result" in status_thing:
                    res = fx_ser.deserialize(status_thing['result'])
                    success_returns += 1
                    self.logger.debug(f"Success Counter: {success_returns}")
                    self.logger.debug(f"Failure Counter: {failed_returns}")
                    self.logger.debug(f"Received response: {res}")

                    # TODO: Still need to return group_ids so we can mark accordingly...
                    if "metadata" not in res:
                        if "exception" not in res:
                            raise XtractError("[POLL] Received undefined empty from funcX. Trapping. Marking as failed")
                        else:
                            failed_returns += 1
                            try:
                                res['exception'].reraise()
                            except HttpsDownloadTimeout as e1:
                                self.logger.error(e1)
                                self.logger.error("May need longer timeout or lower network load. "
                                                  "Marking as FAILED for later retry.")

                                # TODO: Put on failed queue.
                                # cur = self.conn.cursor()
                                # for group in res['groups']:
                                #     gid = group["group_id"]
                                #     update_q = f"UPDATE group_metadata_2 SET status='FAILED' WHERE group_id='{gid}';"
                                #     cur.execute(update_q)
                                # self.conn.commit()

                            except PetrelRetrievalError as e2:
                                self.logger.error(e2)
                                # TODO: Should have max_retries that are tracked here.
                                self.logger.error("Petrel Retrieval Error. "
                                                  "Re-queuing immediately...")
                                self.task_dict["active"].put(ex_id)
                            except ExtractorError as e3:
                                self.logger.error(e3)
                                self.logger.error("Not a retry class! "
                                                  "Marking as failed and deleting from work queue!")
                                # cur = self.conn.cursor()
                                # for group in res['groups']:
                                #     gid = group["group_id"]
                                #     update_q = f"UPDATE groups SET status='FAILED' WHERE group_id='{gid}';"
                                #     cur.execute(update_q)
                                self.conn.commit()
                        continue
                            # TODO: Should mark as failed in db.

                    for fam_id in res["metadata"]:

                        group_coll = res["metadata"][fam_id]["metadata"]
                        trans_time = res["metadata"][fam_id]["metadata"]

                        # TODO: Lowkey just delete all of this?
                        # for gid in group_coll:

                            # logging.debug(f"Processing GID: {gid}")
                            # cur = self.conn.cursor()
                            # # TODO: [Optimize] Do something smarter than pulling this down a second time (cache???)
                            # get_mdata = f"SELECT metadata FROM group_metadata_2 where group_id='{gid}';"
                            # cur.execute(get_mdata)
                            # old_mdata = pickle.loads(bytes(cur.fetchone()[0]))
                            #
                            # # TODO: Should we catch the metadata that 'successfully' returned nothing here?
                            # # TODO: Answer -- yes!
                            # parser = list(group_coll[gid].keys())[0]  # TODO: This is weird. There's only 1!!
                            # old_mdata["matio"] = group_coll[gid][parser]["matio"]
                            #
                            # self.logger.debug("Pushing freshly-retrieved metadata to DB (Update: 1/2)")
                            # update_mdata = f"UPDATE group_metadata_2 " \
                            #     f"SET metadata={psycopg2.Binary(pickle.dumps(old_mdata))} where group_id='{gid}';"
                            # cur.execute(update_mdata)
                            #
                            # self.logger.debug("Updating group status (Update: 2/2)")
                            # update_q = f"UPDATE group_metadata_2 SET status='EXTRACTED' WHERE group_id='{gid}';"
                            # cur.execute(update_q)
                            # self.conn.commit()
                            # break
                else:
                    self.task_dict["active"].put(ex_id)


# TODO: .put() data for HTTPS on Petrel.
# TODO: Move these functions to outside this file (like a dir of functions)

def matio_test(event):

    """
    Function
    :param event (dict) -- contains auth header and list of HTTP links to extractable files:
    :return metadata (dict) -- metadata as gotten from the materials_io library:
    """

    import os
    import sys
    import time
    import shutil
    import requests
    import tempfile
    import threading

    t0 = time.time()

    try:
        sys.path.insert(1, '/')
        import xtract_matio_main
        from exceptions import RemoteExceptionWrapper, HttpsDownloadTimeout, ExtractorError, PetrelRetrievalError

    except Exception as e:
        return e

    # A list of file paths
    all_families = event['inputs']
    https_bool = True
    dir_name = None

    if https_bool:
        dir_name = tempfile.mkdtemp()
        os.chdir(dir_name)

    def get_file(file_path, headers, dir_name, file_id, family_id):

        try:
            # file_path is https://link
            # return "but am i here tho"
            # return file_path
            req = requests.get(file_path, headers=headers)
            # return req.co
        except Exception as e:
            # return e
            try:
                raise PetrelRetrievalError(f"Caught the following error when downloading from Petrel: {e}")
            except PetrelRetrievalError:
                return RemoteExceptionWrapper(*sys.exc_info())

        os.makedirs(f"{dir_name}/{family_id}", exist_ok=True)
        local_file_path = f"{dir_name}/{family_id}/{file_path.split('/')[-1]}"

        # return req.content

        # TODO: if response is 200...
        # TODO: IT IS POSSIBLE TO GET A CI LOGON HTML PAGE RETURNED HERE!!!
        with open(local_file_path, 'wb') as f:
            f.write(req.content)
        # TODO: Why do I need this? Lol.
        return req.content

    thread_pool = []
    timeout = 60

    if len(all_families) == 0:
        return "Received no file-group families. Returning!"

    t_download_start = time.time()
    for family in all_families:
        for file_url in family['files']:
            file_payload = family['files'][file_url]

            try:
                try:
                    file_payload['headers']['Authorization'] = f"Bearer {file_payload['headers']['Petrel']}"
                    file_id = file_payload['file_id']
                    file_path = file_payload["url"]
                except Exception as e:
                    raise PetrelRetrievalError(e)
            except PetrelRetrievalError as e:
                return {'exception': RemoteExceptionWrapper(*sys.exc_info()), 'groups': str(e)}

            # TODO: Uncomment when working again.
            download_thr = threading.Thread(target=get_file,
                                            args=(file_path,
                                                  file_payload['headers'],
                                                  dir_name,
                                                  file_id,
                                                  family['family_id']))  # TODO: relax for families.
            download_thr.start()
            thread_pool.append(download_thr)

            # file_content = get_file(file_path, file_payload['headers'], dir_name, file_id, family['family_id'])
            # return file_content

    # Walk through the thread-pool until they've all completed.
    for thr in thread_pool:
        thr.join(timeout=timeout)

        if thr.is_alive():
            try:
                raise HttpsDownloadTimeout(f"Download failed with timeout: {timeout}")
            except HttpsDownloadTimeout:
                return {'exception': RemoteExceptionWrapper(*sys.exc_info()), 'groups': 'todo-- add this back'}
    t_download_finish = time.time()

    total_download_time = t_download_finish - t_download_start
    # return "FILES DOWNLOADED, BUTCH. "

    mat_mdata = None
    family_metadata = {}
    for family in all_families:

        all_groups = family['groups']

        mat_mdata = {}
        for group in all_groups:
            parser = all_groups[group]['parser']
            native_file_collection = all_groups[group]['files']

            local_file_collection = []
            for file_item in native_file_collection:
                local_file_collection.append(dir_name + '/' + family['family_id'] + '/' + file_item.split('/')[-1])

            all_items = []
            try:
                for item in local_file_collection:
                    all_items.append(item)
            except Exception as e:
                return e

            mat_mdata[group] = {}
            try:
                # return "HERE"
                # return all_items
                new_mdata = xtract_matio_main.extract_matio(all_items, parser)
                mat_mdata[group][parser] = new_mdata

            except Exception as e:
                try:
                    raise XtractError(f"Caught the following error trying to extract metadata: {e}")
                except XtractError:
                    return {'exception': RemoteExceptionWrapper(*sys.exc_info())}
                    # TODO: I think we want more returned here (filenames?).
        # return ("Made it here! ")
        family_metadata[family["family_id"]] = {'trans_time': total_download_time, "metadata": mat_mdata}

    # Don't be an animal -- clean up your mess!
    shutil.rmtree(dir_name)
    t1 = time.time()
    # return "MADE IT HERE. "
    return {'metadata': family_metadata, 'tot_time': t1-t0}


# TODO: Batch metadata saving requests (And store in different file).
def save_mdata(event):

    import json

    main_dir = event['dirname']
    crawl_id = str(event['crawl_id'])
    group_id = str(event['group_id'])
    mdata = event['metadata']

    with open(f"{main_dir}{crawl_id}/{group_id}", 'w') as f:
        json.dump(mdata, f)
    return None


def hello_world(event):
    import time
    import os
    time.sleep(1)
    return f"Container Version: {os.environ['container_version']}"
