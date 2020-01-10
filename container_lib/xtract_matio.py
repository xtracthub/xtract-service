
import threading
import requests
import logging
import time
import json

from funcx.serialize import FuncXSerializer
from psycopg2.extras import Json
from queue import Queue

from utils.pg_utils import pg_conn


fx_ser = FuncXSerializer()

# TODO: Figure out how to be smart about these DB requests -- should I have multiple cursors? Commit each? Batch commit?
# TODO: Do a proper logger to a file.


def serialize_fx_inputs(*args, **kwargs):
    from funcx.serialize import FuncXSerializer
    fx_serializer = FuncXSerializer()
    ser_args = fx_serializer.serialize(args)
    ser_kwargs = fx_serializer.serialize(kwargs)
    payload = fx_serializer.pack_buffers([ser_args, ser_kwargs])
    return payload


class MatioExtractor:
    def __init__(self, crawl_id, headers, funcx_eid, globus_eid,
                 mdata_store_path, logging_level='info', suppl_store=False):
        self.funcx_eid = funcx_eid
        self.globus_eid = globus_eid

        # TODO: The endpoints need not be hardcoded.
        self.func_id = "5cdd4602-ffd8-4ddd-acc7-c6e3583807fb"
        self.source_endpoint = 'e38ee745-6d04-11e5-ba46-22000b92c6ec'
        self.dest_endpoint = '5113667a-10b4-11ea-8a67-0e35e66293c2'

        self.live_ids = Queue()
        self.finished_ids = []
        self.headers = headers
        self.batch_size = 1
        self.crawl_id = crawl_id

        self.mdata_store_path = mdata_store_path
        self.suppl_store = suppl_store

        self.conn = pg_conn()
        self.fx_headers = {"Authorization": f"Bearer {self.headers['FuncX']}", 'FuncX': self.headers['FuncX']}
        self.post_url = 'https://dev.funcx.org/api/v1/submit'
        self.get_url = 'https://dev.funcx.org/api/v1/{}/status'

        self.logging_level = logging_level

        if self.logging_level == 'debug':
            logging.basicConfig(format='%(asctime)s - %(message)s', filename='crawler.log', level=logging.DEBUG)
        elif self.logging_level == 'info':
            logging.basicConfig(format='%(asctime)s - %(message)s', filename='crawler.log', level=logging.INFO)
        else:
            raise KeyError("Only logging levels '-d / debug' and '-i / info' are supported.")

        # TODO: Add a preliminary loop-polling 'status check' on the endpoint that returns a noop
        # TODO: And do it here in the init.

    def send_files(self, debug=False):
        while True:
            data = {'inputs': []}

            try:
                query = f"SELECT group_id FROM groups WHERE status='crawled' " \
                    f"and crawl_id='{self.crawl_id}' LIMIT {self.batch_size};"
                cur = self.conn.cursor()
                cur.execute(query)
                logging.debug(f"Successfully retrieved extractable file list of {self.batch_size} items from database.")
            # TODO: Catch connection errors.
            except:
                print("OOPS. ")

            if not debug:
                for gid in cur.fetchall():
                    logging.debug(f"Processing GID: {gid[0]}")

                    # Update DB to flag group_id as 'EXTRACTING'.
                    # TODO: Catch these connection errors.
                    cur = self.conn.cursor()
                    update_q = f"UPDATE groups SET status='EXTRACTING' WHERE group_id='{gid[0]}';"
                    cur.execute(update_q)
                    self.conn.commit()

                    # Get the metadata for each group_id
                    get_mdata = f"SELECT metadata FROM group_metadata where group_id='{gid[0]}';"
                    cur.execute(get_mdata)
                    old_mdata = cur.fetchone()[0]

                    # TODO: Partial groups can occur here.
                    for f_obj in old_mdata["files"]:
                        payload = {
                            # TODO: Un-hardcode.
                            'url': f'https://e38ee745-6d04-11e5-ba46-22000b92c6ec.e.globus.org{f_obj}',
                            'headers': self.fx_headers, 'file_id': gid[0]}
                        data["inputs"].append(payload)
                        data["transfer_token"] = self.headers['Transfer']
                        data["source_endpoint"] = 'e38ee745-6d04-11e5-ba46-22000b92c6ec'
                        data["dest_endpoint"] = '5113667a-10b4-11ea-8a67-0e35e66293c2'

                    res = requests.post(url=self.post_url,
                                        headers=self.fx_headers,
                                        json={
                                            'endpoint': self.funcx_eid,
                                              'func': self.func_id,
                                              'payload': serialize_fx_inputs(
                                                  event=data)}
                                        )

                    fx_res = json.loads(res.content)

                    # TODO: Actually do something here.
                    if fx_res["status"] == "failed":
                        logging.error("TODO: DO SOMETHING IF BAD FUNCX ENDPOINT GIVEN. ")
                    task_uuid = fx_res['task_uuid']
                    self.live_ids.put(task_uuid)

            while True:
                # TODO: this needs to be a tunable parameter. like max_active_tasks.
                if self.live_ids.qsize() <  25:
                    print("FEWER THAN 25 LIVE TASKS! ")
                    break
                else:
                    pass

                cur = self.conn.cursor()
                crawled_query = f"SELECT COUNT(*) FROM groups WHERE status='crawled' AND crawl_id='{self.crawl_id}';"
                cur.execute(crawled_query)
                count_val = cur.fetchall()[0][0]

                logging.debug(f"Files left to extract: {count_val}")

                if count_val == 0:
                    print("There are no more unprocessed objects")
                    break

                time.sleep(1)

    def launch_extract(self):
        ex_thr = threading.Thread(target=self.send_files, args=())
        ex_thr.start()

    def launch_poll(self):
        po_thr = threading.Thread(target=self.poll_responses, args=())
        po_thr.start()

    def poll_responses(self):
        while True:
            if self.live_ids.empty():
                logging.debug("No live IDs... sleeping...")
                time.sleep(1)
                continue

            num_elem = self.live_ids.qsize()
            for _ in range(0, num_elem):
                ex_id = self.live_ids.get()
                status_thing = requests.get(self.get_url.format(ex_id), headers=self.fx_headers)
                status_thing = json.loads(status_thing.content)

                logging.debug(f"Status: {status_thing}")

                if "result" in status_thing:
                    res = fx_ser.deserialize(status_thing['result'])
                    logging.debug(f"Received response: {res}")

                    for g_obj in res["metadata"]:
                        gid = g_obj["group_id"]

                        cur = self.conn.cursor()
                        # TODO: [Optimization] Do something smarter than pulling this down a second time
                        #  (cache somewhere???)
                        get_mdata = f"SELECT metadata FROM group_metadata where group_id='{gid}';"
                        cur.execute(get_mdata)
                        cur.execute(get_mdata)

                        old_mdata = cur.fetchone()[0]
                        old_mdata["matio"] = g_obj["matio"]

                        logging.debug("Pushing freshly-retrieved metadata to DB (Update: 1/2)")
                        update_mdata = f"UPDATE group_metadata SET metadata={Json(old_mdata)} where group_id='{gid}';"
                        cur.execute(update_mdata)

                        # TODO: Make sure we're updating the parser-list in the db.
                        logging.debug("Updating group status (Update: 2/2)")
                        update_q = f"UPDATE groups SET status='EXTRACTED' WHERE group_id='{gid}';"
                        cur.execute(update_q)
                        self.conn.commit()
                    break
                elif 'exception' in status_thing:
                    exc = fx_ser.deserialize(status_thing['exception'])
                    logging.error(f"Caught Exception: {exc}")
                else:
                    self.live_ids.put(ex_id)

            # TODO: Get rid of sleep soon.
            time.sleep(2)


# TODO: .put() data for HTTPS.

# TODO: Smarter separation of groups in file system (but not important at smaller scale).
# TODO: Move these functions to outside this file (like a dir of functions.
def matio_test(event):
    """
    Function
    :param event (dict) -- contains auth header and list of HTTP links to extractable files:
    :return metadata (dict) -- metadata as gotten from the materials_io library:
    """
    import os
    import time
    import shutil
    import sys
    import globus_sdk
    import random
    from globus_sdk import TransferClient, AccessTokenAuthorizer

    sys.path.insert(1, '/')
    import xtract_matio_main

    # Make a temp dir and download the data
    dir_name = f'/home/ubuntu/tmpfileholder-{random.randint(0,999999999)}'
    if os.path.exists(dir_name):
        shutil.rmtree(dir_name)
    os.makedirs(dir_name, exist_ok=True)

    try:
        os.chdir(dir_name)
    except Exception as e:
        return str(e)

    # A list of file paths
    all_files = event['inputs']
    transfer_token = event['transfer_token']
    dest_endpoint = event['dest_endpoint']
    source_endpoint = event['source_endpoint']

    t0 = time.time()
    mdata_list = []

    for item in all_files:
        ta = time.time()
        file_id = item['file_id']

        try:
            authorizer = AccessTokenAuthorizer(transfer_token)
            tc = TransferClient(authorizer=authorizer)

            file_path = item["url"].split('globus.org')[1]
            dest_file_path = dir_name + '/' +  file_path.split('/')[-1]

            tdata = globus_sdk.TransferData(tc, source_endpoint,
                                        dest_endpoint,
                                        label='neat transfer')
        except Exception as e:
            return e

        try:
            # TODO: Switch to regular Globus Transfer w/ polling.
            tdata.add_item(f'{file_path}', dest_file_path, recursive=False)

        except Exception as e:
            return e
        tb = time.time()

        tc.endpoint_autoactivate(source_endpoint)
        tc.endpoint_autoactivate(dest_endpoint)

        try:
            submit_result = tc.submit_transfer(tdata)
        except Exception as e:
            return str(e).split('\n')

        gl_task_id = submit_result['task_id']

        tc.task_wait(gl_task_id)

        # POLL GLOBUS TRANSFER PROGRESS.
        new_mdata = xtract_matio_main.extract_matio(dir_name)

        new_mdata['group_id'] = file_id
        new_mdata['trans_time'] = tb-ta
        mdata_list.append(new_mdata)

        # Don't be an animal -- clean up your mess!
        shutil.rmtree(dir_name)
    t1 = time.time()
    return {'metadata': mdata_list, 'tot_time': t1-t0}


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


