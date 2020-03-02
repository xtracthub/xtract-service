
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
    # TODO: Cleanup unnnecessary args.
    def __init__(self, crawl_id, headers, funcx_eid, globus_eid,
                 mdata_store_path, logging_level='debug', suppl_store=False):
        self.funcx_eid = funcx_eid
        self.globus_eid = globus_eid

        # TODO: The endpoints need not be hardcoded.
        self.func_id = "91677f55-ff78-4d09-9d87-a111aaf26c69"
        self.source_endpoint = 'e38ee745-6d04-11e5-ba46-22000b92c6ec'
        self.dest_endpoint = globus_eid

        # self.finished_ids = []
        self.task_dict = {"active": Queue(), "pending": Queue(), "results": [], "failed": Queue()}

        self.headers = headers
        self.batch_size = 1
        self.max_active_batches = 2
        self.crawl_id = crawl_id

        self.mdata_store_path = mdata_store_path
        self.suppl_store = suppl_store

        self.conn = pg_conn()
        self.fx_headers = {"Authorization": f"Bearer {self.headers['FuncX']}", 'FuncX': self.headers['FuncX']}
        self.post_url = 'https://dev.funcx.org/api/v1/submit'
        self.get_url = 'https://dev.funcx.org/api/v1/{}/status'

        self.logging_level = logging_level

        self.logger = logging.getLogger(__name__)
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.DEBUG)

        # TODO: Add a preliminary loop-polling 'status check' on the endpoint that returns a noop
        # TODO: And do it here in the init.

    def get_next_groups(self):
        try:
            query = f"SELECT group_id FROM groups WHERE status='crawled' " \
                f"and crawl_id='{self.crawl_id}' LIMIT {self.batch_size};"
            cur = self.conn.cursor()
            cur.execute(query)
            self.logger.debug(f"Successfully retrieved extractable file list of {self.batch_size} items from database.")
            gids = cur.fetchall()
            return gids

        # TODO: Catch connection errors.
        except:
            # TODO: Don't exit lol.
            print("FUCK. ")
            exit()

    def get_test_files(self):

        try:
            query = f"SELECT group_id FROM groups " \
                f"and crawl_id='{self.crawl_id}' LIMIT {self.batch_size};"
            cur = self.conn.cursor()
            cur.execute(query)
            self.logger.debug(f"Successfully retrieved extractable file list of {self.batch_size} items from database.")
            gids = cur.fetchall()
            return gids
        except:
            print("[Xtract] Unable to fetch new group_ids from DB")

    def send_files(self):
        # Just keep looping in case something comes up in crawl.
        while True:
            data = {'inputs': []}

            gids = self.get_next_groups()
            self.logger.info(f"Received {len(gids)} tasks from DB!")

            for gid in gids:
                self.logger.debug(f"Processing GID: {gid[0]}")

                # Update DB to flag group_id as 'EXTRACTING'.
                cur = self.conn.cursor()
                try:
                    update_q = f"UPDATE groups SET status='EXTRACTING' WHERE group_id='{gid[0]}';"
                    cur.execute(update_q)
                    self.conn.commit()
                except Exception as e:
                    print("[Xtract] Unable to update status to 'EXTRACTING'.")
                    print(e)

                # Get the metadata for each group_id
                try:
                    get_mdata = f"SELECT metadata FROM group_metadata where group_id='{gid[0]}' LIMIT 1;"
                    cur.execute(get_mdata)
                    old_mdata = cur.fetchone()[0]
                except Exception as e:
                    print("[Xtract] Unable to retrieve metadata")
                    print(e)

                # TODO: Partial groups can occur here.
                # Take all of the files and append them to our payload.
                for f_obj in old_mdata["files"]:
                    payload = {
                        # TODO: Un-hardcode. This is hardcoded to Petrel data addresses.
                        'url': f'https://e38ee745-6d04-11e5-ba46-22000b92c6ec.e.globus.org{f_obj}',
                        'headers': self.fx_headers, 'file_id': gid[0]}
                    data["inputs"].append(payload)
                    data["transfer_token"] = self.headers['Transfer']

                    data["source_endpoint"] = self.source_endpoint
                    data["dest_endpoint"] = self.dest_endpoint

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
                    self.logger.error("TODO: DO SOMETHING IF BAD FUNCX ENDPOINT GIVEN. ")

                task_uuid = fx_res['task_uuid']

                self.logger.info(f"Placing Task ID {task_uuid} onto live queue")
                self.task_dict["active"].put(task_uuid)
                self.logger.info(f"Approximate current size of live task queue: {self.task_dict['active'].qsize()}")

            while True:
                if self.task_dict["active"].qsize() < self.max_active_batches:
                    self.logger.debug(f"FEWER THAN {self.max_active_batches} LIVE TASKS! ")
                    break
                else:
                    pass

                cur = self.conn.cursor()
                crawled_query = f"SELECT COUNT(*) FROM groups WHERE status='crawled' AND crawl_id='{self.crawl_id}';"
                cur.execute(crawled_query)
                count_val = cur.fetchall()[0][0]

                self.logger.debug(f"Files left to extract: {count_val}")

                if count_val == 0:
                    self.logger.info("There are no more unprocessed objects")
                    break

                time.sleep(1)

    def launch_extract(self):
        ex_thr = threading.Thread(target=self.send_files, args=())
        ex_thr.start()

    def launch_poll(self):
        po_thr = threading.Thread(target=self.poll_responses, args=())
        po_thr.start()

    def poll_responses(self):
        success_returns = 0
        while True:
            if self.task_dict["active"].empty():
                self.logger.debug("No live IDs... sleeping...")
                time.sleep(1)
                continue

            num_elem = self.task_dict["active"].qsize()
            for _ in range(0, num_elem):
                ex_id = self.task_dict["active"].get()
                status_thing = requests.get(self.get_url.format(ex_id), headers=self.fx_headers)
                status_thing = json.loads(status_thing.content)

                self.logger.debug(f"Status: {status_thing}")

                if "result" in status_thing:
                    res = fx_ser.deserialize(status_thing['result'])
                    if res == True:
                        success_returns += 1
                        print(success_returns)
                    res = fx_ser.deserialize(status_thing['result'])
                    self.logger.debug(f"Received response: {res}")

                    for g_obj in res["metadata"]:
                        gid = g_obj["group_id"]

                        cur = self.conn.cursor()
                        # TODO: [Optimize] Do something smarter than pulling this down a second time (cache???)
                        get_mdata = f"SELECT metadata FROM group_metadata where group_id='{gid}';"
                        cur.execute(get_mdata)
                        #     cur.execute(get_mdata)  # TODO: Is this second one necessary?
                        old_mdata = cur.fetchone()[0]
                        old_mdata["matio"] = g_obj["matio"]

                        self.logger.debug("Pushing freshly-retrieved metadata to DB (Update: 1/2)")
                        update_mdata = f"UPDATE group_metadata SET metadata={Json(old_mdata)} where group_id='{gid}';"
                        cur.execute(update_mdata)

                        # TODO: Make sure we're updating the parser-list in the db.
                        self.logger.debug("Updating group status (Update: 2/2)")
                        update_q = f"UPDATE groups SET status='EXTRACTED' WHERE group_id='{gid}';"
                        cur.execute(update_q)
                        self.conn.commit()
                    break
                elif 'exception' in status_thing:
                    exc = fx_ser.deserialize(status_thing['exception'])
                    self.logger.error(f"Caught Exception: {exc}")
                else:
                    self.task_dict["active"].put(ex_id)

            # TODO: Get rid of sleep soon.
            time.sleep(2)


# TODO: .put() data for HTTPS on Petrel.

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
    import threading
    from queue import Queue
    from globus_sdk import TransferClient, AccessTokenAuthorizer
    from datetime import datetime

    post_globus_q = Queue()
    post_extract_q = Queue()

    sys.path.insert(1, '/home/tskluzac/xtract-matio')
    import xtract_matio_main

    # Make a temp dir and download the data
    dir_name = f'/home/tskluzac/tmpfileholder-{random.randint(0,999999999)}'
    if os.path.exists(dir_name):
        shutil.rmtree(dir_name)
    os.makedirs(dir_name, exist_ok=True)

    try:
        os.chdir(dir_name)
        os.chmod(dir_name, 0o777)
    except Exception as e:
        return str(e)

    # A list of file paths
    all_files = event['inputs']
    transfer_token = event['transfer_token']
    dest_endpoint = event['dest_endpoint']
    source_endpoint = event['source_endpoint']
    # file_id = event['file_id']

    # return file_id

    t0 = time.time()
    mdata_list = []

    i = 0
    for item in all_files:
        file_id = item['file_id']
        i += 1

        try:
            authorizer = AccessTokenAuthorizer(transfer_token)
            tc = TransferClient(authorizer=authorizer)

            file_path = item["url"].split('globus.org')[1]
            dest_file_path = dir_name + '/' + file_path.split('/')[-1]

            tdata = globus_sdk.TransferData(tc, source_endpoint,
                                        dest_endpoint,
                                        label=str(i))
        except Exception as e:
            return e

        tdata.add_item(f'{file_path}', dest_file_path, recursive=False)

        tc.endpoint_autoactivate(source_endpoint)
        tc.endpoint_autoactivate(dest_endpoint)

        try:
            submit_result = tc.submit_transfer(tdata)
        except Exception as e:
            return str(e).split('\n')

        # TODO: WE MAKE IT HERE, SO TRANSFER IS AT LEAST SUBMITTED.
        gl_task_id = submit_result['task_id']
        # return gl_task_id

        # TODO: task_wait obviously isn't working...
        finished = tc.task_wait(gl_task_id, timeout=60)

        def task_loop():
            while True:
                r = tc.get_task(submit_result['task_id'])
                if r['status'] == "SUCCEEDED":
                    break
                else:
                    time.sleep(0.5)
            post_globus_q.put("SUCCESS1")

        try:
            t1 = threading.Thread(target=task_loop, args=())
            t1.start()
        except Exception as e:
            return str(e)

        # TODO: If not finished in 20 seconds, then return "Waited 20 seconds and returned nothing.
        t_init = time.time()
        while True:
            if not post_globus_q.empty():
                break
            if time.time() - t_init >= 20:
                return "Transfer OPERATION TIMED OUT AFTER 20 seconds"
            time.sleep(1)

        # TODO: MADE IT THROUGH HERE
        try:
            new_mdata = xtract_matio_main.extract_matio(dir_name, str(datetime.now()))
            post_extract_q.put(new_mdata)
        except Exception as e:
            return str(e)

        t_ext_st = time.time()
        while True:
            if not post_extract_q.empty():
                break
            if time.time() - t_ext_st >= 120:
                return "Extract TIMED OUT AFTER 20 seconds"

        new_mdata = post_extract_q.get()

        new_mdata['group_id'] = file_id
        # TODO: Bring this back.
        # new_mdata['trans_time'] = tb-ta
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
