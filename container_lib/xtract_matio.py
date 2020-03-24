
import threading
import requests
import logging
import psycopg2
import pickle
import time
import json


from funcx.serialize import FuncXSerializer
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
    def __init__(self, crawl_id, headers, funcx_eid, source_eid, dest_eid,
                 mdata_store_path, logging_level='debug', suppl_store=False):
        self.funcx_eid = funcx_eid
        self.func_id = "148c9dcc-d9c6-430b-b3a7-ddd02193f418"
        self.source_endpoint = source_eid
        self.dest_endpoint = dest_eid

        self.task_dict = {"active": Queue(), "pending": Queue(), "failed": Queue()}

        self.headers = headers
        self.batch_size = 1
        self.max_active_batches = 10
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
            # exit()

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
                    get_mdata = f"SELECT metadata FROM group_metadata_2 where group_id='{gid[0]}' LIMIT 1;"
                    cur.execute(get_mdata)
                    old_mdata = pickle.loads(bytes(cur.fetchone()[0]))
                except Exception as e:  # TODO: this MUST be less broad.
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

                self.logger.debug(f"Num active Task IDs: {self.task_dict['active'].qsize()}")
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
                        get_mdata = f"SELECT metadata FROM group_metadata_2 where group_id='{gid}';"
                        cur.execute(get_mdata)
                        #     cur.execute(get_mdata)  # TODO: Is this second one necessary?
                        old_mdata = pickle.loads(bytes(cur.fetchone()[0]))
                        old_mdata["matio"] = g_obj["matio"]

                        self.logger.debug("Pushing freshly-retrieved metadata to DB (Update: 1/2)")
                        update_mdata = f"UPDATE group_metadata_2 SET metadata={psycopg2.Binary(pickle.dumps(old_mdata))} where group_id='{gid}';"
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

def fatio_test(event):

    """
    Function
    :param event (dict) -- contains auth header and list of HTTP links to extractable files:
    :return metadata (dict) -- metadata as gotten from the materials_io library:
    """
    import threading
    import os
    import time
    import shutil
    import tempfile
    from queue import Queue
    import requests
    import sys

    post_globus_q = Queue()
    post_extract_q = Queue()

    try:
        sys.path.insert(1, '/')
        import xtract_matio_main
    except Exception as e:
        return e

    # A list of file paths
    all_files = event['inputs']
    https_bool = True

    dir_name = None
    if https_bool:
        dir_name = tempfile.mkdtemp()
        os.chdir(dir_name)

    t0 = time.time()
    mdata_list = []

    def get_file(file_path, headers, dir_name, file_id):
        try:
            req = requests.get(file_path, headers)
        except Exception as e:
            return e

        local_file_path = f"{dir_name}/{file_id}"
        # TODO: if response is 200...
        # TODO: Should really be dirname/groupid/fileid
        with open(local_file_path, 'wb') as f:
            f.write(req.content)

    file_dict = {}
    for item in all_files:

        item['headers']['Authorization'] = f"Bearer {item['headers']['Petrel']}"

        file_id = item['file_id']
        file_path = item["url"]

        timeout = 20

        ta = time.time()
        download_thr = threading.Thread(target=get_file, args=(file_path, item['headers'], dir_name, file_id))
        download_thr.start()
        download_thr.join(timeout=timeout)
        if download_thr.is_alive():
            return "The HTTPS download timed out!"

        tb = time.time()

        file_dict[file_id] = {}
        file_dict[file_id]["file_path"] = file_path

    try:
        new_mdata = xtract_matio_main.extract_matio(dir_name)
        post_extract_q.put(new_mdata)
    except Exception as e:
        return str(e)

    t_ext_st = time.time()
    while True:
        if not post_extract_q.empty():
            break

    new_mdata = post_extract_q.get()

    new_mdata['group_id'] = file_id
    # TODO: Bring this back.
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

def matio_test(event):
    import time
    time.sleep(5)
    return "Hello World!"