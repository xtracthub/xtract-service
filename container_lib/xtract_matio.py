
import threading
import requests
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
    def __init__(self, eid, crawl_id, headers):

        # TODO: Pass in these 3 args from user.
        self.process_endpoint_uuid = '5e3eed81-1f54-493f-b3fd-b66c6437473b'
        self.data_endpoint_uuid = "TODO"
        self.func_id = "f329d678-937f-4c8b-aa24-a660a9383c06"

        self.live_ids = Queue()
        self.finished_ids = []
        self.headers = headers
        self.batch_size = 1
        self.crawl_id = crawl_id

        # TODO: Pass in these args from user.
        self.mdata_base_dir = '/projects/DLHub/mdf_metadata/'
        self.suppl_store = False

        self.conn = pg_conn()
        self.fx_headers = {"Authorization": f"Bearer {self.headers['FuncX']}"}
        self.post_url = 'https://dev.funcx.org/api/v1/submit'
        self.get_url = 'https://dev.funcx.org/api/v1/{}/status'

    def send_files(self, debug=False):
        counter = 0

        while True:

            data = {'inputs': []}


            try:
                query = f"SELECT group_id FROM groups WHERE status='crawled' and crawl_id='{self.crawl_id}' LIMIT {self.batch_size};"
                cur = self.conn.cursor()
                cur.execute(query)
                print("SUCCESSFULY PULLED DOWN DATA")
            # TODO: Catch connection errors.
            except:
                print("OOPS. ")

            if not debug:

                for gid in cur.fetchall():
                    print(f"Processing GID: {gid[0]}")

                    # Update DB to flag group_id as 'EXTRACTING'.
                    # TODO: Catch these connection errors.
                    cur = self.conn.cursor()
                    update_q = f"UPDATE groups SET status='EXTRACTING' WHERE group_id='{gid[0]}';"
                    cur.execute(update_q)
                    self.conn.commit()

                    # Get the metadata for each group_id
                    get_mdata = f"SELECT metadata FROM group_metadata where group_id='{gid[0]}';"
                    cur.execute(get_mdata)
                    # cur.execute(get_mdata)  # TODO: Why was this executed twice??

                    old_mdata = cur.fetchone()[0]

                    # TODO: Partial groups can occur here.
                    for f_obj in old_mdata["files"]:
                        payload = {
                            # TODO: Un-hardcode.
                            'url': f'https://e38ee745-6d04-11e5-ba46-22000b92c6ec.e.globus.org{f_obj}',
                            'headers': self.headers, 'file_id': gid[0]}
                        data["inputs"].append(payload)

                    res = requests.post(url=self.post_url,
                                        headers=self.fx_headers,
                                        json={'endpoint': self.process_endpoint_uuid,
                                              'func': self.func_id,
                                              'payload': serialize_fx_inputs(
                                                  event=data)}
                                        )

                    task_uuid = json.loads(res.content)['task_uuid']
                    self.live_ids.put(task_uuid)

            while True:
                if self.live_ids.qsize() < 25:
                    print("FEWER THAN 25 LIVE TASKS! ")
                    break
                else:
                    pass

                cur = self.conn.cursor()
                crawled_query = f"SELECT COUNT(*) FROM groups WHERE status='crawled' AND crawl_id='{self.crawl_id}';"
                cur.execute(crawled_query)
                count_val = cur.fetchall()[0][0]

                print(f"FILES LEFT TO EXTRACT: {count_val}")

                if count_val == 0:
                    print("THERE ARE NO MORE UNPROCESSED OBJECTS!")
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
                print("No live IDs... sleeping...")
                time.sleep(1)
                continue

            num_elem = self.live_ids.qsize()
            for _ in range(0, num_elem):
                ex_id = self.live_ids.get()
                status_thing = requests.get(self.get_url.format(ex_id), headers=self.fx_headers)
                status_thing = json.loads(status_thing.content)

                print(f"Status: {status_thing}")

                if "result" in status_thing:
                    res = fx_ser.deserialize(status_thing['result'])
                    print(res)

                    for g_obj in res["metadata"]:
                        gid = g_obj["group_id"]

                        cur = self.conn.cursor()
                        # TODO: [Optimization] Do something smarter than pulling this down a second time (cache somewhere???)
                        get_mdata = f"SELECT metadata FROM group_metadata where group_id='{gid}';"
                        cur.execute(get_mdata)
                        cur.execute(get_mdata)

                        old_mdata = cur.fetchone()[0]
                        old_mdata["matio"] = g_obj["matio"]

                        print("PUSHING METADATA TO DB (Update #1)")
                        update_mdata = f"UPDATE group_metadata SET metadata={Json(old_mdata)} where group_id='{gid}';"
                        cur.execute(update_mdata)

                        # TODO: Make sure we're updating the parser-list in the db.

                        print("DB UPDATE #2")
                        update_q = f"UPDATE groups SET status='EXTRACTED' WHERE group_id='{gid}';"
                        cur.execute(update_q)
                        self.conn.commit()
                    break
                elif 'exception' in status_thing:
                    exc = fx_ser.deserialize(status_thing['exception'])
                    print(exc)
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
    import tempfile
    import shutil
    import sys
    from home_run.base import _get_file

    sys.path.insert(1, '/')
    import xtract_matio_main

    # Make a temp dir and download the data
    dir_name = tempfile.mkdtemp()
    os.chdir(dir_name)

    # A list of file paths
    all_files = event['inputs']

    t0 = time.time()
    mdata_list = []
    for item in all_files:
        ta = time.time()
        dir_name = tempfile.mkdtemp()
        file_id = item['file_id']

        try:
            input_data = _get_file(item, dir_name)  # Download the file
        except Exception as e:
            return e
        tb = time.time()

        extract_path = '/'.join(input_data.split('/')[0:-1])

        new_mdata = xtract_matio_main.extract_matio(extract_path)

        new_mdata['group_id'] = file_id
        new_mdata['trans_time'] = tb-ta
        mdata_list.append(new_mdata)

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

