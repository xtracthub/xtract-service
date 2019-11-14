
from funcx.serialize import FuncXSerializer
import time
import json
from queue import Queue
import requests
from utils.pg_utils import pg_conn
import threading
import logging

# fxc = FuncXClient()
fx_ser = FuncXSerializer()


def serialize_fx_inputs(*args, **kwargs):
    from funcx.serialize import FuncXSerializer
    fx_serializer = FuncXSerializer()
    ser_args = fx_serializer.serialize(args)
    ser_kwargs = fx_serializer.serialize(kwargs)
    payload = fx_serializer.pack_buffers([ser_args, ser_kwargs])
    return payload


class ExtractionQueue:
    def __init__(self):
        self.queue = Queue()


class ResultsPoller:
    def __init__(self):
        print("HELLO2")


class MatioExtractor:
    def __init__(self, eid, crawl_id, headers):

        # logging.basicConfig(level=logging.INFO, format='%(relativeCreated)6d %(threadName)s %(message)s')

        # logging.debug("hello")

        self.process_endpoint_uuid = '068def43-3838-43b7-ae4e-5b13c24424fb'  # "731bad9b-5f8d-421b-88f5-a386e4b1e3e0"
        self.data_endpoint_uuid = "TODO"
        self.func_id = "f329d678-937f-4c8b-aa24-a660a9383c06"
        self.live_ids = Queue()
        self.finished_ids = []
        self.headers = headers
        self.batch_size = 1
        self.crawl_id = crawl_id
        self.mdata_base_dir = '/projects/DLHub/mdf_metadata/'
        self.conn = pg_conn()
        self.fx_headers = {"Authorization": f"Bearer {self.headers['FuncX']}"}
        self.post_url = 'https://dev.funcx.org/api/v1/submit'
        self.get_url = 'https://dev.funcx.org/api/v1/{}/status'
        self.suppl_store = False# True

    # TODO: Move register_func elsewhere since not using the client anymore.
    # def register_func(self):
    #     func_uuid = fxc.register_function(matio_test,
    #                                       description="A test function for the matio extractor.")
    #     self.func_id = func_uuid
    #     print(f"MatIO Function ID: {self.func_id}")

    def send_files(self, debug=False):
        counter = 0

        while True:
            print(f"Counter: {counter}")
            data = {'inputs': []}

            counter += 1

            try:
                query = f"SELECT group_id FROM groups WHERE status='crawled' and crawl_id='{self.crawl_id}' LIMIT {self.batch_size};"
                cur = self.conn.cursor()
                cur.execute(query)
                print("SUCCESSFULY PULLED DOWN DATA")
            except:
                print("OOPS. ")

            print(debug)

            if not debug:

                print("MADE IT HERE 1. ")
                # print(cur.fetchall())

                for gid in cur.fetchall():
                    print(f"GID: {gid[0]}")
                    # Update DB to flag group_id as 'EXTRACTING'.
                    cur = self.conn.cursor()
                    update_q = f"UPDATE groups SET status='EXTRACTING' WHERE group_id='{gid[0]}';"
                    cur.execute(update_q)
                    self.conn.commit()

                    # Get the metadata for each group_id
                    get_mdata = f"SELECT metadata FROM group_metadata where group_id='{gid[0]}';"
                    cur.execute(get_mdata)
                    cur.execute(get_mdata)

                    old_mdata = cur.fetchone()[0]
                    # pr
                    print(old_mdata)

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
        # logging.debug("Hello from thread launcher")
        ex_thr = threading.Thread(target=self.send_files, args=())
        ex_thr.start()

    def launch_poll(self):
        # logging.debug("Hello from ")
        po_thr = threading.Thread(target=self.poll_responses, args=())
        po_thr.start()


    def poll_responses(self):

        # print(self.live_ids)
        while True:
            # print("New loop iter")
            if self.live_ids.empty():
                print("No live IDs... sleeping...")
                time.sleep(1)
                continue

            num_elem = self.live_ids.qsize()
            # print(num_elem)
            for _ in range(0, num_elem):
                ex_id = self.live_ids.get()
                # status_thing = f.get_task_status(ex_id)
                status_thing = requests.get(self.get_url.format(ex_id), headers=self.fx_headers)

                # print(status_thing.content)
                status_thing = json.loads(status_thing.content)

                print(f"Status: {status_thing}")

                if "result" in status_thing:
                    res = fx_ser.deserialize(status_thing['result'])
                    print(res)

                    for g_obj in res["metadata"]:
                        gid = g_obj["group_id"]

                        cur = self.conn.cursor()
                        # TODO: Do something smarter than pulling this down a second time (cache somewhere???)
                        get_mdata = f"SELECT metadata FROM group_metadata where group_id='{gid}';"
                        cur.execute(get_mdata)
                        cur.execute(get_mdata)

                        old_mdata = cur.fetchone()[0]
                        old_mdata["matio"] = g_obj["matio"]

                        from psycopg2.extras import Json
                        update_mdata = f"UPDATE group_metadata SET metadata={Json(old_mdata)};"
                        cur.execute(update_mdata)
                        self.conn.commit()

                        # Save ALL the metadata as a new (replacement) file.
                        # TODO: Send a funcx function to save data LOL.
# LOL                        with open(f"{self.mdata_base_dir}/{gid}.mdata", 'w') as g:
#                             json.dump(old_mdata, g)
                        # TODO: Update the parser list as well.

                        payload = {'group_id': gid, 'metadata': old_mdata,
                                   'crawl_id': self.crawl_id, 'dirname': 'mdf_metadata/'} # self.mdata_base_dir}

                        print(f"Sending data to FuncX Endpoint: {self.process_endpoint_uuid}")

                        if self.suppl_store:
                            res = requests.post(url=self.post_url,
                                                headers=self.fx_headers,
                                                json={'endpoint': self.process_endpoint_uuid,
                                                      'func': '0797ee6c-0161-4297-a634-8ff3d450c49f',
                                                      'payload': serialize_fx_inputs(
                                                          event=payload)}
                                                )



                        cur = self.conn.cursor()
                        update_q = f"UPDATE groups SET status='EXTRACTED' WHERE group_id='{gid}';"
                        cur.execute(update_q)
                        self.conn.commit()
                    break
                elif 'exception' in status_thing:
                    exc = fx_ser.deserialize(status_thing['exception'])
                    print(exc)
                else:
                    self.live_ids.put(ex_id)
            time.sleep(2)


# TODO: Smarter separation of groups in file system (but not important at smaller scale).
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
        # return "Made it here!"

        new_mdata['group_id'] = file_id
        new_mdata['trans_time'] = tb-ta
        mdata_list.append(new_mdata)

        shutil.rmtree(dir_name)
    t1 = time.time()
    return {'metadata': mdata_list, 'tot_time': t1-t0}


# TODO: Batch metadata saving requests.
def save_mdata(event):

    import json

    main_dir = event['dirname']
    crawl_id = str(event['crawl_id'])
    group_id = str(event['group_id'])
    mdata = event['metadata']

    with open(f"{main_dir}{crawl_id}/{group_id}", 'w') as f:
        json.dump(mdata, f)

    return None

