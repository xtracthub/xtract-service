
from funcx.sdk.client import FuncXClient
from funcx.serialize import FuncXSerializer
import random
import time
import json
from queue import Queue
import requests
from utils.pg_utils import pg_conn

# TODO: Have to use POST request.
# fxc = FuncXClient()
fx_ser = FuncXSerializer()


def serialize_fx_inputs(*args, **kwargs):
    from funcx.serialize import FuncXSerializer
    fx_serializer = FuncXSerializer()
    ser_args = fx_serializer.serialize(args)
    ser_kwargs = fx_serializer.serialize(kwargs)
    payload = fx_serializer.pack_buffers([ser_args, ser_kwargs])
    return payload


class MatioExtractor:
    def __init__(self, eid, crawl_id, headers):

        self.process_endpoint_uuid = eid  # "731bad9b-5f8d-421b-88f5-a386e4b1e3e0"
        self.data_endpoint_uuid = "TODO"
        self.func_id = "f329d678-937f-4c8b-aa24-a660a9383c06"
        self.live_ids = Queue()
        self.finished_ids = []
        self.headers = headers
        self.batch_size = 1
        self.crawl_id = crawl_id
        self.mdata_base_dir = './xtract_metadata'
        self.conn = pg_conn()
        self.fx_headers = {"Authorization": f"Bearer {self.headers['FuncX']}"}
        self.post_url = 'https://dev.funcx.org/api/v1/submit'
        self.get_url = 'https://dev.funcx.org/api/v1/{}/status'


        # request.post dev/funcx.org/api/v1/submit  -- set authorization header with funcx called Authorization (with 'Bearer' stuff)
        # func, endpoint_id, payload


    # TODO: Move register_func elsewhere since not using the client anymore.
    # def register_func(self):
    #     func_uuid = fxc.register_function(matio_test,
    #                                       description="A test function for the matio extractor.")
    #     self.func_id = func_uuid
    #     print(f"MatIO Function ID: {self.func_id}")


    def send_files(self, debug=False):
        # headers = get_headers()
        counter = 0

        while counter < 2:
            data = {'inputs': []}

            counter += 1
            query = f"SELECT group_id FROM groups WHERE status='crawled' and crawl_id='{self.crawl_id}' LIMIT {self.batch_size};"
            cur = self.conn.cursor()
            cur.execute(query)

            if not debug:

                for gid in cur.fetchall():

                    cur = self.conn.cursor()
                    update_q = f"UPDATE groups SET status='EXTRACTING' WHERE group_id='{gid[0]}';"
                    cur.execute(update_q)
                    self.conn.commit()

                    filename = f'{self.mdata_base_dir}/{gid[0]}.mdata'

                    print(f"ATTEMPTING TO OPEN FILE: {filename}")

                    with open(filename, 'r') as f:
                        old_mdata = json.load(f)
                        print(f"Old metadata: {old_mdata}")

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

            # This is completely pointless, but it's just a well-defined file I use for testing funcX :)
            # else:
            #     print("Sending debug file to funcX...")
            #     payload = {
            #         'url': 'https://e38ee745-6d04-11e5-ba46-22000b92c6ec.e.globus.org/MDF/mdf_connect/prod/data'
            #                '/au_sr_polymorphism_v1/Au144_MD6341surface1.xyz',
            #         'headers': self.headers, 'file_id': str(random.randint(10000, 99999))}
            #
            #     data["inputs"].append(payload)
            #
            #     # res = fxc.run(data, endpoint_id=self.endpoint_uuid, function_id=self.func_id)
            #     res = requests.post(url=self.post_url,
            #                         headers=self.fx_headers,
            #                         json={'endpoint': '068def43-3838-43b7-ae4e-5b13c24424fb',
            #                               'func': self.func_id,
            #                               'payload': serialize_fx_inputs(data=data)})
            #
            #     print(f"Appending ID {res} to live_ids")
            #     self.live_ids.put(res)
            #     print(res)

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

    def poll_responses(self):

        # print(self.live_ids)
        while True:
            # print("New loop iter")
            if self.live_ids.empty():
                break

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

                        # Get the metadata from the old file.
                        with open(f"{self.mdata_base_dir}/{gid}.mdata", 'r') as f:
                            mdata = json.load(f)
                            mdata["matio"] = g_obj["matio"]

                        # Save ALL the metadata as a new (replacement) file.
                        with open(f"{self.mdata_base_dir}/{gid}.mdata", 'w') as g:
                            json.dump(mdata, g)

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
            time.sleep(0.25)


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
