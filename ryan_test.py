import materials_io
from fair_research_login import NativeClient
from queue import Queue
import requests
import time
import threading

n_tasks = 1000

def matio_test(event, thread):
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
    import glob
    import random
    import threading
    from queue import Queue
    from globus_sdk import TransferClient, AccessTokenAuthorizer

    post_globus_q = Queue()
    post_extract_q = Queue()

    sys.path.insert(1, '/')
    import xtract_matio_main

    print('a')

    # Make a temp dir and download the data
    dir_name = f'/Users/tylerskluzacek/tmpfileholder-{random.randint(0,999999999)}'
    print(dir_name)
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

    i = 0
    for item in all_files:
        i += 1
        ta = time.time()
        file_id = item['file_id']

        try:
            authorizer = AccessTokenAuthorizer(transfer_token)
            tc = TransferClient(authorizer=authorizer)

            file_path = item["url"].split('globus.org')[1]
            dest_file_path = dir_name + '/' + file_path.split('/')[-1]

            tdata = globus_sdk.TransferData(tc, source_endpoint,
                                        dest_endpoint,
                                        label=str(thread))
        except Exception as e:
            return e

        try:
            tdata.add_item(f'{file_path}', dest_file_path, recursive=False)

        except Exception as e:
            return e
        tb = time.time()

        tc.endpoint_autoactivate(source_endpoint)
        tc.endpoint_autoactivate(dest_endpoint)

        submit_time = time.time()

        try:
            trans_start = time.time()
            submit_result = tc.submit_transfer(tdata)
        except Exception as e:
            return str(e).split('\n')


        gl_task_id = submit_result['task_id']
        print(f"Globus Task ID: {gl_task_id}")

        # TODO: task_wait obviously isn't working...
        finished = tc.task_wait(gl_task_id, timeout=60)
        print(f"Finished Status: {finished}")

        # return finished
        # TODO: Returning "FINISHED" works okay.

        def task_loop():
            while True:
                r = tc.get_task(submit_result['task_id'])
                if r['status'] == "SUCCEEDED":
                    print(f"Total transfer time: {time.time()-trans_start}")
                    break
                else:
                    time.sleep(0.5)
            post_globus_q.put("SUCCESS1")

        def extract_metadata_loop():
            ts = time.time()
            new_mdata = xtract_matio_main.extract_matio(dir_name)
            post_extract_q.put(new_mdata)
            td = time.time()
            print(f"Total extraction time: {td - ts}")


        # try:
        #     t1 = threading.Thread(target=task_loop, args=())
        #     t1.start()
        # except Exception as e:
        #     return str(e)
        print("Entering task Loop!")
        task_loop()

        # TODO: If not finished in 20 seconds, then return "Waited 20 seconds and returned nothing.
        t_init = time.time()
        while True:
            if not post_globus_q.empty():
                break
            if time.time() - t_init >= 20:
                return "Transfer OPERATION TIMED OUT AFTER 20 seconds"
            time.sleep(1)

        print("Entering extraction Loop!")
        extract_metadata_loop()


        t_ext_st = time.time()
        while True:
            if not post_extract_q.empty():
                break
            if time.time() - t_ext_st >= 120:
                return "Extract TIMED OUT AFTER 20 seconds"

        # return "MADE IT THROUGH MDATA EXTRACT THREAD"

        # return post_extract_q.get()

        new_mdata = post_extract_q.get()

        new_mdata['group_id'] = file_id
        new_mdata['trans_time'] = tb-ta
        mdata_list.append(new_mdata)

        # Don't be an animal -- clean up your mess!
        shutil.rmtree(dir_name)
    t1 = time.time()
    print(mdata_list)
    return {'metadata': mdata_list, 'tot_time': t1-t0}


client = NativeClient(client_id='7414f0b4-7d05-4bb6-bb00-076fa3f17cf5')
tokens = client.login(
    requested_scopes=['https://auth.globus.org/scopes/56ceac29-e98a-440a-a594-b41e7a084b62/all',
                      'urn:globus:auth:scope:transfer.api.globus.org:all',
                     "https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all",
                     'email', 'openid'],
    no_local_server=True,
    no_browser=True)

auth_token = tokens["petrel_https_server"]['access_token']
transfer_token = tokens['transfer.api.globus.org']['access_token']
funcx_token = tokens['funcx_service']['access_token']

headers = {'Authorization': f"Bearer {funcx_token}", 'Transfer': transfer_token, 'FuncX': funcx_token}
print(f"Headers: {headers}")

old_mdata = {"files": ["/MDF/mdf_connect/prod/data/h2o_13_v1-1/split_xyz_files/watergrid_60_HOH_180__0.7_rOH_1.8_vario_PBE0_AV5Z_delta_PS_data/watergrid_PBE0_record-1237.xyz"]}

data = {"inputs": []}
data["transfer_token"] = transfer_token
data["source_endpoint"] = 'e38ee745-6d04-11e5-ba46-22000b92c6ec'  # PETREL
# data["source_endpoint"] = '1c115272-a3f2-11e9-b594-0e56e8fd6d5a'
data["dest_endpoint"] = '1c115272-a3f2-11e9-b594-0e56e8fd6d5a'  # MACBOOK

for f_obj in old_mdata["files"]:
    payload = {
        'url': f'https://e38ee745-6d04-11e5-ba46-22000b92c6ec.e.globus.org{f_obj}',
        'headers': headers, 'file_id': 'googoogoo'}
    data["inputs"].append(payload)


task_dict = {"active": Queue(), "pending": Queue(), "results": [], "failed": Queue()}
t_launch_times = {}
work_q = Queue()
finished_counter = 0

for i in range(n_tasks):
    print(i)
    # TODO: Finish packing this function for local testing.
    work_q.put(data)


num_active = 0


# cmd = "singularity exec /Users/tylerskluzacek/Desktop/matio.simg "

def launch_job(thread):
    while True:
        data = work_q.get()
        matio_test(data, thread)
        print(matio_test)
        done_queue.put("done")


done_queue = Queue()
for i in range(10):
    t1 = threading.Thread(target=launch_job, args=(i,))
    t1.start()


while True:
    print(done_queue.qsize())
    time.sleep(10)





