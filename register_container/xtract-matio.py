import json
import sys

from funcx.sdk.client import FuncXClient

fxc = FuncXClient()

location = '039706667969.dkr.ecr.us-east-1.amazonaws.com/xtract-matio'
description = 'Logans MatIO Extractor (Logan Ward; ANL)'
container_type = 'docker'
name = "xtract/matio"
container_uuid = fxc.register_container(name, location, description, container_type)

func2 = """
def matio_test(event):
    import os
    import time
    import tempfile
    import xtract_matio_main
    import shutil
    from home_run.base import _get_file

    # Make a temp dir and download the data
    dir_name = tempfile.mkdtemp()
    os.chdir(dir_name)

    # A list of file paths
    all_files = event['data']['inputs']
   
    t0 = time.time()
    mdata_list = []
    for item in all_files:
        ta = time.time()
        dir_name = tempfile.mkdtemp()
        file_id = item['file_id']
        input_data = _get_file(item, dir_name)  # Download the file
        tb = time.time()
        
        new_mdata = xtract_matio_main.extract_matio(input_data)
        new_mdata['file_id'] = file_id
        new_mdata['trans_time'] = tb-ta
        mdata_list.append(new_mdata)
        
        shutil.rmtree(dir_name)     
    t1 = time.time()
    return {'metadata': mdata_list, 'tot_time': t1-t0}
"""

func = """
def matio_test(event):
    import os
    import tempfile
    import time
    import xtract_matio_main
    from home_run.base import _get_file

    from threading import Thread
    import requests

    from shutil import copyfile 

    # Make a temp dir and download the data
    dir_name = tempfile.mkdtemp()
    os.chdir(dir_name)

    # A list of file paths
    urls = event['data']['inputs']

    def download_url(data):
        file_id = data['file_id']
        url = data['url']
        headers = data['headers']
        print("downloading: ",url)
        file_name_start_pos = url.rfind("/") + 1
        # file_name = url[file_name_start_pos:]
        file_name = file_id
        r = requests.get(url, headers=headers, stream=True)
        if r.status_code == requests.codes.ok:
            with open(file_name, 'wb') as f:
                for data in r:
                    f.write(data)
        return url

    threads = []


    for item in urls:
       t = Thread(target=download_url, args=(item,))
       threads.append(t)
       t.start()

    for t in threads:
        t.join()

    t0 = time.time()
    mdata_list = []

    # return (os.listdir('.'), urls)
    for item in urls:
        # return(input_data)
        ta = time.time()
        file_id = item['file_id']
        tb = time.time()

        new_mdata = xtract_matio_main.extract_matio(file_id)

        new_mdata['file_id'] = file_id

        mdata_list.append(new_mdata)

    t1 = time.time()
    return {'metadata': mdata_list, 'tot_time': t1-t0, 'trans_time': tb-ta} 
"""

# TODO: Fix transfer time above!!!! (it's only for 1 file).

func_uuid = fxc.register_function("matio_test", func, "matio_test",
                                  description="A test function for the sample extractor.",
                                  container=container_uuid)
print(func_uuid)

from fair_research_login import NativeClient

client = NativeClient(client_id='7414f0b4-7d05-4bb6-bb00-076fa3f17cf5')
tokens = client.login(
    requested_scopes=['https://auth.globus.org/scopes/56ceac29-e98a-440a-a594-b41e7a084b62/all'])
auth_token = tokens["petrel_https_server"]['access_token']
headers = {'Authorization': f'Bearer {auth_token}'}

data = {'inputs': []}


import random

for i in range(0, 10):
    payload = {
        'url': 'https://e38ee745-6d04-11e5-ba46-22000b92c6ec.e.globus.org/MDF/mdf_connect/prod/data/au_sr_polymorphism_v1/Au144_MD6341surface1.xyz',
        'headers': headers, 'file_id': str(random.randint(10000, 99999))}

    # print(thingy)
    data['inputs'].append(payload)

print(data)
endpoint_uuid = '3cc6d6ce-9a60-4559-bf21-1a3d2ce5da20'  # DLHub endpoint for testing
# res = fxc.run(data, endpoint_uuid, func_uuid, asynchronous=True)


times = []
res_list = []
for i in range(0, 1):
    res = fxc.run(data, endpoint_uuid, func_uuid, asynchronous=True)
    res_list.append(res)
# res = fxc.run(data, endpoint_uuid, func_uuid, asynchronous=True)

import time
while True:
    for item in res_list:
        print(fxc.get_task_status(res))
    time.sleep(2)

