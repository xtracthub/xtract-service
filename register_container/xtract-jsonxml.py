import json
import sys

from funcx.sdk.client import FuncXClient

fxc = FuncXClient()

location = '039706667969.dkr.ecr.us-east-1.amazonaws.com/xtract-images'
description = 'JSON/XML Extractor'
container_type = 'docker'
name = "xtract/xtract-images"
container_uuid = fxc.register_container(name, location, description, container_type)

func = """
def jsonxml_test(event):
    import os
    import tempfile
    import time
    import json
    from shutil import copyfile, rmtree
    
    
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
        
        # import json


        t0 = time.time()
        with open(input_data, 'r') as f:
            data = json.load(f)

        field_dict = {'keys': []}
        for key in data:
            field_dict['keys'].append(key)

        t1 = time.time()
        
        field_dict['extract_time'] = t1-t0
        
        print(field_dict)
        
        field_dict['file_id'] = file_id
        field_dict['trans_time'] = tb-ta
        
        mdata_list.append(field_dict)
        rmtree(dir_name)
        

    t1 = time.time()
    return {'metadata': mdata_list, 'tot_time': t1-t0} 
"""

func2 = """
def jsonxml_test(event):
    import os
    import tempfile
    import time
    import xtract_jsonxml_main
    from shutil import copyfile, rmtree


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

        new_mdata = xtract_jsonxml_main.extract_json_metadata(input_data)
        new_mdata['file_id'] = file_id
        new_mdata['trans_time'] = tb-ta

        mdata_list.append(new_mdata)
        rmtree(dir_name)


    t1 = time.time()
    return {'metadata': mdata_list, 'tot_time': t1-t0} 
"""

func_uuid = fxc.register_function("jsonxml_test", func, "jsonxml_test",
                                  description="A test function for the jsonxml extractor.",
                                  container=container_uuid)
# print(func_uuid)
print(func_uuid)

from fair_research_login import NativeClient

client = NativeClient(client_id='7414f0b4-7d05-4bb6-bb00-076fa3f17cf5')
tokens = client.login(
    requested_scopes=['https://auth.globus.org/scopes/56ceac29-e98a-440a-a594-b41e7a084b62/all'])
auth_token = tokens["petrel_https_server"]['access_token']
headers = {'Authorization': f'Bearer {auth_token}'}

data = {'inputs': []}

payload = {
    # TODO: Add a JSONXML file here.
    'url': 'https://e38ee745-6d04-11e5-ba46-22000b92c6ec.e.globus.org/MDF/mdf_connect/prod/data/nist_ip_v1/interchange/potential.1989--Adams-J-B--Au.xml',
    'headers': headers, 'file_id': "Skrrrrrrt"}
print("Payload is {}".format(payload))

for i in range(2):
    data['inputs'].append(payload)

endpoint_uuid = 'a92945a1-2778-4417-8cd1-4957bc35ce66'  # DLHub endpoint for testing

res_list = []
for i in range(1,2):
    res = fxc.run(data, endpoint_uuid, func_uuid, asynchronous=True)
    res_list.append(res)

print("Waiting for result...")
# result = res.result()
import time
while True:

    for res in res_list:
        print(fxc.get_task_status(res))
    time.sleep(2)
