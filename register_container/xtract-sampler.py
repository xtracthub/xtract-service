import json
import sys

from funcx.sdk.client import FuncXClient

fxc = FuncXClient()

location = '039706667969.dkr.ecr.us-east-1.amazonaws.com/xtract-sampler'
description = 'Sampler Extractor'
container_type = 'docker'
name = "xtract/sampler"
container_uuid = fxc.register_container(name, location, description, container_type)

func = """
def sampler_test(event):
    import os
    import tempfile
    import time
    import xtract_sampler_main
    from home_run.base import _get_file
   
   
    from shutil import copyfile 
    
    # Make a temp dir and download the data
    dir_name = tempfile.mkdtemp()
    os.chdir(dir_name)
    
    copyfile('/rf-head-2019-08-26.pkl', f'{dir_name}/rf-head-2019-08-26.pkl')
    copyfile('/CLASS_TABLE.json', f'{dir_name}/CLASS_TABLE.json')
    
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
        
        new_mdata = xtract_sampler_main.extract_sampler(mode='predict', predict_file=input_data, trained_classifier='rf-head-2019-08-26.pkl')
        
        new_mdata['file_id'] = file_id
        
        mdata_list.append(new_mdata)
      
    t1 = time.time()
    return {'metadata': mdata_list, 'tot_time': t1-t0, 'trans_time': tb-ta} 
"""

# TODO: Fix transfer time above!!!! (it's only for 1 file).

func_uuid = fxc.register_function("sampler_test", func, "sampler_test",
                                 description="A test function for the sample extractor.",
                                 container=container_uuid)
# print(func_uuid)
print(func_uuid)

from fair_research_login import NativeClient

client = NativeClient(client_id='7414f0b4-7d05-4bb6-bb00-076fa3f17cf5')
tokens = client.login(
    requested_scopes=['https://auth.globus.org/scopes/56ceac29-e98a-440a-a594-b41e7a084b62/all'])
auth_token = tokens["petrel_https_server"]['access_token']
headers = {'Authorization': f'Bearer {auth_token}'}

data = {'inputs':[]}

payload = {'url': 'https://e38ee745-6d04-11e5-ba46-22000b92c6ec.e.globus.org/MDF/blahblah/Ti3O2.xyz',
                       'headers': headers, 'file_id': 'this-is-my-file-id'}
print("Payload is {}".format(payload))

for i in range(10):
    data['inputs'].append(payload)

endpoint_uuid = '3cc6d6ce-9a60-4559-bf21-1a3d2ce5da20'  # DLHub endpoint for testing
res = fxc.run(data, endpoint_uuid, func_uuid, asynchronous=True)

print("Waiting for result...")
# result = res.result()
print(res)
