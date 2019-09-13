import json
import sys

from funcx.sdk.client import FuncXClient

fxc = FuncXClient()

location = '039706667969.dkr.ecr.us-east-1.amazonaws.com/xtract-jsonxml'
description = 'JSON/XML Extractor'
container_type = 'docker'
name = "xtract/xtract-jsonxml"
container_uuid = fxc.register_container(name, location, description, container_type)

func = """
def jsonxml_test(event):
    import os
    import tempfile
    import time
    import xtract_jsonxml_main
    from home_run.base import _get_file

    from shutil import copyfile 

    # Make a temp dir and download the data
    dir_name = tempfile.mkdtemp()
    os.chdir(dir_name)

    # A list of file paths
    all_files = event['data']['inputs']

    t0 = time.time()
    mdata_list = []
    for item in all_files:
        dir_name = tempfile.mkdtemp()
        input_data = _get_file(item, dir_name)  # Download the file
        mdata_list.append(xtract_jsonxml_main.extract_json_metadata(input_data))

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
    'url': 'https://e38ee745-6d04-11e5-ba46-22000b92c6ec.e.globus.org/MDF/mdf_connect/prod/data/mdr_item_891_v1/Ti3O2_LAMMPS/Ti3O2_LAMMPS/Ti3O2.xyz',
    'headers': headers}
print("Payload is {}".format(payload))

for i in range(10):
    data['inputs'].append(payload)

endpoint_uuid = 'a92945a1-2778-4417-8cd1-4957bc35ce66'  # DLHub endpoint for testing
res = fxc.run(data, endpoint_uuid, func_uuid, asynchronous=True)

print("Waiting for result...")
# result = res.result()
print(res)
