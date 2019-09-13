
from funcx.sdk.client import FuncXClient

fxc = FuncXClient()

location = '039706667969.dkr.ecr.us-east-1.amazonaws.com/xtract-images'
description = 'Xtract image types container'
container_type = 'docker'
name = "xtract/images"
container_uuid = fxc.register_container(name, location, description, container_type)


func = """
def images_xtract(event):
   import os
   import tempfile
   import xtract_images_main
   import time
   
   from home_run.base import _get_file
   from shutil import copyfile, rmtree
   
   t0 = time.time()
   
   # sdMake a temp dir and download the data
   dir_name = tempfile.mkdtemp()
   os.chdir(dir_name)
   
   # os.mkdir('app')
   copyfile('/app/pca_model.sav', f'{dir_name}/pca_model.sav')
   copyfile('/app/clf_model.sav', f'{dir_name}/clf_model.sav')
   
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
      
      new_mdata = xtract_images_main.extract_image('predict', input_data)
      new_mdata['file_id'] = file_id
      new_mdata['trans_time'] = tb-ta
      
      mdata_list.append(new_mdata)
      shutil.rmtree(dir_name)      
   t1 = time.time()
   return {'metadata': mdata_list, 'tot_time': t1-t0} 
"""
func_uuid = fxc.register_function("images_xtract", func, "images_xtract",
                                 description="A function for the images extractor.",
                                 container=container_uuid)

# func_uuid = "d4b1649a-11a8-4325-a215-e199dca5ca47"
print(func_uuid)

from fair_research_login import NativeClient

client = NativeClient(client_id='7414f0b4-7d05-4bb6-bb00-076fa3f17cf5')
tokens = client.login(
    requested_scopes=['https://auth.globus.org/scopes/56ceac29-e98a-440a-a594-b41e7a084b62/all'])
auth_token = tokens["petrel_https_server"]['access_token']
headers = {'Authorization': f'Bearer {auth_token}'}


data = {'inputs':[]}

payload = {'url': 'https://e38ee745-6d04-11e5-ba46-22000b92c6ec.e.globus.org/MDF/mdf_connect/prod/data/stantiberiu_raw_images_learning_v1.1/data/Raw Images/XCT_Training/c34_ObjT1_z075_x1y1.png',
                       'headers': headers, 'file_id': "heheheh"}
for i in range(10):
    data['inputs'].append(payload)


# print("Payload is {}".format(payload))

endpoint_uuid = 'a92945a1-2778-4417-8cd1-4957bc35ce66'  # DLHub endpoint for testing
res = fxc.run(data, endpoint_uuid, func_uuid, asynchronous=True)

print("Waiting for result...")
print(res)

import time
while True:
    print(fxc.get_task_status(res))
    time.sleep(2)

