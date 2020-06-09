
import funcx
import time
import json
import os
import requests
from container_lib.xtract_matio import serialize_fx_inputs
from container_lib.xtract_images import images_extract
from fair_research_login import NativeClient
from funcx.serialize import FuncXSerializer
from queue import Queue
from mdf_matio.validator import MDFValidator
from materials_io.utils.interface import ParseResult
from typing import Iterable, Set, List
from functools import reduce, partial
from mdf_matio.grouping import groupby_file, groupby_directory


from mdf_toolbox import dict_merge


# Standard Py Imports
import os
import json
import pickle
import os.path
import requests

# Google Imports
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
_merge_func = partial(dict_merge, append_lists=True)


fxc = funcx.FuncXClient()
fx_ser = FuncXSerializer()

vald_obj = MDFValidator(schema_branch="master")

post_url = 'https://dev.funcx.org/api/v1/submit'
get_url = 'https://dev.funcx.org/api/v1/{}/status'
# globus_ep = "1adf6602-3e50-11ea-b965-0e16720bb42f"
globus_ep = "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec"

fx_ep = "82ceed9f-dce1-4dd1-9c45-6768cf202be8"
n_tasks = 5000

burst_size = 5

batch_size = 5

container_id = fxc.register_container(location='039706667969.dkr.ecr.us-east-1.amazonaws.com/xtract-image:latest',
                                      container_type='docker',
                                      name='kube-images',
                                      description='I don\'t think so!')
fn_id = fxc.register_function(images_extract,
                              container_uuid=container_id,
                              description="A sum function")


print(f"Function UUID: {fn_id}")

# Get the Headers....
client = NativeClient(client_id='7414f0b4-7d05-4bb6-bb00-076fa3f17cf5')
tokens = client.login(
    requested_scopes=['https://auth.globus.org/scopes/56ceac29-e98a-440a-a594-b41e7a084b62/all',
                      'urn:globus:auth:scope:transfer.api.globus.org:all',
                     "https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all",
                    "urn:globus:auth:scope:data.materialsdatafacility.org:all",
                     'email', 'openid'],
    no_local_server=True,
    no_browser=True)

# print(tokens)
# exit()

auth_token = tokens["petrel_https_server"]['access_token']
transfer_token = tokens['transfer.api.globus.org']['access_token']
mdf_token = tokens["data.materialsdatafacility.org"]['access_token']
funcx_token = tokens['funcx_service']['access_token']

headers = {'Authorization': f"Bearer {funcx_token}", 'Transfer': transfer_token, 'FuncX': funcx_token, 'Petrel': mdf_token}
print(f"Headers: {headers}")

# NCSA file
old_mdata = {"files": ["/mdf_open/kearns_biofilm_rupture_location_v1.1/Biofilm Images/Paper Images/Isofluence Images (79.4)/THY+75mM AA/1.jpg"]}

# TODO: bring back
# data = {"inputs": [], "transfer_token": transfer_token, "source_endpoint": 'e38ee745-6d04-11e5-ba46-22000b92c6ec',
#         "dest_endpoint": globus_ep}
data = {}


SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly', 'https://www.googleapis.com/auth/drive']

# Stolen from Google Quickstart docs
# https://developers.google.com/drive/api/v3/quickstart/python
def do_login_flow():
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)

    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    return creds, None  # Returning None because Tyler can't figure out how he wants to structure this yet.

# THIS should force-open a Google Auth window in your local browser. If not, you can manually copy-paste it.
auth_creds = do_login_flow()

# data["gdrive_pkl"] = pickle.dumps(auth_creds)
# data["file_id"] = "1zAJJy4bFQ2ZANV7W3iv7WdpZWRBp8iEN"

data = {'gdrive_pkl': b'\x80\x03cgoogle.oauth2.credentials\nCredentials\nq\x00)\x81q\x01}q\x02(X\x05\x00\x00\x00tokenq\x03X\xab\x00\x00\x00ya29.a0AfH6SMDkmvH9GI9EL6eKYaQo5OUF4c9Um3oIqgel5IN-Fc9nJqIbke9PTGPc3BuJvWhSuKuWT9NnXZmy9PIN1xMi7rkhCH_gY8bQttlPUEkBJ7HoKi_xPj1nU_pd3qQMnQvyV7oi_CeAaf_LTBrsK22x--WK7wA5cbjFq\x04X\x06\x00\x00\x00expiryq\x05cdatetime\ndatetime\nq\x06C\n\x07\xe4\x06\x08\x152\x1f\x08\x89\x05q\x07\x85q\x08Rq\tX\x0e\x00\x00\x00_refresh_tokenq\nXg\x00\x00\x001//0fYtW4N1qkLF2CgYIARAAGA8SNwF-L9Ir8QaekPkzMwhqN8orPQxeV_ayzLVwFmIHDsW4fmVRU_pj2zJZew1j3dqSY_XywTP_S3gq\x0bX\t\x00\x00\x00_id_tokenq\x0cNX\x07\x00\x00\x00_scopesq\r]q\x0e(X7\x00\x00\x00https://www.googleapis.com/auth/drive.metadata.readonlyq\x0fX%\x00\x00\x00https://www.googleapis.com/auth/driveq\x10eX\n\x00\x00\x00_token_uriq\x11X#\x00\x00\x00https://oauth2.googleapis.com/tokenq\x12X\n\x00\x00\x00_client_idq\x13XH\x00\x00\x00364500245041-r1eebsermd1qp1qo68a3qp09hhpc5dfi.apps.googleusercontent.comq\x14X\x0e\x00\x00\x00_client_secretq\x15X\x18\x00\x00\x00XMGaOYAeBxLz9ZWvsVQCvWtkq\x16X\x11\x00\x00\x00_quota_project_idq\x17NubN\x86q\x18.', 'file_id': '1zAJJy4bFQ2ZANV7W3iv7WdpZWRBp8iEN'}


id_count = 0
group_count = 0
groups_in_family = 1
family = {"family_id": "test-family", "files": {}, "groups": {}}

for i in range(groups_in_family):
    group = {'group_id': group_count, 'files': [], 'parser': 'image'}
    group_count += 1

    # Here we populate the groups.
    for f_obj in old_mdata["files"]:
        # file_url = f'https://e38ee745-6d04-11e5-ba46-22000b92c6ec.e.globus.org{f_obj}'
        file_url = f'https://data.materialsdatafacility.org{f_obj}'
        print(file_url)
        payload = {
            'url': file_url,
            'headers': headers, 'file_id': id_count}
        id_count += 1
        # Here we add file to family list if it doesn't already exist there.
        if file_url not in family['files']:
            family['files'][file_url] = payload
        group['files'].append(file_url)
    family["groups"][group_count-1] = group


# data["inputs"].append(family)

task_dict = {"active": Queue(), "pending": Queue(), "results": [], "failed": Queue()}
t_launch_times = {}

assert(n_tasks % burst_size == 0)

for n in range(int(n_tasks/burst_size)):
    print(f"Processing burst: {n}")

    for i in range(burst_size):
        print(headers)
        print(data)
        # exit()
        res = requests.post(url=post_url,
                            headers=headers,
                            json={'endpoint': fx_ep,
                                  'func': fn_id,
                                  'payload': serialize_fx_inputs(
                                      event=data)
                                  }
                            )

        print(res.content)
        if res.status_code == 200:
            task_uuid = json.loads(res.content)['task_uuid']
            task_dict["active"].put(task_uuid)
            t_launch_times[task_uuid] = time.time()
        print(i)

    # TODO: Add ghetto-retry for pending tasks to catch lost ones.
    # TODO x10000: move this logic
    timeout = 120
    failed_counter = 0
    while True:

        if task_dict["active"].empty():
            print("Active task queue empty... sleeping... ")
            time.sleep(0.5)
            break  # This should jump out to main_loop

        cur_tid = task_dict["active"].get()
        print(cur_tid)
        status_thing = requests.get(get_url.format(cur_tid), headers=headers).json()

        if 'result' in status_thing:
            result = fx_ser.deserialize(status_thing['result'])
            print(f"Result: {result}")

        elif 'exception' in status_thing:
            print(f"Exception: {fx_ser.deserialize(status_thing['exception'])}")
            # break
        else:
            task_dict["active"].put(cur_tid)