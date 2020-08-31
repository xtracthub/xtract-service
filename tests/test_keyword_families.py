
from extractors.xtract_keyword import KeywordExtractor
from xtract_sdk.packagers.family import Family
from xtract_sdk.packagers.family_batch import FamilyBatch
import pickle
from fair_research_login import NativeClient
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import os
from queue import Queue
from funcx import FuncXClient
from extractors.utils.batch_utils import remote_extract_batch

fxc = FuncXClient()
fx_batch = fxc.create_batch()

img_extractor = KeywordExtractor()

img_funcid = img_extractor.register_function()

# Image collection
tab_1 = "11s1EWKAdqIgaEC0ksSKunrQq_WgSKyZH6l1yig-lFgA"  # Algorithms study guide gdoc
tab_2 = "1Yr88RgyYWWyuZzemtoI8nSFQLHh_XFb9"  # PDF resume (Lang)

fam_1 = Family()
fam_2 = Family()
fam_1.add_group(files=[{"path": tab_1, "is_gdoc": True, "mimeType": "text/plain", "metadata": {}}], parser='text')
fam_2.add_group(files=[{"path": tab_2, "is_gdoc": False, "mimeType": "application/pdf", "metadata": {}}], parser='text')


# Full version of file throwing error
# fam_2.add_group = {'files': [{'path': '1eTSkC46GUN4z-6mhFTJHsE9Z9EpmllMpCzdm-LLpuCQ', 'metadata': {'id': '1eTSkC46GUN4z-6mhFTJHsE9Z9EpmllMpCzdm-LLpuCQ', 'name': 'DCSL Info Session 1/24', 'mimeType': 'application/vnd.google-apps.presentation', 'webViewLink': 'https://docs.google.com/presentation/d/1eTSkC46GUN4z-6mhFTJHsE9Z9EpmllMpCzdm-LLpuCQ/edit?usp=drivesdk', 'shared': True, 'size': 0, 'is_gdoc': True, 'user_is_owner': False, 'shared_user_ids': ['03530326841615676301', '03711339970616793712', '09490290160418701680', '14453663582181058099', '09932392942899452680', '02136978862942460576', '12120473023176343867', '08122454148025534369'], 'parent': '1Qx9X7LzkMLdrEZNKZDTgQvjaZgxdqQBx', 'last_modified': 1591778133.558, 'extension': '', 'extractor': 'text'}, 'is_gdoc': True, 'mimeType': None, 'file_id': '2ed94934-2e41-49eb-a33f-ebe6698ee5ca'}, {'path': '1MEgpv9BO3yPbX0MPFy617OxM_SeIcIHp03DL3suQ7ZM', 'metadata': {'id': '1MEgpv9BO3yPbX0MPFy617OxM_SeIcIHp03DL3suQ7ZM', 'name': 'Learning Agreement | CDAC Summer Lab', 'mimeType': 'application/vnd.google-apps.document', 'webViewLink': 'https://docs.google.com/document/d/1MEgpv9BO3yPbX0MPFy617OxM_SeIcIHp03DL3suQ7ZM/edit?usp=drivesdk', 'shared': True, 'size': 0, 'is_gdoc': True, 'user_is_owner': False, 'shared_user_ids': ['03530326841615676301', '14453663582181058099', '09932392942899452680', '02136978862942460576', '12120473023176343867', '08122454148025534369'], 'parent': '1hVe6F5v_YduMMycSYKAR2BD7aEVDek0P', 'last_modified': 1591766119.419, 'extension': '', 'extractor': 'text'}, 'is_gdoc': True, 'mimeType': 'text/plain', 'file_id': '5d04e8a3-22eb-4d81-a4d5-60b84c327155'}]}
fam_2.add_group = {'files': [{'path': '1eTSkC46GUN4z-6mhFTJHsE9Z9EpmllMpCzdm-LLpuCQ',
                              'metadata': {},
                              'is_gdoc': True,
                              'mimeType': None,
                              'file_id': '2ed94934-2e41-49eb-a33f-ebe6698ee5ca'},
                             {'path': '1MEgpv9BO3yPbX0MPFy617OxM_SeIcIHp03DL3suQ7ZM',
                              'metadata': {},
                              'is_gdoc': True, 'mimeType': 'text/plain',
                              'file_id': '5d04e8a3-22eb-4d81-a4d5-60b84c327155'}]}


fam_batch = FamilyBatch()
fam_batch.add_family(fam_1)
fam_batch.add_family(fam_2)  # TODO: bring this back.

# TODO: Get creds for both Globus and Google here.
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

auth_token = tokens["petrel_https_server"]['access_token']
transfer_token = tokens['transfer.api.globus.org']['access_token']
mdf_token = tokens["data.materialsdatafacility.org"]['access_token']
funcx_token = tokens['funcx_service']['access_token']

headers = {'Authorization': f"Bearer {funcx_token}", 'Transfer': transfer_token, 'FuncX': funcx_token, 'Petrel': mdf_token}
print(f"Headers: {headers}")

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

    return creds


print(f"Function ID: {img_funcid}")

task_dict = {"active": Queue(), "pending": Queue(), "results": [], "failed": Queue()}
# task_id = img_extractor.remote_extract_solo(event={'family_batch': fam_batch, 'creds': do_login_flow()}, fx_eid="82ceed9f-dce1-4dd1-9c45-6768cf202be8", headers=headers)
task_ids = remote_extract_batch([{"event": {'family_batch': fam_batch, 'creds': do_login_flow()}, "func_id": img_funcid}], ep_id="82ceed9f-dce1-4dd1-9c45-6768cf202be8")

for task_id in task_ids:
    task_dict["active"].put(task_id)

print(task_dict["active"].qsize())
#task_dict["active"].put(task_id)

import time
import requests
from funcx.serialize import FuncXSerializer
fx_ser = FuncXSerializer()
while True:

    get_url = 'https://funcx.org/api/v1/{}/status'

    if task_dict["active"].empty():
        print("Active task queue empty... sleeping... ")
        time.sleep(0.5)
        break  # This should jump out to main_loop

    cur_tid = task_dict["active"].get()
    print(cur_tid)
    status_thing = requests.get(get_url.format(cur_tid), headers=headers).json()

    if 'result' in status_thing:
        result = fx_ser.deserialize(status_thing['result'])

        print(result)

        print(result['family_batch'].families[0].metadata)
        print(result['family_batch'].families[1].metadata)

        print(f"Result: {result}")

    elif 'exception' in status_thing:
        print(f"Exception: {fx_ser.deserialize(status_thing['exception'])}")
    else:
        task_dict["active"].put(cur_tid)
