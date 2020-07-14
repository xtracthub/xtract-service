
from extractors.xtract_tabular import TabularExtractor
from xtract_sdk.packagers.family import Family
from xtract_sdk.packagers.family_batch import FamilyBatch
import pickle
from fair_research_login import NativeClient
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import os
from queue import Queue

img_extractor = TabularExtractor()

img_funcid = img_extractor.register_function()

# Image collection
tab_1 = "1sGTskjKmA5n-vE2WDovil98RcMK-mJNMJBmqbvc5PbQ"  # SC19 SCC roster
tab_2 = "0B5nDSpS9a_3kUFdiTXRFdS12QUk"  # CDIAC data

fam_1 = Family()
fam_2 = Family()
fam_1.add_group(files=[{"path": tab_1, "is_gdoc": True, "mimeType": "text/csv", "metadata": {}}], parser='tabular')
fam_2.add_group(files=[{"path": tab_2, "is_gdoc": False, "mimeType": "text/csv", "metadata": {}}], parser='tabular')

fam_batch = FamilyBatch()
fam_batch.add_family(fam_1)
fam_batch.add_family(fam_2)

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


task_dict = {"active": Queue(), "pending": Queue(), "results": [], "failed": Queue()}
task_id = img_extractor.remote_extract_solo(event={'family_batch': fam_batch, 'creds': do_login_flow()}, fx_eid="82ceed9f-dce1-4dd1-9c45-6768cf202be8", headers=headers)

task_dict["active"].put(task_id)

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

        print(result['family_batch'].families[0].metadata)
        print(result['family_batch'].families[1].metadata)

        print(f"Result: {result}")

    elif 'exception' in status_thing:
        print(f"Exception: {fx_ser.deserialize(status_thing['exception'])}")
    else:
        task_dict["active"].put(cur_tid)
