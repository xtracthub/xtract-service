
import time
from fair_research_login import NativeClient
from funcx.serialize import FuncXSerializer
from queue import Queue
from functools import partial

fx_ser = FuncXSerializer()
from mdf_toolbox import dict_merge
import numpy as np

# Standard Py Imports
import os
import json
import pickle
import os.path
import requests

class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)


# Google Imports
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
_merge_func = partial(dict_merge, append_lists=True)

post_url = 'https://funcx.org/api/v1/submit'
get_url = 'https://funcx.org/api/v1/{}/status'
# globus_ep = "1adf6602-3e50-11ea-b965-0e16720bb42f"
globus_ep = "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec"

fx_ep = "82ceed9f-dce1-4dd1-9c45-6768cf202be8"
n_tasks = 5000

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
                '../tests/credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    return creds, None  # Returning None because Tyler can't figure out how he wants to structure this yet.

# THIS should force-open a Google Auth window in your local browser. If not, you can manually copy-paste it.
auth_creds = do_login_flow()

pending_mdata_holder = dict()

with open('will_crawl.mdata', 'r') as f:
    crawl_mdata = json.load(f)

task_dict = {"active": Queue(), "pending": Queue(), "results": [], "failed": Queue()}

from extractors.xtract_images import ImageExtractor

imgx = ImageExtractor()
imgx.register_function()
image_counter = 0

from xtract_sdk.packagers.family import Family
from xtract_sdk.packagers.family_batch import FamilyBatch
for file_obj in crawl_mdata:

    # print(crawl_mdata)
    # print(file_obj)

    file_id = file_obj["id"]
    extractor = file_obj["extractor"]
    is_gdoc = file_obj["is_gdoc"]

    if extractor != "images":
        print(extractor)
        continue

    image_counter += 1
    print(f"Number of images for processing: {image_counter}")
    print(file_obj)
    # if image_counter > 100:
    #     break

    fam = Family(download_type="gdrive", headers=headers)
    fam.add_group(files=[{'path': file_id, 'metadata': {}, 'is_gdoc': is_gdoc, 'mimeType': "image/jpg"}], parser="image")
    fam_batch = FamilyBatch()
    fam_batch.add_family(fam)


    t_launch_times = {}

    task_id = imgx.remote_extract_solo(event={"family_batch": fam_batch, "creds": auth_creds[0]}, fx_eid=fx_ep, headers=headers)
    print(f"Task ID: {task_id}")
    task_dict["active"].put(task_id)
    pending_mdata_holder[task_id] = file_obj

timeout = 120
failed_counter = 0
failed = 0

print("ABOUT TO ENTER THE WHILE LOOP")
while True:

    if task_dict["active"].empty():
        print("Active task queue empty... sleeping... ")
        time.sleep(0.5)
        break  # This should jump out to main_loop

    cur_tid = task_dict["active"].get()
    print(cur_tid)

    status_thing = requests.get(get_url.format(cur_tid), headers=headers).json()
    # print(status_thing.json())

    if 'result' in status_thing:
        result = fx_ser.deserialize(status_thing['result'])

        try:
            the_metadata = result['family_batch'].families[0].metadata
            print(f"Resultant metadata: {the_metadata}")

            pending_mdata_holder[cur_tid]["extractor_metadata"] = dict()
            pending_mdata_holder[cur_tid]["extractor_metadata"]["images"] = the_metadata

            print(pending_mdata_holder[cur_tid])
        except:
            failed += 1
            print(f"Num. failed: {failed}")

    elif 'exception' in status_thing:
        print(f"Exception: {fx_ser.deserialize(status_thing['exception'])}")
        # break
    else:
        task_dict["active"].put(cur_tid)


vals = list(pending_mdata_holder.values())

with open("will_images.mdata", "w") as f:
    json.dump(vals, f, cls=NumpyEncoder)