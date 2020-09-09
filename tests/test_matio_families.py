
from extractors.xtract_matio import MatioExtractor
from xtract_sdk.packagers.family import Family
from xtract_sdk.packagers.family_batch import FamilyBatch
from fair_research_login import NativeClient
from queue import Queue

img_extractor = MatioExtractor()

img_funcid = img_extractor.register_function()


fam_1 = Family()
# fam_2 = Family()

# NCSA test
# base_url = "https://data.materialsdatafacility.org"
# base_path = "/thurston_selfassembled_peptide_spectra_v1.1/DFT/MoleculeConfigs/di_30_-10.xyz/"

# Petrel test
base_url = 'https://e38ee745-6d04-11e5-ba46-22000b92c6ec.e.globus.org'
base_path = "/MDF/mdf_connect/prod/data/foo_v1/Ag2Al.hP2/"

# TODO: Need 2 level xtract-matio.

# fam_batch.add_family(fam_2)

# TODO: Get creds for both Globus and Google here.
# Get the Headers....

print("Starting NativeClient processing...")
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

full_headers = {'Authorization': f"Bearer {funcx_token}", 'Transfer': transfer_token, 'FuncX': funcx_token, 'Petrel': auth_token}
#print(f"Headers: {headers}")
file_headers = {'Authorization': f"Bearer {auth_token}", 'Transfer': transfer_token, 'FuncX': funcx_token, 'Petrel': auth_token}


fam_1.add_group(files=[{"path": f"{base_path}INCAR", "metadata": {}, "base_url": base_url}, {"path": f"{base_path}OUTCAR", "metadata": {}, "base_url": base_url}, {"path": f"{base_path}POSCAR", "metadata": {}, "base_url": base_url}], parser='dft')
fam_1.headers = file_headers

fam_batch = FamilyBatch()
fam_batch.add_family(fam_1)

task_dict = {"active": Queue(), "pending": Queue(), "results": [], "failed": Queue()}

print("submitting task")
task_id = img_extractor.remote_extract_solo(event={'family_batch': fam_batch},
                                            fx_eid="68bade94-bf58-4a7a-bfeb-9c6a61fa5443",
                                            headers=full_headers)

print(task_id)
task_dict["active"].put(task_id)

print(f"Task ID: {task_id}")


import time
import requests
from funcx.serialize import FuncXSerializer
fx_ser = FuncXSerializer()
while True:

    get_url = 'https://api.funcx.org/v1/{}/status'

    if task_dict["active"].empty():
        print("Active task queue empty... sleeping... ")
        time.sleep(0.5)
        break  # This should jump out to main_loop

    cur_tid = task_dict["active"].get()
    print(cur_tid)
    status_thing = requests.get(get_url.format(cur_tid), headers=full_headers).json()

    if 'result' in status_thing:
        result = fx_ser.deserialize(status_thing['result'])

        print(f"Result: {result}")

        for family in result["family_batch"].families:
            print(family)
            for gid in family.groups:
                print(family.groups[gid].metadata)

    elif 'exception' in status_thing:
        exception = fx_ser.deserialize(status_thing['exception'])
        print(f"Exception: {exception.reraise()}")
    else:
        task_dict["active"].put(cur_tid)
