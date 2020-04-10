
import funcx
import time
import json
import requests
from container_lib.xtract_matio import serialize_fx_inputs, matio_test, hello_world
from fair_research_login import NativeClient
from funcx.serialize import FuncXSerializer
from queue import Queue


fxc = funcx.FuncXClient()
fx_ser = FuncXSerializer()

post_url = 'https://dev.funcx.org/api/v1/submit'
get_url = 'https://dev.funcx.org/api/v1/{}/status'
globus_ep = "1adf6602-3e50-11ea-b965-0e16720bb42f"

fx_ep = "82ceed9f-dce1-4dd1-9c45-6768cf202be8"

fn_id = "5ce11b57-1808-422b-9511-aeccd5cf3278"

# container_id = fxc.register_container(location='039706667969.dkr.ecr.us-east-1.amazonaws.com/xtract-matio:latest',
#                                       container_type='docker',
#                                       name='kube-matio5',
#                                       description='I don\'t think so!')
# fn_id = fxc.register_function(matio_test,
#                               container_uuid=container_id,
#                               description="A sum function")


print(f"Function UUID: {fn_id}")

# Get the Headers....
client = NativeClient(client_id='7414f0b4-7d05-4bb6-bb00-076fa3f17cf5')
tokens = client.login(
    requested_scopes=['https://auth.globus.org/scopes/56ceac29-e98a-440a-a594-b41e7a084b62/all',
                      'urn:globus:auth:scope:transfer.api.globus.org:all',
                     "https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all",
                     'email', 'openid'],
    no_local_server=True,
    no_browser=True)

auth_token = tokens["petrel_https_server"]['access_token']
transfer_token = tokens['transfer.api.globus.org']['access_token']
funcx_token = tokens['funcx_service']['access_token']

headers = {'Authorization': f"Bearer {funcx_token}", 'Transfer': transfer_token, 'FuncX': funcx_token, 'Petrel': auth_token}
print(f"Headers: {headers}")

# ASE/crystal
old_mdata = {"files": ["/MDF/mdf_connect/prod/data/h2o_13_v1-1/split_xyz_files/watergrid_60_HOH_180__0.7_rOH_1.8_vario_PBE0_AV5Z_delta_PS_data/watergrid_PBE0_record-1237.xyz"]}

# Images
# old_mdata = {"files": ["/MDF/mdf_connect/prod/data/klh_1_v1/exposure1_jpg.jpg/01nov26b.001.002.001.001.jpg"]*batch_size}

# DFT
# old_mdata ={"files": ["/MDF/mdf_connect/prod/data/_test_einstein_9vpflvd_v1.1/INCAR", "/MDF/mdf_connect/prod/data/_test_einstein_9vpflvd_v1.1/OUTCAR", "/MDF/mdf_connect/prod/data/_test_einstein_9vpflvd_v1.1/POSCAR"]}

data = {"inputs": [], "transfer_token": transfer_token, "source_endpoint": 'e38ee745-6d04-11e5-ba46-22000b92c6ec',
        "dest_endpoint": globus_ep}

id_count = 0
group_count = 0

family = {"family_id": "test-family", "files": {}, "groups": {}}


group = {'group_id': 'test-group', 'files': [], 'parser': 'ase'}
group_count += 1

# Here we populate the groups.
for f_obj in old_mdata["files"]:
    file_url = f'https://e38ee745-6d04-11e5-ba46-22000b92c6ec.e.globus.org{f_obj}'
    payload = {
        'url': file_url,
        'headers': headers, 'file_id': id_count}
    id_count += 1
    # Here we add file to family list if it doesn't already exist there.
    if file_url not in family['files']:
        family['files'][file_url] = payload
    group['files'].append(file_url)
family["groups"][group_count-1] = group


data["inputs"].append(family)


task_dict = {"active": Queue(), "pending": Queue(), "results": [], "failed": Queue()}
t_launch_times = {}



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


# Polling for result starts here.
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
        task_dict["results"].append(result)
        print(len(task_dict["results"]))

    elif 'exception' in status_thing:
        print(f"Exception: {fx_ser.deserialize(status_thing['exception'])}")
        # break
    else:
        task_dict["active"].put(cur_tid)

            # Manager: f2ab81f26f40