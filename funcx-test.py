

import funcx
import time
import json
import requests
from container_lib.xtract_matio import matio_test, serialize_fx_inputs
from fair_research_login import NativeClient
from funcx.serialize import FuncXSerializer

fxc = funcx.FuncXClient()
fx_ser = FuncXSerializer()


post_url = 'https://dev.funcx.org/api/v1/submit'
get_url = 'https://dev.funcx.org/api/v1/{}/status'

#@ headers = {'Authorization': 'Bearer AgPvrzO1kglgjp9dkrgvYK6n5YwDxqm5Dqyp5oQ3P6mbb94zGrFmCV51GWdMko81y5pNNlWXqYzy7OSq1d8Xnt1gv0', 'FuncX': 'AgPvrzO1kglgjp9dkrgvYK6n5YwDxqm5Dqyp5oQ3P6mbb94zGrFmCV51GWdMko81y5pNNlWXqYzy7OSq1d8Xnt1gv0'}


func_uuid = fxc.register_function(matio_test,
                                  description="A sum function")


print(f"Function UUID: {func_uuid}")

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

headers = {'Authorization': f"Bearer {funcx_token}", 'Transfer': transfer_token, 'FuncX': funcx_token}
print(f"Headers: {headers}")


old_mdata = {"files": ["/MDF/mdf_connect/prod/data/h2o_13_v1-1/split_xyz_files/watergrid_60_HOH_180__0.7_rOH_1.8_vario_PBE0_AV5Z_delta_PS_data/watergrid_PBE0_record-1237.xyz"]}

data = {"inputs": []}
data["transfer_token"] = transfer_token
data["source_endpoint"] = 'e38ee745-6d04-11e5-ba46-22000b92c6ec'
data["dest_endpoint"] = '5113667a-10b4-11ea-8a67-0e35e66293c2'

for f_obj in old_mdata["files"]:
    payload = {
        # TODO: Un-hardcode.
        'url': f'https://e38ee745-6d04-11e5-ba46-22000b92c6ec.e.globus.org{f_obj}',
        'headers': headers, 'file_id': 'googoogoo'}
    data["inputs"].append(payload)


res = requests.post(url=post_url,
                    headers=headers,
                    json={'endpoint': '450746de-f876-4581-b97e-8bbefd16bf3a',
                          'func': func_uuid,
                          'payload': serialize_fx_inputs(
                              event=data)
                          }
                    )

task_uuid = json.loads(res.content)['task_uuid']

while True:
    status_thing = requests.get(get_url.format(task_uuid), headers=headers).json()

    print(status_thing)
    if 'result' in status_thing:
        print(f"Result: {fx_ser.deserialize(status_thing['result'])}")
        break
    elif 'exception' in status_thing:
        print(f"Exception: {fx_ser.deserialize(status_thing['exception'])}")
        break

    time.sleep(2)
