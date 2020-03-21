
import funcx
import time
from container_lib.xtract_matio import matio_test, fatio_test
from funcx.serialize import FuncXSerializer
from fair_research_login import NativeClient


fxc = funcx.FuncXClient()
fx_ser = FuncXSerializer()

post_url = 'https://dev.funcx.org/api/v1/submit'
get_url = 'https://dev.funcx.org/api/v1/{}/status'
globus_ep = "1adf6602-3e50-11ea-b965-0e16720bb42f"

fx_ep = "82ceed9f-dce1-4dd1-9c45-6768cf202be8"

container_id = fxc.register_container(location='039706667969.dkr.ecr.us-east-1.amazonaws.com/xtract-keyword:latest',
                                      container_type='docker',
                                      name='kube-matio5',
                                      description='I don\'t think so!')
fn_id = fxc.register_function(fatio_test,
                              container_uuid=container_id,
                              description="A sum function")

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

f_obj = "/MDF/mdf_connect/prod/data/h2o_13_v1-1/split_xyz_files/watergrid_60_HOH_180__0.7_rOH_1.8_vario_PBE0_AV5Z_delta_PS_data/watergrid_PBE0_record-1237.xyz"

data = {"inputs": []}
data["transfer_token"] = transfer_token
data["source_endpoint"] = 'e38ee745-6d04-11e5-ba46-22000b92c6ec'
# data["source_endpoint"] = '1c115272-a3f2-11e9-b594-0e56e8fd6d5a'
data["dest_endpoint"] = globus_ep


num_tasks = 2500
for i in range(num_tasks):

    payload = {
            # TODO: Un-hardcode.
            'url': f'https://e38ee745-6d04-11e5-ba46-22000b92c6ec.e.globus.org{f_obj}',
            'headers': headers, 'file_id': 'hi'}
    data["inputs"].append(payload)

# TODO: my container on this.
# payload = [1]
res2 = fxc.map_run(data, endpoint_id=fx_ep, function_id=fn_id)
print(res2)


# ### Uncomment for the 'successful tasks' counter ####
# while True:
#     a = fxc.get_batch_status(res2)
#     print(a)
#     print(len(a))
#     time.sleep(5)
# #####################################################


pending_tasks = set(res2)

while True:
    a = fxc.get_batch_status(res2)
    for t in a:
        if t in pending_tasks:
            pending_tasks.remove(t)
    print(pending_tasks)
    print(len(pending_tasks))

    time.sleep(2)
