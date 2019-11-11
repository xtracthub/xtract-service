
from fair_research_login import NativeClient
import requests
import json

from funcx import FuncXClient
from funcx.serialize import FuncXSerializer

fxc = FuncXClient()
fx_ser = FuncXSerializer()

client = NativeClient(client_id='7414f0b4-7d05-4bb6-bb00-076fa3f17cf5')
tokens = client.login(
    requested_scopes=['https://auth.globus.org/scopes/56ceac29-e98a-440a-a594-b41e7a084b62/all',
                      'urn:globus:auth:scope:transfer.api.globus.org:all',
                     "https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all"],
    no_local_server=True,
    no_browser=True)


funcx_token = tokens['funcx_service']['access_token']
headers = {'Authorization': f"Bearer {funcx_token}"}
post_url = 'https://dev.funcx.org/api/v1/submit'


def apple(num1, num2):
    return num1+num2

def serialize_fx_inputs(*args, **kwargs):
    from funcx.serialize import FuncXSerializer
    fx_serializer = FuncXSerializer()
    ser_args = fx_serializer.serialize(args)
    ser_kwargs = fx_serializer.serialize(kwargs)
    payload = fx_serializer.pack_buffers([ser_args, ser_kwargs])
    return payload

func_uuid = fxc.register_function(apple, description="A test function for the matio extractor.")


res = requests.post(url=post_url, headers=headers, json={'endpoint': '068def43-3838-43b7-ae4e-5b13c24424fb', 'func': func_uuid, 'payload': serialize_fx_inputs(num1=2, num2=4)})
cont = json.loads(res.content)

task_id = cont['task_uuid']

import time
while True:
    status_url = f'https://dev.funcx.org/api/v1/{task_id}/status'
    status_thing = requests.get(status_url, headers=headers)
    cont2 = json.loads(status_thing.content)
    print(cont2)

    if "result" in cont2:
        res = fx_ser.deserialize(cont2['result'])
        print(res)

    elif "exception" in cont2:
        res = fx_ser.deserialize(cont2['exception'])
        print(res)

    time.sleep(1)

