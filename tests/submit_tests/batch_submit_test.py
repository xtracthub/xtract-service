from extractors.utils.batch_utils import remote_extract_batch, remote_poll_batch
from funcx import FuncXClient
from utils.fx_utils import invoke_solo_function
import requests
import json
from funcx.serialize import FuncXSerializer

from fair_research_login import NativeClient

post_url = 'https://api.funcx.org/v1/submit'


def serialize_fx_inputs(*args, **kwargs):
    from funcx.serialize import FuncXSerializer
    fx_serializer = FuncXSerializer()
    ser_args = fx_serializer.serialize(args)
    ser_kwargs = fx_serializer.serialize(kwargs)
    payload = fx_serializer.pack_buffers([ser_args, ser_kwargs])
    return payload


# TODO: Put this into a 'test' function so I don't copy and paste into each function.
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


def hello_world(event):
    return "Hello World!"


fxc = FuncXClient()

func_uuid = fxc.register_function(hello_world)
print(func_uuid)
event = None

endpoint = '68bade94-bf58-4a7a-bfeb-9c6a61fa5443'


items_to_batch = [{"func_id": func_uuid, "event": {}}, {"func_id": func_uuid, "event": {}}]
x = remote_extract_batch(items_to_batch, endpoint, headers=headers)

fx_ser = FuncXSerializer()

import time
while True:
    a = remote_poll_batch(x, headers)
    print(f"The returned: {a}")

    for tid in a:
        if "exception"  in a[tid]:
            exception = a[tid]["exception"]
            print(f"The serialized exception: {exception}")
            d_exception = fx_ser.deserialize(exception)
            print(f"The deserialized exception {d_exception}")

            print("RERAISING EXCEPTION!")

            d_exception.reraise()
                #print(fx_ser.deserialize(a[tid]["exception"]))
        else:
            print(a[tid])
    time.sleep(5.1)

