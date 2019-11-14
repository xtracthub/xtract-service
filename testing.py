
from funcx import FuncXClient


fxc = FuncXClient()


client = NativeClient(client_id='7414f0b4-7d05-4bb6-bb00-076fa3f17cf5')
tokens = client.login(
    requested_scopes=['https://auth.globus.org/scopes/56ceac29-e98a-440a-a594-b41e7a084b62/all',
                      'urn:globus:auth:scope:transfer.api.globus.org:all',
                     "https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all"],
    no_local_server=True,
    no_browser=True)

auth_token = tokens["petrel_https_server"]['access_token']
transfer_token = tokens['transfer.api.globus.org']['access_token']
funcx_token = tokens['funcx_service']['access_token']

# TODO: RYAN -- how to nicely read these headers in auth?
headers = {'Authorization': f"Bearer {auth_token}", 'Transfer': transfer_token, 'FuncX': funcx_token}
print(headers)

def save_mdata(event):
    import json
    import os

    main_dir = event['dirname']
    crawl_id = str(event['crawl_id'])
    group_id = str(event['group_id'])
    mdata = event['metadata']

    os.makedirs(f"{main_dir}{crawl_id}", exist_ok=True)

    with open(f"{main_dir}{crawl_id}/{group_id}", 'w') as f:
        json.dump(mdata, f)

    return None


def register_func():
    func_uuid = fxc.register_function(save_mdata,
                                      description="A test function for the matio extractor.")
    print(f"Metadata save function: {func_uuid}")


register_func()


