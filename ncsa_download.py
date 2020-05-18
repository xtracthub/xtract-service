
from fair_research_login import NativeClient
import requests

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
funcx_token = tokens['funcx_service']['access_token']
mdf_token = tokens["data.materialsdatafacility.org"]['access_token']


headers = {'Authorization': f"Bearer {mdf_token}", 'Transfer': transfer_token, 'FuncX': funcx_token, 'Petrel': auth_token}
print(f"Headers: {headers}")

# file_path = "https://82f1b5c6-6e9b-11e5-ba47-22000b92c6ec.e.globus.org/mdf_open/tsopanidis_wha_amaps_v1.1/WHA Dataset/Cropped Images/A_102_1.png"
file_path = "https://data.materialsdatafacility.org/mdf_open/tsopanidis_wha_amaps_v1.1/WHA Dataset/Cropped Images/A_102_1.png"

req = requests.get(file_path, headers=headers)

print(req.content)