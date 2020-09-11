
from fair_research_login import NativeClient
import requests

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
headers = {'Authorization': f"Bearer {auth_token}"}


file_url = "https://e38ee745-6d04-11e5-ba46-22000b92c6ec.e.globus.org/MDF/mdf_connect/prod/data/foo_v1/Ag2Al.hP2/INCAR"

print(f"File URL: {file_url}")
print(f"Headers: {headers}")

payload = {
    'url': file_url,
    'headers': headers
}

file_stream = requests.get(url=file_url, headers=headers)
print(file_stream.content)