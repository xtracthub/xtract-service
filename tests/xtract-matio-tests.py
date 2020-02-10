
from container_lib.xtract_matio import MatioExtractor

from fair_research_login import NativeClient


# *** SET VARIABLES HERE TO TEST *** #
funcx_eid = "023a8653-5e9d-42a9-a566-8bee45a123d3"

globus_eid = "e38ee745-6d04-11e5-ba46-22000b92c6ec"

# ********************************** #


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

headers = {'Authorization': f"Bearer {auth_token}", 'Transfer': transfer_token, 'FuncX': funcx_token}
print(f"Headers: {headers}")

mex = MatioExtractor('fake_crawl_id', headers, funcx_eid, globus_eid, None)
