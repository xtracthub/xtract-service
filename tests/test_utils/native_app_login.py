
import os
import pickle

from fair_research_login import NativeClient
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request


def globus_native_auth_login():
    client = NativeClient(client_id='7414f0b4-7d05-4bb6-bb00-076fa3f17cf5')
    tokens = client.login(
        requested_scopes=['https://auth.globus.org/scopes/56ceac29-e98a-440a-a594-b41e7a084b62/all',
                          'urn:globus:auth:scope:transfer.api.globus.org:all',
                          "https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all",
                          "urn:globus:auth:scope:data.materialsdatafacility.org:all",
                          'email', 'openid', 'urn:globus:auth:scope:search.api.globus.org:all'],
        no_local_server=True,
        no_browser=True, force=True)

    print(tokens)

    auth_token = tokens["petrel_https_server"]['access_token']
    transfer_token = tokens['transfer.api.globus.org']['access_token']
    mdf_token = tokens["data.materialsdatafacility.org"]['access_token']
    funcx_token = tokens['funcx_service']['access_token']
    search_token = tokens['search.api.globus.org']['access_token']
    openid_token = tokens['auth.globus.org']['access_token']

    headers = {'Authorization': f"Bearer {funcx_token}", 'Transfer': transfer_token, 'FuncX': funcx_token,
               'Petrel': mdf_token, 'Search': search_token, 'Openid': openid_token}
    print(f"Headers: {headers}")
    return headers


# Stolen from Google Quickstart docs
# https://developers.google.com/drive/api/v3/quickstart/python
def do_google_login_flow():
    SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly', 'https://www.googleapis.com/auth/drive']

    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)

    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    return creds
