
import os
from globus_sdk import ConfidentialAppAuthClient


def get_uid_from_token(auth_token):
    # Step 1: Get Auth Client with Secrets.
    client_id = os.getenv("GLOBUS_FUNCX_CLIENT")
    secret = os.getenv("GLOBUS_FUNCX_SECRET")

    # Step 2: Transform token and introspect it.
    conf_app_client = ConfidentialAppAuthClient(client_id, secret)
    token = str.replace(str(auth_token), 'Bearer ', '')

    auth_detail = conf_app_client.oauth2_token_introspect(token)

    try:
        uid = auth_detail['username']
    except KeyError as e:
        raise ValueError(str(e))
    return uid
