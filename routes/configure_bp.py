import pickle
import json
from flask import Blueprint


""" Routes that have to do with using configuration """
configure_bp = Blueprint('configure_bp', __name__)
print(configure_bp.root_path)


@configure_bp.route('/configure_funcx/<globus_eid>/<funcx_eid>/<home>', methods=['GET', 'POST', 'PUT'])
def configure_funcx(globus_eid, funcx_eid, home):
    from fair_research_login import NativeClient
    from mdf_toolbox import login
    import os

    client = NativeClient(client_id='7414f0b4-7d05-4bb6-bb00-076fa3f17cf5')
    tokens = client.login(
    requested_scopes=['https://auth.globus.org/scopes/56ceac29-e98a-440a-a594-b41e7a084b62/all',
                      'urn:globus:auth:scope:transfer.api.globus.org:all',
                      'https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all'],
    no_local_server=True,
    no_browser=True,
    force=True,)

    auths = login(services=[
        "data_mdf",
        "search",
        "petrel",
        "transfer",
        "dlhub",
        "https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all",
    ],
    app_name="Foundry",
    make_clients=True,
    no_browser=False,
    no_local_server=False,)

    fx_scope = 'https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all'
    headers = {
        'Authorization': auths['petrel'],
        'Transfer': auths['transfer'],
        'FuncX': auths[fx_scope],
        'Petrel': auths['petrel']}

    print(headers)

    if not os.path.exists('.xtract/'):
        os.makedirs('.xtract/')
    with open('.xtract/config.pickle', 'wb') as f:
        config = {
            'header_auth': headers,
            'home': '.xtract/',
            'globus_eid': globus_eid,
            'funcx_eid': funcx_eid}
        pickle.dump(config, f)
        return {'status': 'success'}