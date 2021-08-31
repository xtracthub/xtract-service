from flask import Blueprint, request


""" Routes that have to do with using configuration """
configure_bp = Blueprint('configure_bp', __name__)


def configure_endpoint_function(headers, funcx_eid, globus_eid=None, home=None):
    import os
    import json
    if not os.path.exists('.xtract/'):
        os.makedirs('.xtract/')
    with open('.xtract/config.json', 'w') as f:
        config = {
            'header_auth': headers,
            'home': '.xtract/',
            'globus_eid': globus_eid,
            'funcx_eid': funcx_eid}
        json.dump(config, f)
        return {'status': 'success'}


@configure_bp.route('/configure_ep/<funcx_eid>', methods=['GET', 'POST'])
def configure_endpoint(funcx_eid):
    """ Configuring the endpoint means ensuring that all credentials on the endpoint
        are updated/refreshed, and that the Globus + funcX eps are online """
    from funcx import FuncXClient
    from globus_sdk import AccessTokenAuthorizer
    import json
    import mdf_toolbox

    if request.json:
        r = json.loads(request.json)
        funcx_eid = r['funcx_eid']
        globus_eid = r['globus_eid']
        home = r['home']
    else:
        globus_eid = None
        home = None

    auths = mdf_toolbox.login(
        services=["data_mdf",
            "petrel",
            "transfer",
            "search",
            "openid",
            "dlhub",
            "https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all",], 
        app_name="Foundry",
        make_clients=True,
        no_browser=False,
        no_local_server=False)
    fx_scope = 'https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all'
    headers = {'Authorization': f"Bearer {auths['petrel']}",
        'Transfer': auths['transfer'], 
        'FuncX': auths[fx_scope], 
        'Petrel': auths['petrel']}

    print(auths)

    fx_auth = AccessTokenAuthorizer(headers['Authorization'])
    # search_auth = AccessTokenAuthorizer(auths['search'])
    # openid_auth = AccessTokenAuthorizer(auths['openid'])

    fxc = FuncXClient(fx_authorizer=fx_auth,)
        # search_authorizer=search_auth,
        # openid_authorizer=openid_auth)
    ep_uuid = funcx_eid
    func_uuid = fxc.register_function(configure_endpoint_function)
    fxc.run(headers, funcx_eid, globus_eid, home, function_id=func_uuid, endpoint_id=ep_uuid)

    return {'status': 200, 'message': 'Endpoint successfully configured!', 'funcx_eid': funcx_eid}