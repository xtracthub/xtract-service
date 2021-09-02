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
            'funcx_eid': funcx_eid, 
            'globus_eid': globus_eid,
            'home': f'.xtract/{home}',
            'header_auth': headers,
            }
        json.dump(config, f)
        return {'status': 'success'}


@configure_bp.route('/configure_ep/<funcx_eid>', methods=['GET', 'POST'])
def configure_endpoint(funcx_eid):
    """ Configuring the endpoint means ensuring that all credentials on the endpoint
        are updated/refreshed, and that the Globus + funcX eps are online """
    from funcx import FuncXClient
    from globus_sdk import AccessTokenAuthorizer

    if not request.headers:
        return {'status': 200, 'message': 'Endpoint configuration unsuccessful!', 'error': 'missing headers'}

    headers = request.headers
    funcx_eid = headers['funcx_eid']
    globus_eid = headers['globus_eid']
    home = headers['home']
    authorization = headers['authorization']

    fx_auth = AccessTokenAuthorizer(authorization)
    # search_auth = AccessTokenAuthorizer(auths['search'])
    # openid_auth = AccessTokenAuthorizer(auths['openid'])

    fxc = FuncXClient(fx_authorizer=fx_auth)
        # search_authorizer=search_auth,
        # openid_authorizer=openid_auth)
    ep_uuid = funcx_eid
    func_uuid = fxc.register_function(configure_endpoint_function)
    task_id = fxc.run(headers, funcx_eid, globus_eid, home, function_id=func_uuid, endpoint_id=ep_uuid)
    print(fxc.get_result(task_id=task_id))

    return {'status': 200, 'message': 'Endpoint successfully configured!', 'funcx_eid': funcx_eid}