from flask import Blueprint, request
import json
import os
from globus_sdk import auth

from werkzeug.datastructures import Headers


""" Routes that have to do with using configuration """
configure_bp = Blueprint('configure_bp', __name__)


def configure_endpoint_function(header_auth, funcx_eid, globus_eid, home):
    import os
    import json
    if not os.path.exists('.xtract/'):
        os.makedirs('.xtract/')
    with open('.xtract/config.json', 'w') as f:
        config = {
            'funcx_eid': funcx_eid, 
            'globus_eid': globus_eid,
            'home': f'.xtract/{home}',
            'header_auth': header_auth,
            }
        json.dump(config, f)
        return {'status': 'success'}


@configure_bp.route('/configure_ep/<funcx_eid>', methods=['GET', 'POST'])
def configure_ep(funcx_eid):
    """ Configuring the endpoint means ensuring that all credentials on the endpoint
        are updated/refreshed, and that the Globus + funcX eps are online """
    from funcx import FuncXClient
    from globus_sdk import AccessTokenAuthorizer

    if not request.headers:
        return {'status': 200, 'message': 'Endpoint configuration unsuccessful!', 'error': 'missing headers'}

    headers = request.headers
    header_auth = {
        'funcx_eid': headers['funcx_eid'],
        'globus_eid': headers['globus_eid'],
        'home': headers['home'],
        'authorization': headers['authorization']
        }

    fx_auth = AccessTokenAuthorizer(header_auth['authorization'])
    fxc = FuncXClient(fx_authorizer=fx_auth)
    ep_uuid = funcx_eid
    func_uuid = fxc.register_function(configure_endpoint_function)
    task_id = fxc.run(header_auth, 
        header_auth['funcx_eid'], 
        header_auth['globus_eid'], 
        header_auth['home'], 
        function_id=func_uuid, 
        endpoint_id=ep_uuid)

    return {'status': 200, 'message': 'Endpoint successfully configured!', 'funcx_eid': funcx_eid}