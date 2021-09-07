from flask import Blueprint, request
import json
import os
import funcx
import funcx.sdk.utils.throttling
from globus_sdk import auth
from werkzeug.datastructures import Headers


""" Routes that have to do with using configuration """
configure_bp = Blueprint('configure_bp', __name__)
timeout = 10
max_requests = 10


def configure_endpoint_function(header_auth):
    import os
    import json

    try:
        data = json.loads(header_auth)
    except UnicodeDecodeError as err_msg:
        print('Unable to decode header.')
        return {'status': 'failure'}

    # TODO: Verify that the user provides a valid path,
    # possibly via regex/checking if the path is available
    # and that permissions allow us to write to that location
    if not os.path.exists(data['xtract_path']):
        os.makedirs(data['xtract_path'])
    with open(os.path.join(data['xtract_path'], 'config.json'), 'w') as f:
        config = {
            'ep_name': data['ep_name'],
            'funcx_eid': data['funcx_eid'],
            'globus_eid': data['globus_eid'],
            'xtract_path': data['xtract_path'],
            'local-metadata-path': data['local_metadata_path'],
            'headers': data['headers']
            }
        
        json.dump(config, f)
        return {'status': 'success'}


@configure_bp.route('/configure_ep/<funcx_eid>', methods=['GET', 'POST'])
def configure_ep(funcx_eid):
    """ Configuring the endpoint means ensuring that all credentials on the endpoint
        are updated/refreshed, and that the Globus + funcX eps are online """
    import time
    from funcx import FuncXClient
    from globus_sdk import AccessTokenAuthorizer

    if not request.json:
        return {'status': 200, 'message': 'Endpoint configuration unsuccessful!', 'error': 'missing headers'}

    r = request.json

    payload = r
    header_auth = payload['headers']

    fx_auth = AccessTokenAuthorizer(header_auth['authorization'])
    fxc = FuncXClient(fx_authorizer=fx_auth)
    ep_uuid = funcx_eid
    func_uuid = fxc.register_function(configure_endpoint_function)
    task_id = fxc.run(json.dumps(payload),
        function_id=func_uuid,
        endpoint_id=ep_uuid)
    
    start_time = time.time()
    num_requests = 0


    # TODO: Verify that an endpoint is online before configuring it
    # This can be done by calling for the endpoint status function
    # endpoint_status = fxc.get_endpoint_status(funcx_eid)
    # endpoint_status = json.dumps(endpoint_status, indent=4)
    # print(endpoint_status)
    
    while True and num_requests < max_requests:
        result = fxc.get_batch_result(task_id_list=[task_id])
        num_requests += 1
        print(result)
        if 'exception' in result[task_id]:
            result[task_id]['exception'].reraise()

        if result[task_id]['status'] == 'success':
            print("Successfully returned test function. Breaking!")
            break

        elif result[task_id]['status'] == 'FAILED':
            return {'config_status': 'FAILED', 'fx_eid': funcx_eid, 'msg': 'funcX internal failure'}

        else:
            if time.time() - start_time > timeout:
                return {'config_status': "FAILED", 'fx_id': funcx_eid, 'msg': 'funcX return timeout'}
            else:
                time.sleep(2)

    return {'status': 200, 'message': 'Endpoint successfully configured!', 'funcx_eid': funcx_eid}