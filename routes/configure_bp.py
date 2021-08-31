import pickle
import json
from flask import Blueprint

""" Routes that have to do with using configuration """
configure_bp = Blueprint('configure_bp', __name__)
print(configure_bp.root_path)


@configure_bp.route('/configure_endpoint/<globus_eid>/<funcx_eid>/<home>', methods=['GET', 'POST', 'PUT'])
def configure_endpoint(globus_eid, funcx_eid, home=None):
    from funcx import FuncXClient

    # Get these how?
    header = {
        'Authorization': 'authorization token',
        'Transfer': 'transfer token',
        'FuncX': 'fx_scope',
        'Petrel': 'petrel token'}


    def configure_endpoint_fn(header):
        import os
        if not os.path.exists('.xtract/'):
            os.makedirs('.xtract/')
        with open('.xtract/config.json', 'w') as f:
            config = {
                'header_auth': header,
                'home': '.xtract/',
                'globus_eid': globus_eid,
                'funcx_eid': funcx_eid}
            json.dump(config, f)
            return {'status': 'success'}

    fxc = FuncXClient()
    fxc.register_endpoint(funcx_eid)
    fxc.register_function(configure_endpoint_fn(header))
    fxc.run()

    return