
import os
import json

from flask import Blueprint, request, current_app

from utils.pg_utils import pg_conn
from scheddy.scheduler import get_fx_client
from utils.auth.globus_auth import get_uid_from_token
from tests.extractors_at_compute_facilities.xtract_jetstream.test_all_extractors import \
    register_functions, get_execution_information


""" Routes that have to do with using configuration """
configure_bp = Blueprint('configure_bp', __name__)


@configure_bp.route('/config_containers', methods=['POST'])
def config_containers():
    print("In config_containers")
    """ Returns the status of a crawl. """
    r = request.json

    fx_eid = r["fx_eid"]
    container_path = r["container_path"]
    headers = r['headers']

    print(headers)

    try:
        user = get_uid_from_token(str.replace(str(headers['Authorization']), 'Bearer ', ''))
        current_app.logger.info(f"[configure_bp] Authenticated user: {user}")
        print(f"User: {user}")
    except ValueError as e:
        current_app.logger.error(f"[configure_bp] UNABLE TO AUTHENTICATE USER -- CAUGHT: {e}")
        return {'status': 401, 'message': 'Unable to authenticate with given token'}

    del_query_1 = f"""DELETE FROM fxep_container_lookup WHERE fx_eid='{fx_eid}';"""
    del_query_2 = f"""DELETE FROM extractors WHERE fx_eid='{fx_eid}';"""

    print("Connecting?")
    conn = pg_conn()
    cur = conn.cursor()
    print("Connected...")

    print(del_query_1)
    cur.execute(del_query_1)
    print(del_query_2)
    cur.execute(del_query_2)
    print("Queries done...")

    execution_info = get_execution_information('jetstream')  # TODO: loosen this...

    # print("getting fx client...")
    # headers['Authorization'] = fx_token
    fxc = get_fx_client(headers=headers)

    func_uuids = register_functions(execution_info, fxc=fxc)

    print("more queries...")
    in_query_1 = f"""INSERT INTO fxep_container_lookup (fx_eid, container_dir) VALUES ('{fx_eid}', '{container_path}');"""
    cur.execute(in_query_1)
    from uuid import uuid4

    for item in func_uuids:
        in_query_1 = f"""INSERT INTO extractors (ext_id, ext_name, fx_eid, func_uuid) VALUES ('{uuid4()}', 
        '{item}', '{fx_eid}', '{func_uuids[item]}');"""
        cur.execute(in_query_1)
    conn.commit()
    current_app.logger.info("[configure_bp] Successfully registered containers!")
    return {'status': 200, 'message': 'OK'}
