
from flask import Flask, request

# Import Blueprints
from routes import crawl_bp, extract_bp, configure_bp
from utils.pg_utils import pg_conn
from scheddy.scheduler import get_fx_client

application = Flask(__name__)

# Register Blueprints for crawls and extractions.
application.register_blueprint(crawl_bp.crawl_bp)
application.register_blueprint(extract_bp.extract_bp)
application.register_blueprint(configure_bp.configure_bp)


# TODO: LOGGING THROUGHOUT the blueprints
#  https://stackoverflow.com/questions/16994174/in-flask-how-to-access-app-logger-within-blueprint


@application.route('/', methods=['POST', 'GET'])
def xtract_default():
    """ Return the default information as part of the request. """
    # TODO: helpful message here that Vas can check.
    return "FUNCTIONAL"


@application.route('/config_containers', methods=['POST'])
def config_containers():
    """ Returns the status of a crawl. """

    r = request.json

    fx_eid = r["fx_eid"]
    container_path = r["container_path"]
    headers = r['headers']

    del_query_1 = f"""DELETE FROM fxep_container_lookup WHERE fx_eid='{fx_eid}';"""
    del_query_2 = f"""DELETE FROM extractors WHERE fx_eid='{fx_eid}';"""

    conn = pg_conn()
    cur = conn.cursor()

    cur.execute(del_query_1)
    cur.execute(del_query_2)

    from tests.extractors_at_compute_facilities.xtract_jetstream.test_all_extractors \
        import register_functions, get_execution_information

    execution_info = get_execution_information('jetstream')  # TODO: loosen this...

    fxc = get_fx_client(headers=headers)

    func_uuids = register_functions(execution_info, fxc=fxc)

    in_query_1 = f"""INSERT INTO fxep_container_lookup (fx_eid, container_dir) VALUES ('{fx_eid}', '{container_path}');"""
    cur.execute(in_query_1)
    from uuid import uuid4

    for item in func_uuids:
        in_query_1 = f"""INSERT INTO extractors (ext_id, ext_name, fx_eid, func_uuid) VALUES ('{uuid4()}', 
        '{item}', '{fx_eid}', '{func_uuids[item]}');"""
        cur.execute(in_query_1)
    conn.commit()
    return {'status': 'OK'}


if __name__ == '__main__':
    application.run(debug=True, threaded=True, ssl_context="adhoc")
