
from flask import Flask

# Import Blueprints
from routes import crawl_bp, extract_bp

import json

from funcx import FuncXClient

application = Flask(__name__)

# Register Blueprints for crawls and extractions.
application.register_blueprint(crawl_bp.crawl_bp)
application.register_blueprint(extract_bp.extract_bp)

# TODO: Step 1: make a 'configure' blueprint that handles logic that has to do with configuring things.
# TODO: Step 2: have 'configure' route that does the task described in the doc. AND inputs all of the information from the doc.
# TODO: Step 3: so you'll want to do Globus Auth to get the auth headers, both for funcX and for the Globus services.

@application.route('/', methods=['POST', 'GET'])
def xtract_default():
    """ Return the default information as part of the request. """
    return "FUNCTIONAL"


# @application.route('/configure_ep', methods=['POST'])
# def config_ep():
#     r = json.loads(request.json)
#     fx_ep = r['fx_ep']
#
#     fxc = FuncXClient()
#
#     return {'status': 200, 'message': 'Endpoint successfully configured!', 'fx_eid': fx_ep}


if __name__ == '__main__':
    application.run(debug=True, threaded=True, ssl_context="adhoc")
