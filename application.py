
import logging
from flask import Flask, request

# Import Blueprints
from routes import crawl_bp, extract_bp, configure_bp


application = Flask(__name__)
application.logger.setLevel(logging.DEBUG)

# TODO: this causes repeat messages (one too verbose, and this one).
# logging.basicConfig(format='%(message)s')  # remove timestamp, already appended by EB.


# Register Blueprints for crawls and extractions.
application.register_blueprint(crawl_bp.crawl_bp)
application.register_blueprint(extract_bp.extract_bp)
application.register_blueprint(configure_bp.configure_bp)


@application.route('/', methods=['POST', 'GET'])
def xtract_default():
    """ Return the default information as part of the request. """
    return {'status': "FUNCTIONAL", 'code': 200}


@application.route('/test', methods=['POST', 'GET'])
def xtract_default2():
    r1 = request.data
    r2 = request.json
    print(r1)
    print(r2)
    return {'status': "FUNCTIONAL", 'code': 200, 'success': True, 'message': 'testmessage123'}


if __name__ == '__main__':
    application.logger.info("[application.py] Starting Xtract service...")
    application.run(debug=True, threaded=True)# , ssl_context="adhoc")
