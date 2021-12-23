
import logging
from flask import Flask

# Import Blueprints
from routes import crawl_bp, extract_bp, configure_bp


application = Flask(__name__)
application.logger.setLevel(logging.DEBUG)
application.logger.basicConfig(format='%(message)s')  # remove timestamp, already appended by EB.

# Register Blueprints for crawls and extractions.
application.register_blueprint(crawl_bp.crawl_bp)
application.register_blueprint(extract_bp.extract_bp)
application.register_blueprint(configure_bp.configure_bp)


@application.route('/', methods=['POST', 'GET'])
def xtract_default():
    """ Return the default information as part of the request. """
    # TODO: helpful message here that Vas can check.
    return {'status': "FUNCTIONAL", 'code': 200}


if __name__ == '__main__':
    application.logger.info("[application.py] Starting Xtract service...")
    application.run(debug=True, threaded=True, ssl_context="adhoc")
