
from flask import Flask

# Import Blueprints
from routes import crawl_bp, extract_bp


application = Flask(__name__)

# Register Blueprints for crawls and extractions.
application.register_blueprint(crawl_bp.crawl_bp)
application.register_blueprint(extract_bp.extract_bp)


@application.route('/', methods=['POST', 'GET'])
def xtract_default():
    """ Return the default information as part of the request. """
    return "FUNCTIONAL"


if __name__ == '__main__':
    application.run(debug=True, threaded=True, ssl_context="adhoc")
