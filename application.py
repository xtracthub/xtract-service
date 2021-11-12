
from flask import Flask

# Import Blueprints
from routes import crawl_bp, extract_bp, configure_bp


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


if __name__ == '__main__':
    application.run(debug=True, threaded=True, ssl_context="adhoc")
