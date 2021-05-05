

from flask import Blueprint

""" This file contains all routes that have to do with using Xtract as an action provider. """

automate_bp = Blueprint('crawl_bp', __name__)


@automate_bp.route('/crawl', methods=['POST'])
def crawl():
    print("Testing")
