from flask import abort, Blueprint, request
from xtracthub.xcs import XtractConnection

xcs_bp = Blueprint("xcs", __name__)


@xcs_bp.route("/register_container", methods=["POST"])
def register_container():
    funcX_token = request.headers.get("Authorization")
    xconn = XtractConnection(funcX_token)
    try:
        file_obj = request.files["file"]
        file_name = file_obj.filename
        return xconn.register_container(file_name, file_obj)
    except:
        abort(400, "No file")


@xcs_bp.route("/build", methods=["POST"])
def build():
    funcX_token = request.headers.get("Authorization")
    xconn = XtractConnection(funcX_token)
    definition_id = request.json["definition_id"]
    to_format = request.json["to_format"]
    container_name = request.json["container_name"]
    return xconn.build(definition_id, to_format, container_name)


@xcs_bp.route("/get_status", methods=["GET"])
def get_status():
    funcX_token = request.headers.get("Authorization")
    xconn = XtractConnection(funcX_token)
    build_id = request.json["build_id"]
    return xconn.get_status(build_id)


@xcs_bp.route("/repo2docker", methods=["POST"])
def repo2docker():
    funcX_token = request.headers.get("Authorization")
    xconn = XtractConnection(funcX_token)
    try:
        if "git_repo" in request.json:
            return xconn.repo2docker(request.json["container_name"],
                                     git_repo=request.json["git_repo"])
        else:
            file_obj = request.files["file"]
            container_name = file_obj.filename
            return xconn.repo2docker(container_name, file_obj=file_obj)
    except:
        abort(400, "Bad parameters")
