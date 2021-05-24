
import json
import requests
from inspect import getsource
from funcx.serialize import FuncXSerializer
from utils.routes import fx_register_func_url
from tests.test_utils.native_app_login import globus_native_auth_login


fx_ser = FuncXSerializer()

# def hello_world():
#     return 'hello-world!'


#headers = globus_native_auth_login()
#print(headers)


def register_function(function, headers):
    """ Stolen from the funcX client/SDK code...

        function : Python Function
            The function to be registered for remote execution
        function_name : str
            The entry point (function name) of the function. Default: None
        container_uuid : str
            Container UUID from registration with funcX
        description : str
            Description of the file
        public : bool
            Whether or not the function is publicly accessible. Default = False
        group : str
            A globus group uuid to share this function with
        searchable : bool
            If true, the function will be indexed into globus search with the appropriate permissions"""

    source_code = ""
    try:
        source_code = getsource(function)
    except OSError:
        print("Failed to find source code during function registration.")

    serialized_fn = fx_ser.serialize(function)
    packed_code = fx_ser.pack_buffers([serialized_fn])

    function_name = None
    description = "tskiz-function"
    public = False
    group = None
    searchable = True

    req = requests.post(fx_register_func_url, json={"function_name": function.__name__,
                                                    "function_code": packed_code,
                                                    "function_source": source_code,
                                                    "container_uuid": None,
                                                    "entry_point": function_name if function_name else function.__name__,
                                                    "description": description,
                                                    "public": public,
                                                    "group": group,
                                                    "searchable": searchable},
                        headers=headers)

    func_uuid = json.loads(req.content)['function_uuid']
    return func_uuid


#register_function(hello_world, headers=headers)
