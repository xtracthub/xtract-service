
from funcx import FuncXClient
from extractors.xtract_matio import matio_extract
import funcx
import time
import os

# Import all of the sample files
from extractors.xtract_hdf import hdf_extract
from js_families import sample_hdf_1

js_ep_id = "ff24c520-a419-45ba-a96c-5e6091878df0"
base_path = "/home/tskluzac/ext_repos/"

all_containers = [
    # 'xtract-matio/xtract-matio.img',
    # 'xtract-tabular/xtract-tabular.img',
    # 'xtract-keyword/xtract-keyword.img',
    # 'xtract-images/xtract-images.img',
    # 'xtract-jsonxml/xtract-jsonxml.img',
    'xtract-hdf/xtract-hdf.img',
    # 'xtract-netcdf/xtract-netcdf.img'
]


def hello_container(event):
    import os
    return f"Container version: {os.environ['container_version']}"


for container in all_containers:
    print(f"Using funcX version: {funcx.__version__}")
    fxc = FuncXClient()
    base_path = '/home/tskluzac/ext_repos/'
    container_path = os.path.join(base_path, container)
    print(f"Container path: {container_path}")
    container_uuid = fxc.register_container(container_path, 'singularity')

    fn_uuid = fxc.register_function(hdf_extract,
                                    container_uuid=container_uuid,
                                    description="New sum function defined without string spec")

    print(f"FN UUID: {fn_uuid}")
    res = fxc.run(sample_hdf_1, endpoint_id=js_ep_id, function_id=fn_uuid)
    print(res)
    for i in range(100):
        # TODO: break when successful
        try:
            x = fxc.get_result(res)
            print(x)
            break
        except Exception as e:
            print("Exception: {}".format(e))
            time.sleep(2)
