
import time

from funcx import FuncXClient
from tests.test_utils.mock_event import create_mock_event
from extractors.utils.base_extractor import base_extractor
from extractors.xtract_tabular import TabularExtractor

test_file = '/home/tskluzac/testytesty/e0870d99-926d-4530-ad40-56bb6003fbc8'

mock_event = create_mock_event([test_file])
ext = TabularExtractor()

tabular_event = ext.create_event(ep_name="foobar",
                             family_batch=mock_event['family_batch'],
                             xtract_dir="/home/tskluzac/.xtract",
                             sys_path_add="/",
                             module_path="xtract_jsonxml_main",
                             metadata_write_path='/home/tskluzac/testytesty')


def test(event):
    import os
    return os.environ['container_version']


def main(fxc, ep_id):
    container_uuid = fxc.register_container('/projects/CSC249ADCD01/skluzacek/containers/xtract-jsonxml.img', 'singularity')
    print("Container UUID: {}".format(container_uuid))
    fn_uuid = fxc.register_function(base_extractor,
                                    #ep_id, # TODO: We do not need ep id here
                                    container_uuid=container_uuid,
                                    description="Tabular test function.")
    print("FN_UUID : ", fn_uuid)
    res = fxc.run(tabular_event,
                  endpoint_id=ep_id, function_id=fn_uuid)
    print(res)
    for i in range(100):
        try:
            x = fxc.get_result(res)
            print(x)
            break
        except Exception as e:
            print("Exception: {}".format(e))
            time.sleep(2)


if __name__=="__main__":
    fxc = FuncXClient()
    main(fxc, "0ac60203-68f1-464b-a595-b10e85ae2084")