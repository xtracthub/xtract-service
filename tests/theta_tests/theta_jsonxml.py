
import time

from funcx import FuncXClient
from tests.test_utils.mock_event import create_mock_event
from extractors.utils.base_extractor import base_extractor
from extractors.xtract_jsonxml import JsonXMLExtractor

test_file = '/home/tskluzac/.xtract/.test_files/covid-test-paper.json'

mock_event = create_mock_event([test_file])
ext = JsonXMLExtractor()

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
    container_uuid = fxc.register_container('/home/tskluzac/.xtract/.containers/xtract-jsonxml.img', 'singularity')
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
    main(fxc, "e1398319-0d0f-4188-909b-a978f6fc5621")