
import time

from funcx import FuncXClient
from tests.test_utils.mock_event import create_mock_event
from extractors.utils.base_extractor import base_extractor
from extractors.xtract_xpcs import XPCSExtractor

# This filename is just very long.
# test_file = '/home/tskluzac/.xtract/.test_files/animal-photography-olga-barantseva-11.jpg'
test_file = "/home/tskluzac/XtractPredictor/cdiac_EAGLE.csv"
# test_file = '/projects/CSC249ADCD01/skluzacek/2021-1/leheny202101/' \
#             'A792_00005_Vol20_T102p7to102p9ohms_1800s_att2_Rq0/' \
#             'A792_00005_Vol20_T102p7to102p9ohms_1800s_att2_Rq0_0001-100000.hdf'

# *** CHANGE THE FOLLOWING TO RUN JOB *** #
extractor = "tabular"
mock_event = create_mock_event([test_file])
ext = XPCSExtractor()

xpcs_event = ext.create_event(ep_name="foobar",
                                 family_batch=mock_event['family_batch'],
                                 xtract_dir="/home/tskluzac/.xtract",
                                 sys_path_add="/",
                                 module_path=f"xtract_{extractor}_main",
                                 metadata_write_path='/home/tskluzac/test_tabular')


def main(fxc, ep_id):
    container_uuid = fxc.register_container(f'/home/tskluzac/.xtract/.containers/xtract-{extractor}.img',
                                            'singularity')
    print("Container UUID: {}".format(container_uuid))
    fn_uuid = fxc.register_function(base_extractor,
                                    #ep_id, # TODO: We do not need ep id here
                                    container_uuid=container_uuid,
                                    description="Tabular test function.")
    print("FN_UUID : ", fn_uuid)
    res = fxc.run(xpcs_event,
                  endpoint_id=ep_id, function_id=fn_uuid)
    print(res)
    for i in range(200):
        try:
            x = fxc.get_result(res)
            print(x)
            # print(x['family_batch'].families[0].metadata)
            break
        except Exception as e:
            print("Exception: {}".format(e))
            time.sleep(2)


if __name__ == "__main__":
    fxc = FuncXClient()
    # main(fxc, "e1398319-0d0f-4188-909b-a978f6fc5621")  # Jetstream
    main(fxc, "12ff7fa9-a76b-4188-82ea-1c0081c3c73a")  # Theta-scalable
