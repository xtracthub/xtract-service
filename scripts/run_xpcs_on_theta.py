
import time
import csv
from funcx import FuncXClient
from extractors.utils.batch_utils import remote_extract_batch, remote_poll_batch
from tests.test_utils.mock_event import create_mock_event
from extractors.utils.base_extractor import base_extractor
from extractors.xtract_xpcs import XPCSExtractor

"""
This script will run the Gladier team's XPCS script on each 
file from the 2021-1 file set on Petrel. It will do so on Theta. 
"""

fxc = FuncXClient()
xpcs_x = XPCSExtractor()

ep_id = "0ac60203-68f1-464b-a595-b10e85ae2084"

container_uuid = fxc.register_container('/projects/CSC249ADCD01/skluzacek/containers/xtract-xpcs.img', 'singularity')
print("Container UUID: {}".format(container_uuid))
fn_uuid = fxc.register_function(base_extractor,
                                #ep_id, # TODO: We do not need ep id here
                                container_uuid=container_uuid,
                                description="Tabular test function.")
print("FN_UUID : ", fn_uuid)

hdf_count = 0
crawl_info = "/Users/tylerskluzacek/Desktop/xpcs_crawl_info.csv"
with open(crawl_info, 'r') as f:
    csv_reader = csv.reader(f)

    for line in csv_reader:
        raw_filename = line[0]
        if not raw_filename.endswith('.hdf'):
            continue
        real_filename = raw_filename.replace('/XPCSDATA/', '/projects/CSC249ADCD01/skluzacek/')
        hdf_count += 1

        event = create_mock_event([real_filename])
        xpcs_event = xpcs_x.create_event(ep_name="foobar",
                                        family_batch=event['family_batch'],
                                        xtract_dir="/home/tskluzac/.xtract",
                                        sys_path_add="/",
                                        module_path="gather_xpcs_metadata",
                                        metadata_write_path='/home/tskluzac/testytesty')

        res = fxc.run(xpcs_event,
                      endpoint_id=ep_id, function_id=fn_uuid)

        for i in range(100):
            try:
                x = fxc.get_result(res)
                print(x)
                # print(x['family_batch'].families[0].metadata)
                break
            except Exception as e:
                print("Exception: {}".format(e))
                time.sleep(2)

print(f"HDF count: {hdf_count}")
