
import time
import json
import csv
from queue import Queue
from funcx import FuncXClient
from extractors.utils.batch_utils import remote_extract_batch, remote_poll_batch
from tests.test_utils.mock_event import create_mock_event, create_many_family_mock_event
from extractors.utils.base_extractor import base_extractor
from extractors.xtract_xpcs import XPCSExtractor
from extractors.xtract_netcdf import NetCDFExtractor
from extractors.xtract_jsonxml import JsonXMLExtractor
from extractors.xtract_hdf import HDFExtractor
from extractors.xtract_keyword import KeywordExtractor
from extractors.xtract_imagesort import ImagesExtractor
from extractors.xtract_python import PythonExtractor
from extractors.xtract_c_code import CCodeExtractor
from extractors.xtract_tabular import TabularExtractor
from extractors.xtract_tika import TikaExtractor

from extractors.utils.base_event import create_event


"""
This script will run the Gladier team's XPCS script on each 
file from the 2021-1 file set on Petrel. It will do so on Theta. 
"""

# extractors =

# TODO:
# 1. Point to the right xpcs_data file.
# 2. Point to the right bunch of metadata files.

fxc = FuncXClient()
# xpcs_x = XPCSExtractor()
# xpcs_x = NetCDFExtractor()
# xpcs_x = JsonXMLExtractor()
# xpcs_x = HDFExtractor()
# xpcs_x = ImagesExtractor()
# xpcs_x = KeywordExtractor()
# xpcs_x = PythonExtractor()
# xpcs_x = TabularExtractor()
xpcs_x = CCodeExtractor()
# xpcs_x = TikaExtractor()

ep_id = "2293034e-4c9f-459c-a6f0-0ed310a8e618"
extractor_name = "matio"
repo_name = "mdf"

container_uuid = fxc.register_container(f'/home/tskluzac/.xtract/.containers/xtract-{extractor_name}.img', 'singularity')
print("Container UUID: {}".format(container_uuid))
fn_uuid = fxc.register_function(base_extractor,
                                container_uuid=container_uuid,
                                description="Tabular test function.")
print("FN_UUID : ", fn_uuid)

task_batch_size = 16  # 128
fx_batch_size = 64  # 32

hdf_count = 0
task_batches = Queue()
missing_file = "/Users/tylerskluzacek/missing_files.json"
missing_file_0 = "/Users/tylerskluzacek/missing_files_0.json"
# missing_file = None
missing_file = None

min_num = 1900000   # 1400000
max_count = 2300000  # 1900000

# crawl_info = f"/Users/tylerskluzacek/Desktop/xpcs_crawls/xpcs_crawl_info_{trimester}.csv"
# crawl_info = f"/Users/tylerskluzacek/Desktop/iccs_crawls/cdiac_EAGLE.csv"
# crawl_info = f"/Users/tylerskluzacek/Desktop/iccs_crawls/crawl_cord_EAGLE.csv"
crawl_info = f"/Users/tylerskluzacek/Desktop/crawl_mdf.csv"

# TODO: 'AND missing_files_0.json'
missing_dict_a = dict()
missing_dict_b = dict()

missing_dict = dict()
if missing_file is not None:
    with open(missing_file, 'r') as f:
        missing_data = json.load(f)['missing_ls']
        for item in missing_data:
            missing_dict_a[item] = "hi"

    with open(missing_file_0, 'r') as f:
        missing_data = json.load(f)['missing_ls']
        for item in missing_data:
            missing_dict_b[item] = "hi"

    print("Creating super-dict...")
    for item in missing_dict_a:
        if item in missing_dict_b:
            missing_dict[item] = 'hi'

    print(f"Files in queue: {len(missing_dict)}")

print(f"Reading data...")
with open(crawl_info, 'r') as f:
    csv_reader = csv.reader(f)

    batch = fxc.create_batch()

    current_app_batch = 0
    current_files_to_batch = []
    file_count = 0
    for line in csv_reader:
        raw_filename = line[0]
        # if not raw_filename.endswith('.hdf'):
        #     continue
        # real_filename = raw_filename.replace('/XPCSDATA/', '/projects/CSC249ADCD01/skluzacek/')
        real_filename = raw_filename.replace('/MDF/', '/projects/CSC249ADCD01/skluzacek/MDF/')
        # hdf_count += 1

        # We should scan, unless the file is not in a "Missing data" json.
        should_scan_file = True
        if missing_file is not None:
            if str(file_count) not in missing_dict:
                should_scan_file = False

        if file_count < min_num:
            # print("BELOW")

            should_scan_file = False

        # If we should scan the file, then throw it in the batch!
        if should_scan_file:
            print(file_count)
            current_files_to_batch.append({'filename': real_filename, 'family_id': file_count})

        if len(current_files_to_batch) >= task_batch_size:
            event = create_many_family_mock_event(current_files_to_batch)
            task_batches.put(event)
            current_app_batch = 0
            current_files_to_batch = []

        file_count += 1
        if file_count > max_count:
            break

    # Grab the 'mini batch' at the end...
    if len(current_files_to_batch) > 0:
        event = create_many_family_mock_event(current_files_to_batch)
        task_batches.put(event)
        print(f"Loaded partial batch of size: {len(current_files_to_batch)}")
        # if hdf_count > max_count:
        #     print(f"DEBUG -- breaking!!!")
        #     break


poll_queue = Queue()
print(f"Number of batches: {task_batches.qsize()}")
# exit()

print(f"Sending batches")
time.sleep(2)
current_batch = []

# While my queue isn't empty
while not task_batches.empty():

    # Temporary edge case fix.
    current_batch = []

    while len(current_batch) < fx_batch_size:
        print(f"All events queue size: {task_batches.qsize()}")
        if task_batches.empty():
            break
        event = task_batches.get()

        # extractor_name = 'opener'
        payload = create_event(ep_name="foobar",
                                   family_batch=event['family_batch'],
                                   xtract_dir="/home/tskluzac/.xtract",
                                   sys_path_add="/",
                                   module_path=f"xtract_{extractor_name}_main",
                                   metadata_write_path=f'/home/tskluzac/{extractor_name}-{repo_name}-completed',
                                   writer='json-np')

        current_batch.append(payload)

        # print(current_batch)
        print(f"Len Current Batch: {len(current_batch)}")

    for item in current_batch:
        batch.add(item, endpoint_id=ep_id, function_id=fn_uuid)

    # List of task_ids
    ts = time.time()
    batch_res = fxc.batch_run(batch)
    te = time.time()

    print(f"Total request round-trip time: {te-ts}")

    for item in batch_res:
        poll_queue.put(item)

    batch = fxc.create_batch()  # empty batch.

    # TODO: sauce this to be much better.
    print("Moving to phase 2...")
    time.sleep(0.5)
    # break  # TODO: remove this break.


# Cleanup the 'non-full-last-batch-stragglers'
if len(current_batch) > 0:
    for item in current_batch:
        batch.add(item, endpoint_id=ep_id, function_id=fn_uuid)

    batch_res = fxc.batch_run(batch)

    for item in batch_res:
        poll_queue.put(item)

success_count = 0
fail_count = 0
print(f"Moving to status checks...")
while True:
    tids_to_check = []
    poll_batch = []
    while len(tids_to_check) < fx_batch_size and not poll_queue.empty():
        tid = poll_queue.get()
        tids_to_check.append(tid)

    x = fxc.get_batch_result(tids_to_check)

    for tid_key in x:
        if not x[tid_key]['pending']:
            if x[tid_key]['status'] != 'failed':
                success_count += 1
            else:
                print("FAILED!")
                fail_count += 1
                print(x[tid_key]['exception'])
                # x[tid_key]['exception'].reraise()

        else:
            poll_queue.put(tid_key)
    print(f"Success Count: {success_count}")
    print(f"Fail Count: {fail_count}")
    print(f"Size of poller queue: {poll_queue.qsize()}")
    time.sleep(1)
