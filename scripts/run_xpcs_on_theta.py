
import time
import csv
from queue import Queue
from funcx import FuncXClient
from extractors.utils.batch_utils import remote_extract_batch, remote_poll_batch
from tests.test_utils.mock_event import create_mock_event, create_many_family_mock_event
from extractors.utils.base_extractor import base_extractor
from extractors.xtract_xpcs import XPCSExtractor
from extractors.utils.base_event import create_event


"""
This script will run the Gladier team's XPCS script on each 
file from the 2021-1 file set on Petrel. It will do so on Theta. 
"""

# TODO:
# 1. Point to the right xpcs_data file.
# 2. Point to the right bunch of metadata files.

fxc = FuncXClient()
xpcs_x = XPCSExtractor()

trimester = "2019-3"
ep_id = "12ff7fa9-a76b-4188-82ea-1c0081c3c73a"

container_uuid = fxc.register_container('/home/tskluzac/.xtract/.containers/xtract-xpcs.img', 'singularity')
print("Container UUID: {}".format(container_uuid))
fn_uuid = fxc.register_function(base_extractor,
                                container_uuid=container_uuid,
                                description="Tabular test function.")
print("FN_UUID : ", fn_uuid)

task_batch_size = 128
fx_batch_size = 32
max_tasks_at_ep = 5000

hdf_count = 0
task_batches = Queue()

max_count = 500000


crawl_info = f"/Users/tylerskluzacek/Desktop/xpcs_crawls/xpcs_crawl_info_{trimester}.csv"

print(f"Reading data...")
with open(crawl_info, 'r') as f:
    csv_reader = csv.reader(f)

    batch = fxc.create_batch()

    current_app_batch = 0
    current_files_to_batch = []
    for line in csv_reader:
        raw_filename = line[0]
        if not raw_filename.endswith('.hdf'):
            continue
        # real_filename = raw_filename.replace('/XPCSDATA/', '/projects/CSC249ADCD01/skluzacek/')
        real_filename = raw_filename.replace('/XPCSDATA/', '/eagle/Xtract/')
        hdf_count += 1

        current_files_to_batch.append(real_filename)

        if len(current_files_to_batch) >= task_batch_size:
            event = create_many_family_mock_event(current_files_to_batch)
            task_batches.put(event)
            current_app_batch = 0
            current_files_to_batch = []

    # Grab the 'mini batch' at the end...
    if len(current_files_to_batch) > 0:
        event = create_many_family_mock_event(current_files_to_batch)
        task_batches.put(event)
        print(f"Loaded partial batch of size: {len(current_files_to_batch)}")
        # if hdf_count > max_count:
        #     print(f"DEBUG -- breaking!!!")
        #     break


poll_queue = Queue()
print(f"Number of HDFs: {hdf_count}")
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

        payload = create_event(ep_name="foobar",
                                   family_batch=event['family_batch'],
                                   xtract_dir="/home/tskluzac/.xtract",
                                   sys_path_add="/",
                                   module_path="xtract_xpcs_main",
                                   metadata_write_path=f'/home/tskluzac/{trimester}-completed')

        current_batch.append(payload)

        # print(current_batch)
        print(f"Len Current Batch: {len(current_batch)}")

    for item in current_batch:
        batch.add(item, endpoint_id=ep_id, function_id=fn_uuid)

    # List of task_ids
    batch_res = fxc.batch_run(batch)

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

    # List of task_ids
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
                # x[tid_key]['exception'].reraise()

        else:
            poll_queue.put(tid_key)
    print(f"Success Count: {success_count}")
    print(f"Fail Count: {fail_count}")
    print(f"Size of poller queue: {poll_queue.qsize()}")
    time.sleep(1)
