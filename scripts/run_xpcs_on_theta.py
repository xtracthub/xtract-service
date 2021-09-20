
import time
import csv
from queue import Queue
from funcx import FuncXClient
from extractors.utils.batch_utils import remote_extract_batch, remote_poll_batch
from tests.test_utils.mock_event import create_mock_event
from extractors.utils.base_extractor import base_extractor
from extractors.xtract_xpcs import XPCSExtractor
from extractors.utils.base_event import create_event


"""
This script will run the Gladier team's XPCS script on each 
file from the 2021-1 file set on Petrel. It will do so on Theta. 
"""

# TODO: figure out why only 9533 are writing to disk. 

fxc = FuncXClient()
xpcs_x = XPCSExtractor()

ep_id = "0ac60203-68f1-464b-a595-b10e85ae2084"

container_uuid = fxc.register_container('/projects/CSC249ADCD01/skluzacek/containers/xtract-xpcs.img', 'singularity')
print("Container UUID: {}".format(container_uuid))
fn_uuid = fxc.register_function(base_extractor,
                                container_uuid=container_uuid,
                                description="Tabular test function.")
print("FN_UUID : ", fn_uuid)

batch_size = 500
# max_tasks_at_ep = 1000

hdf_count = 0
all_events = Queue()

max_count = 10000

crawl_info = "/Users/tylerskluzacek/Desktop/xpcs_crawl_info.csv"

print(f"Reading data...")
with open(crawl_info, 'r') as f:
    csv_reader = csv.reader(f)

    batch = fxc.create_batch()

    for line in csv_reader:
        raw_filename = line[0]
        if not raw_filename.endswith('.hdf'):
            continue
        real_filename = raw_filename.replace('/XPCSDATA/', '/projects/CSC249ADCD01/skluzacek/')
        hdf_count += 1

        event = create_mock_event([real_filename])
        # print(event['family_batch'].to_dict())
        # time.sleep(0.5)
        all_events.put(event)

        if hdf_count > max_count:
            print(f"DEBUG -- breaking!!!")
            break

poll_queue = Queue()

print(f"Sending batches")
time.sleep(2)

# While my queue isn't empty
while not all_events.empty():

    # Temporary edge case fix.
    if all_events.qsize() <= 2:
        break

    current_batch = []

    while len(current_batch) < batch_size:
        print(f"All events queue size: {all_events.qsize()}")
        event = all_events.get()

        print("PLOP")
        payload = create_event(ep_name="foobar",
                                   family_batch=event['family_batch'],
                                   xtract_dir="/home/tskluzac/.xtract",
                                   sys_path_add="/",
                                   module_path="gather_xpcs_metadata",
                                   metadata_write_path='/home/tskluzac/testytesty')

        current_batch.append(payload)

        # print(current_batch)
        print(f"Len Current Batch: {len(current_batch)}")

        # print(f"Pre-item ID: {xpcs_event['family_batch'].to_dict()['families'][0]['family_id']}")
        # time.sleep(0.5)

        # i = 0
        # for i in range(0, len(current_batch)):
        #     item = current_batch[i]
        #     print(item['family_batch'].to_dict()['families'][0]['family_id'])
            # current_batch.put(item)

        # current_batch.put(None)
            # i += 1
            # print(f"Item ID: {item['family_batch'].to_dict()['families'][0]['family_id']}")
        # exit()

    # exit()
    # print(current_batch)
    # current_batch.put(None)
    for item in current_batch:
        # item = current_batch.get()
        # print(f"Item ID: {item['family_batch'].to_dict()['families'][0]['family_id']}")
        batch.add(item, endpoint_id=ep_id, function_id=fn_uuid)
    # current_batch = []
    # time.sleep(5)

    # List of task_ids
    batch_res = fxc.batch_run(batch)

    for item in batch_res:
        poll_queue.put(item)

    batch = fxc.create_batch()  # empty batch.

    # TODO: sauce this to be much better.
    print("Moving to phase 2...")
    time.sleep(1)

success_count = 0
print(f"Moving to status checks...")
while True:
    tids_to_check = []
    poll_batch = []
    while len(tids_to_check) < batch_size:
        tid = poll_queue.get()
        tids_to_check.append(tid)

    x = fxc.get_batch_result(tids_to_check)

    for tid_key in x:
        if not x[tid_key]['pending']:
            print(x[tid_key])
            success_count += 1
        else:
            poll_queue.put(tid_key)
    print(f"Success Count: {success_count}")

    time.sleep(2)
