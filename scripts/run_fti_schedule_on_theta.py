
import time
import json
import csv
from queue import Queue
import threading
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

# TODO 1. Confirm the extractions are actually working
# TODO 2. Send some extractors' metadata to /project space? Ha.


class Orchestrator:
    def __init__(self):
        self.fxc = FuncXClient()
        self.extractors = {
            'netcdf': NetCDFExtractor(),
            'jsonxml': JsonXMLExtractor(),
            'hdf': HDFExtractor(),
            'images': ImagesExtractor(),
            'keyword': KeywordExtractor(),
            'tabular': TabularExtractor(),
            'python': PythonExtractor(),
            'c-code': CCodeExtractor()}

        self.task_batch_size = 32
        self.fx_batch_size = 64
        self.poll_batch_size = 1000

        self.ep_id = "e96e2c50-db6a-4fb7-9968-1733bd9a3a0d"
        self.repo_name = "THESIS_EXPERIMENTS"

        self.fx_uuids = dict()

        self.poll_queue = Queue()

        # *** Step 1. Register all extractors. ***
        self.register_functions()

        # *** Step 2. Create the batches. ***
        self.task_batches = self.create_batches()  # Queue() of batches.

        # *** Step 3. [THREADPOOL-1] Ship batches to funcX EP.
        ship_threads = []
        self.num_send = 1
        for i in range(0, self.num_send):
            ship_thr = threading.Thread(target=self.ship_batches, args=())
            ship_thr.start()
            ship_threads.append(ship_thr)

        # *** Step 4. [THREADPOOL-2] Receive the batches from funcX EP.
        self.num_poll = 1
        poll_threads = []
        for i in range(0, self.num_poll):
            poll_thr = threading.Thread(target=self.poll_batches, args=())
            poll_thr.start()
            poll_threads.append(poll_thr)

        # *** Step 5. JOIN the ship threads.
        for thr in ship_threads:
            thr.join()

        # *** Step 6. JOIN the poll threads.
        for thr in poll_threads:
            thr.join()

    def register_functions(self):
        for extractor in self.extractors:
            container_uuid = self.fxc.register_container(f'/home/tskluzac/.xtract/.containers/xtract-{extractor}.img', 'singularity')
            print("Container UUID: {}".format(container_uuid))
            fn_uuid = self.fxc.register_function(base_extractor,
                                            container_uuid=container_uuid,
                                            description="Tabular test function.")
            self.fx_uuids[extractor] = fn_uuid

    def create_batches(self):

        # TODO: kick these out.
        missing_file = None
        max_count = 500000000

        # *** Step 2. Load up batches with functions. ***
        # crawl_info = f"/Users/tylerskluzacek/cdiac-decomp.csv"
        schedule_info = f"/Users/tylerskluzacek/Dropbox/THESIS/yield-cdiac-scheduler.csv"

        current_files_to_batch = {
            'netcdf': [],
            'jsonxml': [],
            'hdf': [],
            'images': [],
            'keyword': [],
            'tabular': [],
            'python': [],
            'c-code': []
        }

        task_batches = Queue()

        print(f"Reading data...")
        with open(schedule_info, 'r') as f:
            csv_reader = csv.reader(f)
            next(csv_reader)

            batch = self.fxc.create_batch()

            file_count = 0
            for line in csv_reader:

                extractor, fid, filename = line

                # We should scan, unless the file is not in a "Missing data" json.
                should_scan_file = True

                # If we should scan the file, then throw it in the batch!
                if should_scan_file:
                    current_files_to_batch[extractor].append({'filenames': [filename], 'family_id': f"{fid}-{extractor}"})

                for extractor in current_files_to_batch:
                    current_ftb = current_files_to_batch[extractor]
                    if len(current_ftb) >= self.task_batch_size:
                        event = create_many_family_mock_event(current_ftb)
                        task_batches.put({'extractor': extractor, 'event': event})  # used to just be 'event'.
                        current_files_to_batch[extractor] = []

                file_count += 1
                if file_count > max_count:
                    break

            # Grab the 'mini batch(es)' at the end...
            for extractor in current_files_to_batch:
                current_ftb = current_files_to_batch[extractor]
                if len(current_ftb) > 0:
                    event = create_many_family_mock_event(current_ftb)
                    task_batches.put({'extractor': extractor, 'event': event})
                    print(f"Loaded partial batch of size: {len(current_ftb)}")

        return task_batches

    # exit()

    def ship_batches(self):

        print(f"Number of batches: {self.task_batches.qsize()}")

        print(f"Sending batches")
        time.sleep(2)  # for clarity during experimental runs (I can see when it is starting).
        current_batch = []
        batch = self.fxc.create_batch()

        # While my queue isn't empty
        while not self.task_batches.empty():

            # Temporary edge case fix.
            current_batch = []

            while len(current_batch) < self.fx_batch_size:
                print(f"All events queue size: {self.task_batches.qsize()}")
                if self.task_batches.empty():
                    break
                raw_item = self.task_batches.get()
                extractor = raw_item['extractor']
                event = raw_item['event']

                if extractor == 'images':
                    writer = 'json-np'
                else:
                    writer = 'json'

                payload = create_event(ep_name="foobar",
                                       family_batch=event['family_batch'],
                                       xtract_dir="/home/tskluzac/.xtract",
                                       sys_path_add="/",
                                       module_path=f"xtract_{extractor}_main",
                                       # metadata_write_path=f'/home/tskluzac/{extractor}-{self.repo_name}-completed',
                                       metadata_write_path=f'/eagle/Xtract/zzz-test',
                                       writer=writer)

                current_batch.append({'payload': payload, 'extractor': extractor})

                # print(current_batch)
                print(f"Len Current Batch: {len(current_batch)}")

            for item in current_batch:
                payload = item['payload']
                extractor = item['extractor']
                fx_func_id = self.fx_uuids[extractor]

                batch.add(payload, endpoint_id=self.ep_id, function_id=fx_func_id)

            # List of task_ids
            batch_res = self.fxc.batch_run(batch)

            for item in batch_res:
                self.poll_queue.put(item)

            batch = self.fxc.create_batch()  # empty batch.

            # TODO: why a sleep...?
            print("Moving to phase 2...")
            #time.sleep(0.5)

        # Cleanup the 'non-full-last-batch-stragglers'
        if len(current_batch) > 0:
            for item in current_batch:
                payload = item['payload']
                extractor = item['extractor']
                fx_func_id = self.fx_uuids[extractor]
                batch.add(payload, endpoint_id=self.ep_id, function_id=fx_func_id)

            # List of task_ids
            print(batch)
            batch_res = self.fxc.batch_run(batch)

            for item in batch_res:
                self.poll_queue.put(item)

    def poll_batches(self):
        success_count = 0
        fail_count = 0
        print(f"Moving to status checks...")
        while True:
            tids_to_check = []
            poll_batch = []
            while len(tids_to_check) < self.fx_batch_size and not self.poll_queue.empty():
                tid = self.poll_queue.get()
                tids_to_check.append(tid)

            x = self.fxc.get_batch_result(tids_to_check)

            for tid_key in x:
                if not x[tid_key]['pending']:
                    if x[tid_key]['status'] != 'failed':
                        success_count += 1
                    else:
                        print("FAILED!")
                        fail_count += 1
                        try:
                            x[tid_key]['exception'].reraise()
                        except Exception as e:
                            print(f"Caught exception: {e}")
                else:
                    self.poll_queue.put(tid_key)
            print(f"Success Count: {success_count}")
            print(f"Fail Count: {fail_count}")
            print(f"Size of poller queue: {self.poll_queue.qsize()}")

            # TODO: WHY THE SLEEP?!
            time.sleep(1)


x = Orchestrator()
