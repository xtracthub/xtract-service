
from funcx import FuncXClient

from extractors.xtract_matio import matio_extract
from extractors.xtract_matio import MatioExtractor

from extractors.utils.mappings import mapping

from queue import Queue

import time
import json

import threading

from xtract_sdk.packagers.family import Family
from xtract_sdk.packagers.family_batch import FamilyBatch


with open("timer_file.txt", 'w') as f:
    f.close()

# HERE IS WHERE WE SET THE SYSTEM #
system = "theta"

map = None
if system == 'midway2':
    map = mapping['xtract-matio::midway2']

elif system == 'theta':
    map = mapping['xtract-matio::theta']

base_url = ""

base_path = map['data_path']
container_type = map['container_type']
location = map['location']
ep_id = map['ep_id']

# TODO: make sure this is proper size.
map_size = 4
batch_size = 100


file_cutoff = 1120


class test_orch():
    def __init__(self):
        self.current_tasks_on_ep = 0
        self.max_tasks_on_ep = 200000
        self.fxc = FuncXClient()

        self.funcx_batches = Queue()
        self.polling_queue = Queue()

        self.num_poll_reqs = 0
        self.num_send_reqs = 0

        self.total_families_sent = 0

        self.successes = 0
        self.failures = 0

        self.family_queue = Queue()

        self.fam_batches = []

        big_json = "/Users/tylerskluzacek/PyCharmProjects/xtracthub-service/experiments/tyler_20k.json"

        with open(big_json, 'r') as f:
            self.fam_list = json.load(f)

        # Transfer the stored list to a queue to promote good concurrency while making batches.
        for item in self.fam_list:
            self.family_queue.put(item)

        self.start_time = time.time()

        self.preproc_fam_batches()

    def path_converter(self, family_id, old_path):
        path_ls = old_path.split('/')
        file_name = path_ls[-1]
        new_path = None
        if system == "midway2":
            new_path = f"/project2/chard/skluzacek/{family_id}/{file_name}"
        elif system == "theta":
            new_path = f"/projects/CSC249ADCD01/skluzacek/data_to_process/{family_id}/{file_name}"
        return new_path

    def preproc_fam_batches(self):

        fam_count = 0
        # while we have files and haven't exceeded the weak scaling threshold (file_cutoff)
        while not self.family_queue.empty() and fam_count < file_cutoff:

            fam_batch = FamilyBatch()

            # Keep making batch until
            while len(fam_batch.families) < map_size and not self.family_queue.empty() and fam_count < file_cutoff:

                fam_count += 1
                fam = self.family_queue.get()

                # First convert to the correct paths
                for file_obj in fam['files']:
                    old_path = file_obj['path']
                    new_path = self.path_converter(fam['family_id'], old_path)
                    file_obj['path'] = new_path

                for group in fam['groups']:
                    for file_obj in group['files']:
                        old_path = file_obj['path']
                        new_path = self.path_converter(fam['family_id'], old_path)
                        file_obj['path'] = new_path

                empty_fam = Family()
                empty_fam.from_dict(fam)
                fam_batch.add_family(empty_fam)

            self.fam_batches.append(fam_batch)

        img_extractor = MatioExtractor()

        # This check makes sure our batches are the correct size to avoid the January 2021 disaster of having vastly
        #  incorrect numbers of batches.
        #
        #  Here we are checking that the number of families we are processing is LESS than the total number of
        #   batches times the batch size (e.g., the last batch can be full or empty), and the number of families
        #   is GREATER than the case where our last map is missing.
        #
        #
        #  This leaves a very small window for error. Could use modulus to be more exact.
        try:
            assert len(self.fam_batches) * (map_size-1) <= fam_count <= len(self.fam_batches) * map_size
        except AssertionError as e:
            print(f"Caught {e}")
            print(f"Number of batches: {len(self.fam_batches)}")
            print(f"Family Count: {fam_count}")
            # break

        print(f"Container type: {container_type}")
        print(f"Location: {location}")
        self.fn_uuid = img_extractor.register_function(container_type=container_type, location=location,
                                                  ep_id=ep_id, group="a31d8dce-5d0a-11ea-afea-0a53601d30b5")

        print("boingo")
        exit()

        current_batch = []
        for fam_batch in self.fam_batches:
            if len(current_batch) < batch_size:
                current_batch.append(fam_batch)
            else:
                self.funcx_batches.put(current_batch)
                current_batch = [fam_batch]

        # Grab the stragglers.
        if len(current_batch) > 0:
            self.funcx_batches.put(current_batch)

    # TODO: let the failures fail.
    def send_batches_thr_loop(self):

        while not self.funcx_batches.empty():

            if self.current_tasks_on_ep > self.max_tasks_on_ep:
                print(f"There are {self.current_tasks_on_ep}. Sleeping...")
                time.sleep(5)
                continue

            batch = self.funcx_batches.get()
            fx_batch = self.fxc.create_batch()

            for item in batch:

                fam_batch_size = len(item.families)

                fx_batch.add({'family_batch': item}, endpoint_id=ep_id, function_id=self.fn_uuid)
                self.current_tasks_on_ep += fam_batch_size

            try:
                res = self.fxc.batch_run(fx_batch)
                self.num_send_reqs += 1
            except:
                time.sleep(0.5)
                continue

            for tid in res:
                self.polling_queue.put(tid)

            time.sleep(1)

    def polling_loop(self):
        while True:

            current_tid_batch = []
            for i in range(500):  # TODO: 1000 might be too big?
                if self.polling_queue.empty():
                    print("Polling queue empty. Creating batch!")
                    time.sleep(3)
                    break
                else:
                    tid = self.polling_queue.get()
                    current_tid_batch.append(tid)

            if len(current_tid_batch) == 0:
                print("Batch is empty. Sleeping... ")
                time.sleep(5)
            res = self.fxc.get_batch_status(current_tid_batch)
            self.num_poll_reqs += 1


            for item in res:

                if 'result' in res[item]:

                    ret_fam_batch = res[item]['result']['family_batch']

                    timer = res[item]['result']['total_time']

                    with open('timer_file.txt', 'a') as g:
                        g.write(str(timer) + "\n")

                    # print(timer)
                    # logging.info(timer)

                    fam_len = len(ret_fam_batch.families)
                    self.successes += fam_len

                    self.current_tasks_on_ep -= fam_len

                    # NOTE -- we're doing nothing with the returned metadata here.

                elif 'exception' in res[item]:
                    res[item]['exception'].reraise()

                elif 'status' in res[item]:
                    self.polling_queue.put(item)


                else:
                    print("*********ERROR *************")
                    self.failures += 1
                    print(res)

    def stats_loop(self):
        while True:
            print("*********************************")
            print(f"Num successes: {self.successes}")
            print(f"Num failures: {self.failures}")
            print(f"Only {self.current_tasks_on_ep} tasks at endpoint. ")

            print(f"Number of send requests: {self.num_send_reqs}")
            print(f"Number of poll requests: {self.num_poll_reqs}")
            print("*********************************")
            print(f"Elapsed time: {time.time() - self.start_time}")
            time.sleep(5)


perf_orch = test_orch()

for i in range(7):
    thr = threading.Thread(target=perf_orch.send_batches_thr_loop, args=())
    thr.start()
    print(f"Started the {i}th task push thread...")

for i in range(5):
    thr = threading.Thread(target=perf_orch.polling_loop, args=())
    thr.start()
    print(f"Started the {i}th result thread...")

thr = threading.Thread(target=perf_orch.stats_loop, args=())
thr.start()
