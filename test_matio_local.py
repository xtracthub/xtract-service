
from funcx import FuncXClient

from extractors.xtract_matio import matio_extract
from extractors.xtract_matio import MatioExtractor
# from extractors.xtract_nothing import NothingExtractor
from objsize import get_deep_size

from extractors.utils.mappings import mapping

from queue import Queue

import time
import json
import math
import csv

import threading


from xtract_sdk.packagers.family import Family
from xtract_sdk.packagers.family_batch import FamilyBatch


with open("timer2.txt", 'w') as f:
    f.close()

# HERE IS WHERE WE SET THE SYSTEM #
# system = "js"
system = "midway2"

# 10**6 = 1mb
soft_batch_bytes_max = 10**6
soft_batch_bytes_max = 0

map = None
if system == 'midway2':
    map = mapping['xtract-matio::midway2']

elif system == 'theta':
    map = mapping['xtract-matio::theta']

elif system == 'js':
    map = mapping['xtract-matio::js']

base_url = ""

base_path = map['data_path']
container_type = map['container_type']
location = map['location']
ep_id = map['ep_id']

# TODO: make sure this is proper size.

map_size = 8
batch_size = 8

file_cutoff = 90000
max_outstanding_tasks = 90000  # 50000


# skip_n
skip_n = 0

class test_orch():
    def __init__(self):
        self.current_tasks_on_ep = 0
        self.max_tasks_on_ep = file_cutoff # IF SET TO FILE_CUTOFF, THEN THIS IS THE MAX.
        self.fxc = FuncXClient()

        self.funcx_batches = Queue()
        self.polling_queue = Queue()

        self.num_poll_reqs = 0
        self.num_send_reqs = 0

        self.total_families_sent = 0

        self.successes = 0
        self.failures = 0

        self.max_outstanding_tasks = max_outstanding_tasks

        self.family_queue = Queue()

        self.fam_batches = []

        # big_json = "/home/ubuntu/old_xtracthub-service/experiments/tyler_everything.json"
        # big_json = "/Users/tylerskluzacek/Desktop/tyler_everything.json"

        import os
        print(os.getcwd())

        #big_json = "../experiments/tyler_30k.json"
        big_json = "experiments/tyler_200k.json"
        # big_json = "/Users/tylerskluzacek/PyCharmProjects/xtracthub-service/experiments/tyler_20k.json"

        t0 = time.time() 
        with open(big_json, 'r') as f:
            self.fam_list = json.load(f)

        print(f"Number of famlilies in fam_list: {len(self.fam_list)}")
        t1 = time.time()

        print(f"Time to load families: {t1-t0}")
        time.sleep(5) # Time to read!!!

        # Transfer the stored list to a queue to promote good concurrency while making batches.
        i = 0  # TODO: added skip logic here!
        for item in self.fam_list:
            if i < skip_n:
                continue
            self.family_queue.put(item)

        self.start_time = time.time()

        self.preproc_fam_batches()

        print(f"Number of funcX batches: {self.funcx_batches.qsize()}")
        # exit()

    def path_converter(self, family_id, old_path):
        path_ls = old_path.split('/')
        file_name = path_ls[-1]
        new_path = None
        if system == "midway2":
            new_path = f"/project2/chard/skluzacek/data_to_process/{family_id}/{file_name}"
        elif system == "theta":
            new_path = f"/projects/CSC249ADCD01/skluzacek{old_path}"  #TODO: change this for things
        elif system == "js":
            new_path = f"/home/tskluzac/{family_id}/{file_name}"
        return new_path

    def preproc_fam_batches(self):

        fam_count = 0

        # Just create an empty one out here so Python doesn't yell at me.
        fam_batch = FamilyBatch()

        num_overloads = 0
        # while we have files and haven't exceeded the weak scaling threshold (file_cutoff)
        while not self.family_queue.empty() and fam_count < file_cutoff:

            fam_batch = FamilyBatch()
            total_fam_batch_size = 0

            # Keep making batch until
            while len(fam_batch.families) < map_size and not self.family_queue.empty() and fam_count < file_cutoff:

                fam_count += 1
                fam = self.family_queue.get()

                total_family_size = 0
                # First convert to the correct paths
                for file_obj in fam['files']:
                    old_path = file_obj['path']
                    new_path = self.path_converter(fam['family_id'], old_path)
                    file_obj['path'] = new_path
                    file_size = file_obj['metadata']['physical']['size']
                    total_family_size += file_size

                for group in fam['groups']:
                    for file_obj in group['files']:
                        old_path = file_obj['path']
                        new_path = self.path_converter(fam['family_id'], old_path)
                        file_obj['path'] = new_path

                empty_fam = Family()
                empty_fam.from_dict(fam)

                # We will ONLY handle the SIZE issue in here.

                if soft_batch_bytes_max > 0:
                    # So if this last file would put us over the top,
                    if total_fam_batch_size + total_family_size > soft_batch_bytes_max:
                        num_overloads += 1
                        print(f"Num overloads {num_overloads}")
                        # then we append the old batch (if not empty),
                        if len(fam_batch.families) > 0:
                            self.fam_batches.append(fam_batch)

                        # empty the old one
                        fam_batch = FamilyBatch()
                        total_fam_batch_size = total_family_size

                        assert(len(fam_batch.families) == 0)

                # and then continue (here we either add to our prior fam_batch OR the new one).
                fam_batch.add_family(empty_fam)

            assert len(fam_batch.families) <= map_size

            self.fam_batches.append(fam_batch)

        # img_extractor = NothingExtractor()
        img_extractor = MatioExtractor()

        # TODO: ADDING TEST. Making sure we have all of our files here.

        ta = time.time()
        num_families = 0
        for item in self.fam_batches:
            num_families += len(item.families)

        print(num_families)
        tb = time.time()
        print(f"Time to move families: {tb-ta}") 
        time.sleep(5)
        # exit()

        # exit()

        # This check makes sure our batches are the correct size to avoid the January 2021 disaster of having vastly
        #  incorrect numbers of batches.
        #
        #  Here we are checking that the number of families we are processing is LESS than the total number of
        #   batches times the batch size (e.g., the last batch can be full or empty), and the number of families
        #   is GREATER than the case where our last map is missing.
        #
        #
        #  This leaves a very small window for error. Could use modulus to be more exact.

        # TODO: Bring this back (but use for grouping by num. files)

        # try:
        #     assert len(self.fam_batches) * (map_size-1) <= fam_count <= len(self.fam_batches) * map_size
        # except AssertionError as e:
        #     print(f"Caught {e} after creating client batches...")
        #     print(f"Number of batches: {len(self.fam_batches)}")
        #     print(f"Family Count: {fam_count}")
        #
        #     print("Cannot continue. Exiting...")
        #     exit()

        print(f"Container type: {container_type}")
        print(f"Location: {location}")
        self.fn_uuid = img_extractor.register_function(container_type=container_type, location=location,
                                                  ep_id=ep_id, group="a31d8dce-5d0a-11ea-afea-0a53601d30b5")

        # funcX batching. Here we take the 'user' FamilyBatch objects and put them into a batch we send to funcX.
        num_fx_batches = 0
        current_batch = []

        print(f"Number of family batches: {len(self.fam_batches)}")
        for fam_batch in self.fam_batches:

            # print(len(current_batch))
            # print(batch_size)

            if len(current_batch) < batch_size:
                current_batch.append(fam_batch)
            else:
                # print("Marking batch!")
                # print(len(current_batch))
                self.funcx_batches.put(current_batch)
                current_batch = [fam_batch]
                num_fx_batches += 1

        # Grab the stragglers.
        if len(current_batch) > 0:
            print("Marking batch!")
            self.funcx_batches.put(current_batch)
            num_fx_batches += 1

        # See same description as above (map example) for explanation.
        try:
            theor_full_batches = math.ceil(len(self.fam_batches) / batch_size)

            # print(f"Theoretical full batches: {}")
            assert theor_full_batches == num_fx_batches
        except AssertionError as e:
            print(f"Caught {e} after creating funcX batches...")
            print(f"Number of batches: {self.funcx_batches.qsize()}")
            print(f"Family Count: {num_fx_batches}")

            print("Cannot continue. Exiting...")
            exit()

    # TODO: let the failures fail.
    def send_batches_thr_loop(self):

        # While there are still batches to send.
        #  Note that this should not be 'limiting' as we do that in preprocessing.
        while not self.funcx_batches.empty():

            # current_tasks_on_ep = tasks_sent - tasks_received
            if self.current_tasks_on_ep > self.max_outstanding_tasks:
                print(f"There are {self.current_tasks_on_ep}. Sleeping...")
                time.sleep(5)
                continue

            # Grab one
            batch = self.funcx_batches.get()
            fx_batch = self.fxc.create_batch()

            # Now we formally pull down each funcX batch and add each of its elements to an fx_batch.
            # TODO: could do this before putting in list.
            for item in batch:

                fam_batch_size = len(item.families)

                fx_batch.add({'family_batch': item}, endpoint_id=ep_id, function_id=self.fn_uuid)
                self.current_tasks_on_ep += fam_batch_size

            # try:
            # TODO: bring this back when we figure out what errors it's causing.
            import random 
            x = random.randint(1,5)
            time.sleep(x/2)
            res = self.fxc.batch_run(fx_batch)
            self.num_send_reqs += 1
            # except Exception as e:
            #     print("WE CAUGHT AN EXCEPTION WHILE SENDING. ")
            #     time.sleep(0.5)
            #     continue

            for tid in res:
                self.polling_queue.put(tid)


            # import random 
            # time.sleep(random.randint(1,3))
            # time.sleep(0.75)




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

            time.sleep(0.5)

            start_req = time.time()
            res = self.fxc.get_batch_status(current_tid_batch)
            end_req = time.time()
            self.num_poll_reqs += 1

            print(f"Time to process batch: {end_req-start_req}")
            

            for item in res:

                # print(res[item])
                if 'result' in res[item]:

                    print(f"Received result: {res[item]['result']}")
                    exit()

                    # print(res[item])

                    #print(res[item]['result'])

                    # ret_fam_batch = res[item]['result']['family_batch']
                    ret_fam_batch = res[item]['result']

                    num_finished = ret_fam_batch['finished']

                    print(num_finished)



                    # timer = res[item]['result']['total_time']

                    family_file_size = 0
                    bad_extract_time = 0
                    good_extract_time = 0

                    good_parsers = ""

                    # family_mdata_size = get_deep_size(ret_fam_batch)
                    #
                    # for family in ret_fam_batch.families:
                    #
                    #     # print(family.metadata)
                    #
                    #     for file in family.files:
                    #         family_file_size += file['metadata']['physical']['size']
                    #
                    #     for gid in family.groups:
                    #         g_mdata = family.groups[gid].metadata
                    #         # print(g_mdata)
                    #
                    #         if g_mdata['matio'] != {} and g_mdata['matio'] is not None:
                    #             good_parsers = good_parsers + g_mdata['parser']
                    #             good_extract_time += g_mdata['extract time']
                    #         else:
                    #             bad_extract_time = g_mdata['extract time']
                    #
                    #     # TODO: These are at the family_batch level.
                    #
                    #     import_time = res[item]['result']["import_time"]
                    #     family_fetch_time = res[item]['result']["family_fetch_time"]
                    #     file_unpack_time = res[item]['result']["file_unpack_time"]
                    #     full_extraction_loop_time = res[item]['result']["full_extract_loop_time"]


                        # import_time = 0
                        # family_fetch_time = 0
                        # file_unpack_time = 0
                        # full_extraction_loop_time = 0
                        #
                        # with open('timer_file.txt', 'a') as g:
                        #     csv_writer = csv.writer(g)
                        #     csv_writer.writerow([timer, family_file_size, family_mdata_size, good_extract_time,
                        #                         bad_extract_time, import_time, family_fetch_time, file_unpack_time,
                        #                         full_extraction_loop_time, good_parsers])

                    # fam_len = len(ret_fam_batch.families)

                    with open('timer2.txt', 'a') as g:
                        csv_writer = csv.writer(g)
                        csv_writer.writerow([time.time(), num_finished])

                    self.successes += num_finished

                    self.current_tasks_on_ep -= num_finished

                    # NOTE -- we're doing nothing with the returned metadata here.

                elif 'exception' in res[item]:
                    res[item]['exception'].reraise()

                else:
                    self.polling_queue.put(item)
                """

                else:
                    print("*********ERROR *************")
                    self.failures += 1
                    print(res)
                """

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


for i in range(12):
    thr = threading.Thread(target=perf_orch.send_batches_thr_loop, args=())
    thr.start()
    print(f"Started the {i}th task push thread...")

for i in range(14):
    thr = threading.Thread(target=perf_orch.polling_loop, args=())
    thr.start()
    print(f"Started the {i}th result thread...")

thr = threading.Thread(target=perf_orch.stats_loop, args=())
thr.start()


# 5000 at 569.6812839508057

# 10000 at

# 20000 at 2211
