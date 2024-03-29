
import csv
import sys
import json
import time
import logging
import threading

from queue import Queue
from extractors.utils.mappings import mapping
from utils.encoders import NumpyEncoder
from status_checks import get_crawl_status
from funcx.serialize import FuncXSerializer
from xtract_sdk.packagers import Family, FamilyBatch
from prefetcher.prefetcher import GlobusPrefetcher
from extractors.utils.batch_utils import remote_extract_batch, remote_poll_batch
from extractors import xtract_images, xtract_tabular, xtract_matio, xtract_keyword
from orchestrator.orch_utils.utils import create_list_chunks


# TODO: Python garbage collection!
class ExtractorOrchestrator:

    def __init__(self, funcx_eid,
                 mdata_store_path, source_eid=None, dest_eid=None, gdrive_token=None,
                 extractor_finder='gdrive', prefetch_remote=False,
                 data_prefetch_path=None, dataset_mdata=None):

        prefetch_remote = False

        # TODO -- fix this.
        # self.crawl_type = 'from_file'

        self.write_cpe = False

        self.dataset_mdata = dataset_mdata

        self.t_crawl_start = time.time()
        self.t_send_batch = 0
        self.t_transfer = 0

        self.prefetch_remote = prefetch_remote
        self.data_prefetch_path = data_prefetch_path

        self.extractor_finder = extractor_finder

        self.funcx_eid = funcx_eid
        self.func_dict = {"image": xtract_images.ImageExtractor(),
                          "images": xtract_images.ImageExtractor(),
                          "tabular": xtract_tabular.TabularExtractor(),
                          "text": xtract_keyword.KeywordExtractor(),
                          "matio": xtract_matio.MatioExtractor()}

        self.fx_ser = FuncXSerializer()

        self.send_status = "STARTING"
        self.poll_status = "STARTING"
        self.commit_completed = False

        self.source_endpoint = source_eid
        self.dest_endpoint = dest_eid
        self.gdrive_token = gdrive_token

        self.num_families_fetched = 0
        self.get_families_start_time = time.time()
        self.last_checked = time.time()

        self.pre_launch_counter = 0

        self.success_returns = 0
        self.failed_returns = 0

        self.to_send_queue = Queue()

        self.poll_gap_s = 5

        self.get_families_status = "STARTING"

        self.task_dict = {"active": Queue(), "pending": Queue(), "failed": Queue()}

        # Batch size we use to send tasks to funcx.  (and the subbatch size)
        self.map_size = 8
        self.fx_batch_size = 16
        self.fx_task_sublist_size = 500

        # Want to store attributes about funcX requests/responses.
        self.tot_fx_send_payload_size = 0
        self.tot_fx_poll_payload_size = 0
        self.tot_fx_poll_result_size = 0
        self.num_send_reqs = 0
        self.num_poll_reqs = 0
        self.t_first_funcx_invoke = None
        self.max_result_size = 0

        # Number (current and max) of number of tasks sent to funcX for extraction.
        self.max_extracting_tasks = 5
        self.num_extracting_tasks = 0

        self.max_pre_prefetch = 15000  # TODO: Integrate this to actually fix timing bug.

        self.status_things = Queue()

        # If this is turned on, should mean that we hit our local task maximum and don't want to pull down new work...
        self.pause_q_consume = False

        self.file_count = 0
        self.current_batch = []
        self.extract_end = None

        self.mdata_store_path = mdata_store_path
        self.n_fams_transferred = 0

        self.prefetcher_tid = None
        self.prefetch_status = None

        self.fx_headers = {"Authorization": f"Bearer {self.headers['FuncX']}", 'FuncX': self.headers['FuncX']}

        self.family_headers = None
        if 'Petrel' in self.headers:
            self.fx_headers['Petrel'] = self.headers['Petrel']
            self.family_headers = {'Authorization': f"Bearer {self.headers['Petrel']}",
                                   'Transfer': self.headers['Transfer'],
                                   'FuncX': self.headers['FuncX'],
                                   'Petrel': self.headers['Petrel']
                                   }

        self.logger = logging.getLogger(__name__)
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)  # TODO: let's make this configurable.
        self.families_to_process = Queue()
        self.to_validate_q = Queue()

        self.sqs_push_threads = {}
        self.thr_ls = []
        self.commit_threads = 1
        self.get_family_threads = 20

        if self.prefetch_remote:
            self.logger.info("Launching prefetcher...")

            self.logger.info("Prefetcher successfully launched!")

            prefetch_thread = threading.Thread(target=self.prefetcher.main_poller_loop, args=())
            prefetch_thread.start()

        for i in range(0, self.commit_threads):
            thr = threading.Thread(target=self.validate_enqueue_loop, args=(i,))
            self.thr_ls.append(thr)
            thr.start()
            self.sqs_push_threads[i] = True
        self.logger.info(f"Successfully started {len(self.sqs_push_threads)} SQS push threads!")

        if self.crawl_type != 'from_file':
            for i in range(0, self.get_family_threads):
                self.logger.info(f"Attempting to start get_next_families() as its own thread [{i}]... ")
                consumer_thr = threading.Thread(target=self.get_next_families_loop, args=())
                consumer_thr.start()
                print(f"Successfully started the get_next_families() thread number {i} ")
        else:
            print("ATTEMPTING TO LAUNCH **FILE** CRAWL THREAD. ")
            file_crawl_thr = threading.Thread(target=self.read_next_families_from_file_loop, args=())
            file_crawl_thr.start()
            print("Successfully started the **FILE** CRAWL thread!")

        for i in range(0, 15):
            fx_push_thr = threading.Thread(target=self.send_subbatch_thread, args=())
            fx_push_thr.start()
        print("Successfully spun up {i} send threads!")

        with open("cpe_times.csv", 'w') as f:
            f.close()

    def send_subbatch_thread(self):

        # TODO: THIS IS NEW. HUNGRY STANDALONE THREADPOOL THAT SENDS ALL TASKS.
        # TODO: SHOULD TERMINATE WHEN COMPLETED <-- do after paper deadline.

        while True:
            sub_batch = []
            for i in range(10):

                if self.to_send_queue.empty():
                    break

                # I believe this is a blocking call.
                part_batch = self.to_send_queue.get()
                sub_batch.extend(part_batch)

            if len(sub_batch) == 0:
                time.sleep(0.5)
                continue

            batch_send_t = time.time()
            task_ids = remote_extract_batch(sub_batch, ep_id=self.funcx_eid, headers=self.fx_headers)
            batch_recv_t = time.time()

            print(f"Time to send batch: {batch_recv_t - batch_send_t}")

            self.num_send_reqs += 1
            self.pre_launch_counter -= len(sub_batch)

            if type(task_ids) is dict:
                self.logger.exception(f"Caught funcX error: {task_ids['exception_caught']}. \n"
                                      f"Putting the tasks back into active queue for retry")

                for reject_fam_batch in self.current_batch:

                    fam_batch_dict = reject_fam_batch['event']['family_batch']

                    for reject_fam in fam_batch_dict['families']:
                        self.families_to_process.put(json.dumps(reject_fam))

                self.logger.info(f"Pausing for 10 seconds...")

            for task_id in task_ids:
                self.task_dict["active"].put(task_id)
                self.num_extracting_tasks += 1

            time.sleep(0.5)

    def validate_enqueue_loop(self, thr_id):

        self.logger.debug("[VALIDATE] In validation enqueue loop!")
        while True:
            insertables = []

            # If empty, then we want to return.
            if self.to_validate_q.empty():
                # If ingest queue empty, we can demote to "idle"
                if self.poll_status == "COMMITTING":
                    self.sqs_push_threads[thr_id] = "IDLE"
                    print(f"Thread {thr_id} is committing and idle!")
                    time.sleep(0.25)

                    # NOW if all threads idle, then return!
                    if all(value == "IDLE" for value in self.sqs_push_threads.values()):
                        self.commit_completed = True
                        self.poll_status = "COMPLETED"
                        self.extract_end = time.time()
                        self.logger.info(f"Thread {thr_id} is terminating!")
                        return 0

                self.logger.info("[Validate]: sleeping for 5 seconds... ")
                time.sleep(5)
                continue

            self.sqs_push_threads[thr_id] = "ACTIVE"

            # Remove up to n elements from queue, where n is current_batch.
            current_batch = 1
            while not self.to_validate_q.empty() and current_batch < 8:  # TODO: manual downsize so fits on Q.
                item_to_add = self.to_validate_q.get()

                # TODO: THIS IS PULLING THINGS BEFORE THEY GET TO VALIDATE QUEUE.

                insertables.append(item_to_add)
                current_batch += 1

            # TODO: boot all of this out to file.
            if self.write_cpe:
                with open("cpe_times.csv", 'a') as f:

                    csv_writer = csv.writer(f)

                    for item in insertables:

                        fam_batch = json.loads(item['MessageBody'])

                        for family in fam_batch['families']:
                            # print(family)
                            # crawl_timestamp = family['metadata']['crawl_timestamp']
                            pf_timestamp = family['metadata']['pf_transfer_completed']
                            fx_timestamp = family['metadata']['t_funcx_req_received']

                            total_file_size = 0

                            all_files = family['files']
                            total_files = len(all_files)
                            for file_obj in all_files:
                                total_file_size += file_obj['metadata']['physical']['size']

                            csv_writer.writerow(['x', 0, pf_timestamp, fx_timestamp, total_files, total_file_size])

            # TODO: investigate this. Why is this here?
            # try:
            #     ta = time.time()
            #     self.client.send_message_batch(QueueUrl=self.validation_queue_url,
            #                                    Entries=insertables)
            #     tb = time.time()
            #     self.t_send_batch += tb-ta
            #
            # except Exception as e:  # TODO: too vague
            #     print(f"WAS UNABLE TO PROPERLY CONNECT to SQS QUEUE: {e}")

    def send_families_loop(self):

        self.send_status = "RUNNING"
        last_send = time.time()

        while True:

            # print(f"Time since last send: {last_send - time.time()}")
            last_send = time.time()

            if self.num_extracting_tasks > self.max_extracting_tasks:
                self.logger.info(f"[SECOND] Num. active tasks ({self.num_extracting_tasks}) "
                                 f"above threshold. Sleeping for 1 second and continuing...")
                time.sleep(1)
                continue

            if self.prefetch_remote:
                while not self.prefetcher.orch_reader_q.empty():
                    family = self.prefetcher.orch_reader_q.get()
                    family_size = self.prefetcher.get_family_size(json.loads(family))

                    self.prefetcher.bytes_pf_completed -= family_size
                    self.prefetcher.orch_unextracted_bytes += family_size
                    self.pre_launch_counter += 1

                    self.families_to_process.put(family)

            family_list = []
            # Now keeping filling our list of families until it is empty.
            while len(family_list) < self.fx_batch_size and not self.families_to_process.empty():
                family_list.append(self.families_to_process.get())

            if len(family_list) == 0:
                # Here we check if the crawl is complete. If so, then we can start the teardown checks.
                status_dict = get_crawl_status(self.crawl_id)

                if status_dict['crawl_status'] in ["SUCCEEDED", "FAILED"]:

                    # Checking second time due to narrow race condition.
                    if self.families_to_process.empty() and self.get_families_status == "IDLE":

                        if self.prefetch_remote and self.prefetch_status in ["SUCCEEDED", "FAILED"]:

                            self.send_status = "SUCCEEDED"
                            self.logger.info("[SEND] Queue still empty -- terminating!")

                            # this should terminate thread, because there is nothing to process and queue empty
                            return

                    else:  # Something snuck in during the race condition... process it!
                        self.logger.info("[SEND] Discovered final output despite crawl termination. Processing...")
                        time.sleep(0.5)  # This is a multi-minute task and only is reached due to starvation.

            # self.map_size =
            # Cast list to FamilyBatch

            family_batch = FamilyBatch()
            for family in family_list:

                # Get extractor out of each group
                if self.extractor_finder == 'matio':
                    d_type = "HTTPS"
                    extr_code = 'matio'
                    xtr_fam_obj = Family(download_type=d_type)

                    xtr_fam_obj.from_dict(json.loads(family))
                    xtr_fam_obj.headers = self.family_headers

                # TODO: kick this logic for finding extractor into sdk/crawler.
                elif self.extractor_finder == 'gdrive':
                    d_type = 'gdrive'
                    xtr_fam_obj = Family(download_type=d_type)

                    xtr_fam_obj.from_dict(json.loads(family))
                    xtr_fam_obj.headers = self.headers

                    extr_code = xtr_fam_obj.groups[list(xtr_fam_obj.groups.keys())[0]].parser

                else:
                    raise ValueError(f"Incorrect extractor_finder arg: {self.extractor_finder}")

                # TODO: add the decompression work and the hdf5/netcdf extractors!
                if extr_code is None or extr_code == 'hierarch' or extr_code == 'compressed':
                    continue

                extractor = self.func_dict[extr_code]

                # TODO TYLER: Get the proper function ID here!!!
                # ex_func_id = extractor.func_id
                ex_func_id = mapping['xtract-matio::midway2']['func_uuid']

                # Putting into family batch -- we use funcX batching now, but no use rewriting...
                # family_batch = FamilyBatch()
                family_batch.add_family(xtr_fam_obj)
                # print(f"Length of family batch: {len(family_batch.families)}")

                if len(family_batch.families) >= self.map_size:

                    if d_type == "gdrive":
                        self.current_batch.append({"event": {"family_batch": family_batch,
                                                             "creds": self.gdrive_token[0]},
                                                   "func_id": ex_func_id})
                    elif d_type == "HTTPS":
                        self.current_batch.append({"event": {"family_batch": family_batch.to_dict()},
                                                   "func_id": ex_func_id})

                    family_batch = FamilyBatch()

            # Catch any tasks currently in the map and append them to the batch
            if len(family_batch.families) > 0:

                if d_type == "gdrive":
                    self.current_batch.append({"event": {"family_batch": family_batch,
                                                         "creds": self.gdrive_token[0]},
                                               "func_id": ex_func_id})
                elif d_type == "HTTPS":
                    self.current_batch.append({"event": {"family_batch": family_batch.to_dict()},
                                               "func_id": ex_func_id})

            # Now take that straggling family batch and append it.
            if len(self.current_batch) > 0:
                if self.t_first_funcx_invoke is None:
                    self.t_first_funcx_invoke = time.time()

                req_size = 0
                for item in self.current_batch:
                    req_size += sys.getsizeof(item)
                    req_size += 2 * sys.getsizeof(self.funcx_eid)  # need size of fx ep and function id.
                req_size += sys.getsizeof(self.fx_headers)

                self.tot_fx_send_payload_size += req_size

            sub_batches = create_list_chunks(self.current_batch, self.fx_task_sublist_size)

            # print(f"Total sub_batches: {len(sub_batches)}")

            send_threads = []

            # for sub_batch in sub_batches:

            self.to_send_queue.put(self.current_batch)

            # i = 0
            # # TODO: THESE NEED TO BE STANDALONE THREADPOOL
            # for subbatch in sub_batches:
            #     send_thr = threading.Thread(target=self.send_subbatch_thread, args=(subbatch,))
            #     send_thr.start()
            #     send_threads.append(send_thr)
            #     i += 1
            # if i > 0:
            #     print(f"Spun up {i} task-send threads!")
            #
            # for thr in send_threads:
            #     thr.join()

            # Empty the batch! Everything in here has been sent :)
            self.current_batch = []

    def launch_poll(self):
        self.logger.info("Launching poller...")
        po_thr = threading.Thread(target=self.poll_extractions_and_stats, args=())
        po_thr.start()
        self.logger.info("Poller successfully launched!")

    def read_next_families_from_file_loop(self):

        """
        This loads saved crawler state (from a json file) and quickly adds all files to our local
        families_to_process queue. This avoids making calls to SQS to retrieve crawl results.
        """

        with open('/Users/tylerskluzacek/PycharmProjects/xtracthub-service/experiments/tyler_200k.json', 'r') as f:
            all_families = json.load(f)

            num_fams = 0
            for family in all_families:

                self.families_to_process.put(json.dumps(family))
                num_fams += 1
                if num_fams > self.task_cap_until_termination:
                    break

        # self.prefetcher.kill_prefetcher = True
        # self.prefetcher.last_batch = True  # TODO: bring this back for prefetcher.
        print("ENDING LOOP")

    def launch_extract(self):
        ex_thr = threading.Thread(target=self.send_families_loop, args=())
        ex_thr.start()

    def unpack_returned_family_batch(self, family_batch):
        fam_batch_dict = family_batch.to_dict()
        return fam_batch_dict

    def update_and_print_stats(self):
        # STAT CHECK: if we haven't updated stats in 5 seconds, then we update.
        cur_time = time.time()
        if cur_time - self.last_checked > 5:

            # TODO: all the commented-out jazz here should really be 'if prefetch_remote'.
            # total_bytes = self.prefetcher.orch_unextracted_bytes + \
            #               self.prefetcher.bytes_pf_completed + \
            #               self.prefetcher.bytes_pf_in_flight

            print("Phase 4: polling")
            print(f"\t Successes: {self.success_returns}")
            print(f"\t Failures: {self.failed_returns}\n")
            self.logger.debug(f"[VALIDATE] Length of validation queue: {self.to_validate_q.qsize()}")

            # n_pulled = self.prefetcher.next_prefetch_queue.qsize()
            # n_pulled_per_sec = self.num_families_fetched / (cur_time - self.get_families_start_time)
            # n_pf_batch = self.prefetcher.pf_msgs_pulled_since_last_batch
            # n_families_pf_per_sec = self.prefetcher.num_families_transferred / (cur_time - self.get_families_start_time)
            # n_pf = self.prefetcher.num_families_mid_transfer
            # n_awaiting_fx = self.pre_launch_counter + self.prefetcher.orch_reader_q.qsize()
            n_in_fx = self.num_extracting_tasks
            n_success = self.success_returns

            # total_tracked = n_success + n_in_fx + n_awaiting_fx + n_pf + n_pf_batch + n_pulled

            # self.logger.info(f"\n** TASK LOCATION BREAKDOWN **\n"
            #                  f"--Pulled: {n_pulled}\t|\t({n_pulled_per_sec}/s)\n"
            #                  f"--In pf batching: {n_pf_batch}\n"
            #                  f"--Prefetching: {n_pf}\t|\t({n_families_pf_per_sec})\n"
            #                  f"--Awaiting extraction: {n_awaiting_fx}\n"
            #                  f"--In extraction: {n_in_fx}\n"
            #                  f"--Completed: {n_success}\n"
            #                  f"\n-- Fetch-Track Delta: {self.num_families_fetched - total_tracked}\n")

            if self.prefetch_remote:
                # print(f"\t Eff. dir size (GB): {total_bytes / 1024 / 1024 / 1024}")
                print(f"\t N Transfer Tasks: {self.prefetcher.num_transfers}")

            if self.num_send_reqs > 0 and self.num_poll_reqs > 0 and n_success > 0:
                self.logger.info(f"\n** funcX Stats **\n"
                                 f"Num. Send Requests: {self.num_send_reqs}\t|\t({time.time() - self.t_first_funcx_invoke})\n"
                                 f"Num. Poll Requests: {self.num_poll_reqs}\t|\t()\n"
                                 f"Avg. Send Request Size: {self.tot_fx_send_payload_size / self.num_send_reqs}\n"
                                 f"Avg. Poll Request Size: {self.tot_fx_poll_payload_size / self.num_poll_reqs}\n"
                                 f"Avg. Result Size: {self.tot_fx_poll_result_size / n_success}\n"  
                                 f"Max Result Size: {self.max_result_size}\n")

            self.last_checked = time.time()
            print(f"[GET] Elapsed Extract time: {time.time() - self.t_crawl_start}")

    def poll_batch_chunks(self, thr_num, sublist, headers):
        status_thing = remote_poll_batch(sublist, headers)
        self.num_poll_reqs += 1

        status_tup = (thr_num, status_thing)

        self.status_things.put(status_tup)
        time.sleep(1)  # TODO: MIGHT WANT TO TAKE THIS OUT. JUST SEEING IF THIS FIXES funcX SCALE-UP.
        return

    def poll_extractions_and_stats(self):
        mod_not_found = 0
        type_errors = 0

        self.poll_status = "RUNNING"

        while True:

            # This will attempt to print stats on every iteration.
            #   Note: internally, this will only execute max every 5 seconds.
            self.update_and_print_stats()

            if self.task_dict["active"].empty():

                if self.send_status == "SUCCEEDED":
                    print("[POLL] Send status is SUCCEEDED... ")
                    print(f"[POLL] Active tasks: {self.task_dict['active'].empty()}")
                    if self.task_dict["active"].empty():  # check second time b/c of rare r.c.
                        print("Extraction completed. Upgrading status to COMMITTING.")
                        self.poll_status = "COMMITTING"
                        return

                self.logger.debug("No live IDs... sleeping...")
                time.sleep(10)
                continue

            # Here we pull all values from active task queue to create a batch of them!
            num_elem = self.task_dict["active"].qsize()
            print(f"[POLL] Size active queue: {num_elem}")  # TODO: need to shush this.
            tids_to_poll = []  # the batch

            for i in range(0, num_elem):

                ex_id = self.task_dict["active"].get()
                tids_to_poll.append(ex_id)

            t_last_poll = time.time()

            # Send off task_ids to poll, retrieve a bunch of statuses.
            tid_sublists = create_list_chunks(tids_to_poll, self.fx_task_sublist_size)
            # TODO: ^^ this'll need to be a dictionary of some kind so we can track.

            polling_threads = []

            thr_to_sublist_map = dict()

            i = 0
            for tid_sublist in tid_sublists:  # TODO: put a cap on this (if i>10, break (for example)

                # If there's an actual thing in the list...
                if len(tid_sublist) > 0 and i < 15:
                    self.tot_fx_poll_payload_size += sys.getsizeof(tid_sublist)
                    self.tot_fx_poll_payload_size += sys.getsizeof(self.fx_headers) * len(tid_sublist)

                    thr = threading.Thread(target=self.poll_batch_chunks, args=(i, tid_sublist, self.fx_headers))
                    thr.start()
                    polling_threads.append(thr)

                    # NEW STEP: add the thread ID to the dict
                    thr_to_sublist_map[i] = tid_sublist
                    i += 1

            print(f"[POLL] Spun up {i} polling threads!")

            # Now we should wait for our fan-out threads to fan-in
            for thread in polling_threads:
                thread.join()

            # TODO: here is where we can fix this
            # TODO: Create tuples in status things that's a tuple with tid_sublist (OR THREAD ID)
            #  that the status thing is about.
            while not self.status_things.empty():

                thr_id, status_thing = self.status_things.get()

                if "exception_caught" in status_thing:
                    self.logger.exception(f"Caught funcX error: {status_thing['exception_caught']}")
                    self.logger.exception(f"Putting the tasks back into active queue for retry")

                    for reject_tid in thr_to_sublist_map[thr_id]:
                        self.task_dict["active"].put(reject_tid)

                    print(f"Pausing for 20 seconds...")
                    self.pause_q_consume = True
                    time.sleep(20)
                    continue
                else:
                    self.pause_q_consume = False

                for tid in status_thing:
                    task_obj = status_thing[tid]

                    if "result" in task_obj:
                        res = self.fx_ser.deserialize(task_obj['result'])

                        # Decrement number of tasks being extracted!
                        self.num_extracting_tasks -= 1

                        if "family_batch" in res:
                            family_batch = res["family_batch"]
                            unpacked_metadata = self.unpack_returned_family_batch(family_batch)

                            # print(unpacked_metadata)

                            # TODO: make this a regular feature for matio (so this code isn't necessary...)
                            if 'event' in unpacked_metadata:
                                family_batch = unpacked_metadata['event']['family_batch']
                                unpacked_metadata = family_batch.to_dict()
                                unpacked_metadata['dataset'] = self.dataset_mdata

                            if self.prefetch_remote:
                                total_family_size = 0
                                for family in family_batch.families:
                                    # family['metadata']["t_funcx_req_received"] = time.time()
                                    total_family_size += self.prefetcher.get_family_size(family.to_dict())

                                self.prefetcher.orch_unextracted_bytes -= total_family_size

                            for family in unpacked_metadata['families']:
                                family['metadata']["t_funcx_req_received"] = time.time()

                            json_mdata = json.dumps(unpacked_metadata, cls=NumpyEncoder)

                            result_size = sys.getsizeof(json_mdata)

                            self.tot_fx_poll_result_size += result_size

                            if result_size > self.max_result_size:
                                self.max_result_size = result_size

                            try:
                                self.to_validate_q.put({"Id": str(self.file_count),
                                                        "MessageBody": json_mdata})
                                self.file_count += 1
                            except TypeError as e1:
                                self.logger.exception(f"Type error: {e1}")
                                type_errors += 1
                                self.logger.exception(f"Total type errors: {type_errors}")
                        else:
                            self.logger.error(f"[Poller]: \"family_batch\" not in res!")

                        self.success_returns += 1

                        # Leave this in. Great for debugging and we have the IO for it.
                        self.logger.debug(f"Received response: {res}")

                        if type(res) is not dict:
                            self.logger.exception(f"Res is not dict: {res}")
                            continue

                        elif 'transfer_time' in res:
                            if res['transfer_time'] > 0:
                                self.t_transfer += res['transfer_time']
                                self.n_fams_transferred += 1
                                print(f"Avg. Transfer time: {self.t_transfer/self.n_fams_transferred}")

                        # This just means fixing Google Drive extractors...
                        elif 'trans_time' in res:
                            if res['trans_time'] > 0:
                                self.t_transfer += res['trans_time']
                                self.n_fams_transferred += 1
                                print(f"Avg. Transfer time: {self.t_transfer/self.n_fams_transferred}")

                    elif "exception" in task_obj:
                        exc = self.fx_ser.deserialize(task_obj['exception'])
                        try:
                            exc.reraise()
                        except ModuleNotFoundError:
                            mod_not_found += 1
                            self.logger.exception(f"Num. ModuleNotFound: {mod_not_found}")

                        except Exception as e:
                            self.logger.exception(f"Caught exception: {e}")
                            self.logger.exception("Continuing!")
                            pass

                        self.failed_returns += 1
                        self.logger.exception(f"Exception caught: {exc}")

                    else:
                        self.task_dict["active"].put(tid)

            t_since_last_poll = time.time() - t_last_poll
            t_poll_gap = self.poll_gap_s - t_since_last_poll

            if t_poll_gap > 0:
                time.sleep(t_poll_gap)
