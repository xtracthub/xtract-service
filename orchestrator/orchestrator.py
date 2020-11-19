
import os
import json
import time
import boto3
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


class Orchestrator:

    def __init__(self, crawl_id, headers, funcx_eid,
                 mdata_store_path, source_eid=None, dest_eid=None, gdrive_token=None,
                 extractor_finder='gdrive', prefetch_remote=False,
                 data_prefetch_path=None, dataset_mdata=None):

        self.crawl_id = crawl_id

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

        self.poll_gap_s = 5
        self.num_send_reqs = 0
        self.num_poll_reqs = 0
        self.get_families_status = "STARTING"

        self.task_dict = {"active": Queue(), "pending": Queue(), "failed": Queue()}

        # Batch size we use to send tasks to funcx.
        self.fx_batch_size = 100

        # Number (current and max) of number of tasks sent to funcX for extraction.
        self.max_extracting_tasks = 1000
        self.num_extracting_tasks = 0

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

        self.headers = headers
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
        self.logger.setLevel(logging.DEBUG)

        # This creates the extraction and validation queues on the Simple Queue Service.
        self.sqs_base_url = "https://sqs.us-east-1.amazonaws.com/576668000072/"  # TODO: env var.
        self.client = boto3.client('sqs',
                                   aws_access_key_id=os.environ["aws_access"],
                                   aws_secret_access_key=os.environ["aws_secret"], region_name='us-east-1')
        print(f"Creating queue for crawl_id: {self.crawl_id}")
        xtract_queue = self.client.create_queue(QueueName=f"xtract_{str(self.crawl_id)}")
        validation_queue = self.client.create_queue(QueueName=f"validate_{str(self.crawl_id)}")

        if xtract_queue["ResponseMetadata"]["HTTPStatusCode"] == 200 and \
                validation_queue["ResponseMetadata"]["HTTPStatusCode"] == 200:
            self.xtract_queue_url = xtract_queue["QueueUrl"]
            self.validation_queue_url = validation_queue["QueueUrl"]

        else:
            raise ConnectionError("Received non-200 status from SQS!")

        # Adjust the queue to fit the 'prefetch' mode. One extra queue (transferred) if prefetch == True.
        q_prefix = "crawl"

        response = self.client.get_queue_url(
            QueueName=f'{q_prefix}_{self.crawl_id}',
            QueueOwnerAWSAccountId=os.environ["aws_account"]
        )

        self.crawl_queue = response["QueueUrl"]

        self.families_to_process = Queue()
        self.to_validate_q = Queue()

        self.sqs_push_threads = {}
        self.thr_ls = []
        self.commit_threads = 1
        self.get_family_threads = 20

        for i in range(0, self.commit_threads):
            thr = threading.Thread(target=self.validate_enqueue_loop, args=(i,))
            self.thr_ls.append(thr)
            thr.start()
            self.sqs_push_threads[i] = True
        print(f"Successfully started {len(self.sqs_push_threads)} SQS push threads!")

        for i in range(0, self.get_family_threads):
            print(f"Attempting to start get_next_families() as its own thread... ")
            consumer_thr = threading.Thread(target=self.get_next_families_loop, args=())
            consumer_thr.start()
            print(f"Successfully started the get_next_families() thread number {i} ")

        # If configured to be a data 'prefetch' scenario, then we want to go get it.

        print(f"ARE WE PREFETCHING? {self.prefetch_remote}")
        if self.prefetch_remote:
            self.prefetcher = GlobusPrefetcher(transfer_token=self.headers['Transfer'],
                                               crawl_id=self.crawl_id,
                                               data_source=self.source_endpoint,
                                               data_dest=self.dest_endpoint,
                                               data_path=self.data_prefetch_path,
                                               max_gb=5)  # TODO: bounce this out.

            prefetch_thread = threading.Thread(target=self.prefetcher.main_poller_loop, args=())
            prefetch_thread.start()

    def validate_enqueue_loop(self, thr_id):

        self.logger.debug("[VALIDATE] In validation enqueue loop!")
        while True:
            insertables = []

            self.logger.debug(f"[VALIDATE] Length of validation queue: {self.to_validate_q.qsize()}")
            print(f"[GET] Elapsed Send Batch time: {self.t_send_batch}")
            print(f"[GET] Elapsed Extract time: {time.time() - self.t_crawl_start}")

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
                        print(f"Thread {thr_id} is terminating!")
                        return 0

                print("[Validate]: sleeping for 5 seconds... ")
                time.sleep(5)
                continue

            self.sqs_push_threads[thr_id] = "ACTIVE"

            # Remove up to n elements from queue, where n is current_batch.
            current_batch = 1
            while not self.to_validate_q.empty() and current_batch < 10:
                item_to_add = self.to_validate_q.get()
                insertables.append(item_to_add)
                current_batch += 1
            print(f"[VALIDATE] Current insertables: {str(insertables)[0:50]} . . .")

            try:
                ta = time.time()
                self.client.send_message_batch(QueueUrl=self.validation_queue_url,
                                               Entries=insertables)
                tb = time.time()
                self.t_send_batch += tb-ta

            except Exception as e:  # TODO: too vague
                print(f"WAS UNABLE TO PROPERLY CONNECT to SQS QUEUE: {e}")

    def send_families_loop(self):

        self.send_status = "RUNNING"

        while True:

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
                            print("[SEND] Queue still empty -- terminating!")

                            # this should terminate thread, because there is nothing to process and queue empty
                            return

                    else:  # Something snuck in during the race condition... process it!
                        print("[SEND] Discovered final output despite crawl termination. Processing...")
                        time.sleep(0.5)  # This is a multi-minute task and only is reached due to starvation.

            # Cast list to FamilyBatch
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
                family_batch = FamilyBatch()
                family_batch.add_family(xtr_fam_obj)

                if d_type == "gdrive":
                    self.current_batch.append({"event": {"family_batch": family_batch,
                                                         "creds": self.gdrive_token[0]},
                                               "func_id": ex_func_id})
                elif d_type == "HTTPS":
                    self.current_batch.append({"event": {"family_batch": family_batch.to_dict()},
                                               "func_id": ex_func_id})

            if len(self.current_batch) > 0:
                task_ids = remote_extract_batch(self.current_batch, ep_id=self.funcx_eid, headers=self.fx_headers)
                self.num_send_reqs += 1
                self.pre_launch_counter -= len(self.current_batch)
                print(f"Total send requests: {self.num_send_reqs}")

                if type(task_ids) is dict:
                    print(f"Caught funcX error: {task_ids['exception_caught']}")
                    print(f"Putting the tasks back into active queue for retry")
    
                    for reject_fam_batch in self.current_batch:

                        fam_batch_dict = reject_fam_batch['event']['family_batch']

                        for reject_fam in fam_batch_dict['families']:
                            self.families_to_process.put(json.dumps(reject_fam))
    
                    print(f"Pausing for 10 seconds...")
                    time.sleep(10)
                    continue

                for task_id in task_ids:
                    self.task_dict["active"].put(task_id)
                    self.num_extracting_tasks += 1

                # Empty the batch! Everything in here has been sent :)
                self.current_batch = []

    def launch_poll(self):
        print("POLLER IS STARTING!!!!")
        po_thr = threading.Thread(target=self.poll_extractions_and_stats, args=())
        po_thr.start()

    def get_next_families_loop(self):
        """

        get_next_families() will keep polling SQS queue containing all of the crawled data. This should generally be
        read as its own thread, and terminate when the orchestrator's POLLER is completed
        (because, well, that means everything is finished)

        If prefetching is turned OFF, then families will be placed directly into families_to_process queue.

        If prefetching is turned ON, then families will be placed directly into families_to_prefetch queue.

        NOTE: *n* current threads of this loop!

        """

        final_kill_check = False

        while True:

            # Do some queue pause checks (if too many current uncompleted tasks)
            if self.num_extracting_tasks > self.max_extracting_tasks:
                self.pause_q_consume = True
                print(f"[GET] Num. active tasks ({self.num_extracting_tasks}) "
                      f"above threshold. Pausing SQS consumption...")

            if self.pause_q_consume:
                print("[GET] Pausing pull of new families. Sleeping for 20 seconds...")
                time.sleep(20)
                continue

            # Step 1. Get ceiling(batch_size/10) messages down from queue.
            # Otherwise, we can just pluck straight from the sqs crawl queue
            sqs_response = self.client.receive_message(  # TODO: properly try/except this block.
                QueueUrl=self.crawl_queue,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=1)

            del_list = []
            found_messages = False
            deleted_messages = False

            if ("Messages" in sqs_response) and (len(sqs_response["Messages"]) > 0):
                self.get_families_status = "ACTIVE"
                for message in sqs_response["Messages"]:
                    self.num_families_fetched += 1
                    message_body = message["Body"]
                    # print(f"Message: {message_body}")

                    # IF WE ARE NOT PREFETCHING, place directly into processing queue.
                    if not self.prefetch_remote:
                        self.families_to_process.put(message_body)

                    # OTHERWISE, place into prefetching queue.
                    else:
                        self.prefetcher.next_prefetch_queue.put(message_body)

                    del_list.append({'ReceiptHandle': message["ReceiptHandle"],
                                     'Id': message["MessageId"]})
                found_messages = True

            # If we didn't get messages last time around, and the crawl is over.
            if final_kill_check:
                # Make sure no final messages squeaked in...
                if "Messages" not in sqs_response or len(sqs_response["Messages"]) == 0:

                    # If GET about to die, then the next step is that prefetcher should die.
                    # TODO: Technically a race condition here since there are n concurrent threads.
                    # TODO: Should wait until this is the LAST such thread.
                    self.prefetcher.kill_prefetcher = True

                    print(f"[GET] Crawl successful and no messages in queue to get...")
                    print("[GET] Terminating...")
                    return
                else:
                    final_kill_check = False

            # Step 2. Delete the messages from SQS.
            if len(del_list) > 0:
                self.client.delete_message_batch(
                    QueueUrl=self.crawl_queue,
                    Entries=del_list)

            if not deleted_messages and not found_messages:
                print("[GET] FOUND NO NEW MESSAGES. SLEEPING THEN DOWNGRADING TO IDLE...")
                self.get_families_status = "IDLE"

            if "Messages" not in sqs_response or len(sqs_response["Messages"]) == 0:
                status_dict = get_crawl_status(self.crawl_id)

                if status_dict['crawl_status'] in ["SUCCEEDED", "FAILED"]:
                    final_kill_check = True

            time.sleep(2)

    def launch_extract(self):
        ex_thr = threading.Thread(target=self.send_families_loop, args=())
        ex_thr.start()

    def unpack_returned_family_batch(self, family_batch):
        fam_batch_dict = family_batch.to_dict()
        return fam_batch_dict

    def print_stats(self):
        # STAT CHECK: if we haven't updated stats in 5 seconds, then we update.
        cur_time = time.time()
        if cur_time - self.last_checked > 5:
            total_bytes = self.prefetcher.orch_unextracted_bytes + \
                          self.prefetcher.bytes_pf_completed + \
                          self.prefetcher.bytes_pf_in_flight

            print("********** EXTRACTION STATISTICS **********")

            print("Phase 1: Fetch crawl tasks from SQS...")
            print(f"\tTotal messages pulled: {self.num_families_fetched}")
            files_fetched_per_second = self.num_families_fetched / (cur_time - self.get_families_start_time)
            print(f"\tSQS messages per second: {files_fetched_per_second}\n")

            print("Phase 2: prefetch")
            print(f"\t Eff. dir size (GB): {total_bytes / 1024 / 1024 / 1024} | "
                  f"\tNew Pulled Messages {self.prefetcher.pf_msgs_pulled_since_last_batch}")
            print(f"\t N Transfer Tasks: {self.prefetcher.num_transfers}")
            print(f"\t Families transferred per second: "
                  f"{self.prefetcher.num_families_transferred / (cur_time - self.get_families_start_time)} \n")

            print("Phase 3: pre-extraction")
            print(f"\tpre-funcX messages: {self.pre_launch_counter + self.prefetcher.orch_reader_q.qsize()}")
            print(f"\tin-funcX messages: {self.num_extracting_tasks}\n")

            print("Phase 4: polling")
            print(f"\t Successes: {self.success_returns}")
            print(f"\t Failures: {self.failed_returns}\n")

            n_pulled = self.prefetcher.next_prefetch_queue.qsize()
            n_pf_batch = self.prefetcher.pf_msgs_pulled_since_last_batch
            n_pf = self.prefetcher.num_families_mid_transfer
            n_awaiting_fx = self.pre_launch_counter + self.prefetcher.orch_reader_q.qsize()
            n_in_fx = self.num_extracting_tasks
            n_success = self.success_returns

            total_tracked = n_success + n_in_fx + n_awaiting_fx + n_pf + n_pf_batch + n_pulled

            print(f"\n** TASK LOCATION BREAKDOWN **\n"
                  f"--Pulled: {n_pulled}\n"
                  f"--In pf batching: {n_pf_batch}\n"
                  f"--Prefetching: {n_pf}\n" 
                  f"--Awaiting extraction: {n_awaiting_fx}\n"
                  f"--In extraction: {n_in_fx}\n"
                  f"--Completed: {n_success}\n"
                  f"\n-- Fetch-Track Delta: {self.num_families_fetched - total_tracked}")

            self.last_checked = time.time()

    def poll_batch_chunks(self, sublist, headers):
        status_thing = remote_poll_batch(sublist, headers)
        self.num_poll_reqs += 1
        self.status_things.put(status_thing)
        time.sleep(1)  # TODO: MIGHT WANT TO TAKE THIS OUT. JUST SEEING IF THIS FIXES funcX SCALE-UP.
        return

    def poll_extractions_and_stats(self):
        # success_returns = 0
        mod_not_found = 0
        # failed_returns = 0
        type_errors = 0

        self.poll_status = "RUNNING"

        while True:

            # This will attempt to print stats on every iteration.
            #   Note: internally, this will only execute max every 5 seconds.
            self.print_stats()

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
            print(f"[POLL] Size active queue: {num_elem}")
            tids_to_poll = []  # the batch

            for i in range(0, num_elem):

                ex_id = self.task_dict["active"].get()
                tids_to_poll.append(ex_id)

            t_last_poll = time.time()

            # Send off task_ids to poll, retrieve a bunch of statuses.
            tid_sublists = create_list_chunks(tids_to_poll, 100)  # TODO: should definitely be an arg.

            polling_threads = []

            for tid_sublist in tid_sublists:

                # If there's an actual thing in the list...
                if len(tid_sublist) > 0:

                    thr = threading.Thread(target=self.poll_batch_chunks, args=(tid_sublist, self.fx_headers))
                    thr.start()
                    polling_threads.append(thr)

            # Now we should wait for our fan-out threads to fan-in
            for thread in polling_threads:
                thread.join()

            print(f"Total poll requests: {self.num_poll_reqs}")

            while not self.status_things.empty():

                status_thing = self.status_things.get()

                print(f"Status thing: {status_thing}")

                if "exception_caught" in status_thing:
                    print(f"Caught funcX error: {status_thing['exception_caught']}")
                    print(f"Putting the tasks back into active queue for retry")

                    for reject_tid in tids_to_poll:
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

                            # TODO: make this a regular feature for matio (so this code isn't necessary...)
                            if 'event' in unpacked_metadata:
                                family_batch = unpacked_metadata['event']['family_batch']
                                unpacked_metadata = family_batch.to_dict()
                                unpacked_metadata['dataset'] = self.dataset_mdata

                            print(f"[DEBUG] Unpacked metadata: {unpacked_metadata}")

                            if self.prefetch_remote:
                                total_family_size = 0
                                for family in family_batch.families:
                                    total_family_size += self.prefetcher.get_family_size(family.to_dict())

                                self.prefetcher.orch_unextracted_bytes -= total_family_size

                            json_mdata = json.dumps(unpacked_metadata, cls=NumpyEncoder)

                            try:
                                self.to_validate_q.put({"Id": str(self.file_count),
                                                        "MessageBody": json_mdata})
                                self.file_count += 1
                            except TypeError as e1:
                                print(f"Type error: {e1}")
                                type_errors += 1
                                print(f"Total type errors: {type_errors}")
                        else:
                            print(f"[Poller]: \"family_batch\" not in res!")

                        self.success_returns += 1
                        self.logger.debug(f"Success Counter: {self.success_returns}")
                        self.logger.debug(f"Failure Counter: {self.failed_returns}")

                        # Leave this in. Great for debugging and we have the IO for it.
                        self.logger.debug(f"Received response: {res}")

                        if type(res) is not dict:
                            print(f"Res is not dict: {res}")
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
                            print(f"Num. ModuleNotFound: {mod_not_found}")

                        except Exception as e:
                            print(f"Caught exception: {e}")
                            print("Continuing!")
                            pass

                        self.failed_returns += 1
                        print(f"Exception caught: {exc}")

                    else:
                        self.task_dict["active"].put(tid)

            t_since_last_poll = time.time() - t_last_poll
            t_poll_gap = self.poll_gap_s - t_since_last_poll

            if t_poll_gap > 0:
                time.sleep(t_poll_gap)
