
import os
import json
import time
import boto3
import logging
import threading

from queue import Queue
from extractors.utils.mappings import mapping
from utils.fx_utils import invoke_solo_function
from utils.encoders import NumpyEncoder
from status_checks import get_crawl_status
from funcx.serialize import FuncXSerializer
from xtract_sdk.packagers import Family, FamilyBatch
from extractors.utils.batch_utils import remote_extract_batch, remote_poll_batch
from extractors import xtract_images, xtract_tabular, xtract_matio, xtract_keyword


class Orchestrator:
    """
    A class used to ...

    Attributes
    ----------
    says_str : str
        a formatted string to print out what the animal says
    name : str
        the name of the animal
    sound : str
        the sound that the animal makes
    num_legs : int
        the number of legs the animal has (default 4)

    Methods
    -------
    enqueue_loop(thr_id)
        (action)
    send_families_loop()

    launch_poll()

    get_next_families_loop()

    launch_extract()

    unpack_returned_family_batch(family_batch)

    poll_responses()
    """

    # TODO 1: Make source_eid and dest_eid default to None for the HTTPS case?
    # TODO 2: prefetch decision should eventually be decided by the system on file-by-file basis.
    def __init__(self, crawl_id, headers, funcx_eid,
                 mdata_store_path, source_eid=None, dest_eid=None, gdrive_token=None,
                 logging_level='debug', instance_id=None, extractor_finder='gdrive', prefetch_remote=False,
                 data_prefetch_path=None):

        self.t_crawl_start = time.time()
        self.t_get_families = 0
        self.t_send_batch = 0
        self.t_transfer = 0
        self.n_fams_transferred = 0
        self.prefetch_remote = prefetch_remote
        self.data_prefetch_path = data_prefetch_path

        self.extractor_finder = extractor_finder
        self.pf_task_id = None

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

        self.poll_gap_s = 5
        self.get_families_status = "STARTING"

        self.task_dict = {"active": Queue(), "pending": Queue(), "failed": Queue()}

        self.batch_size = 100
        self.crawl_id = crawl_id

        self.file_count = 0
        self.current_batch = []

        self.mdata_store_path = mdata_store_path

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
        q_prefix = 'transferred' if prefetch_remote else 'crawl'

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
        for i in range(0, self.commit_threads):
            thr = threading.Thread(target=self.validate_enqueue_loop, args=(i,))
            self.thr_ls.append(thr)
            thr.start()
            self.sqs_push_threads[i] = True
        print(f"Successfully started {len(self.sqs_push_threads)} SQS push threads!")

        print(f"Attempting to start get_next_families() as its own thread... ")
        consumer_thr = threading.Thread(target=self.get_next_families_loop, args=())
        consumer_thr.start()
        print("Successfully started the get_next_families() thread! ")

        # If configured to be a data 'prefetch' scenario, then we want to go get it.
        if prefetch_remote:
            self.launch_prefetcher()

        # Do the startup checks to ensure that all funcX endpoint are online.
        self.startup_checks()

    def startup_checks(self):
        # Need to use container on all 'singularity' or 'docker' instances. Otherwise 'RAW' is okay.
        pass

    def launch_prefetcher(self):
        pf_func = mapping['prefetcher::midway2']['func_uuid']

        event = {'transfer_token': self.headers['Transfer'],
                 'crawl_id': self.crawl_id,
                 'data_source': self.source_endpoint,
                 'data_dest': self.dest_endpoint,
                 'data_path': self.data_prefetch_path,
                 'max_gb': 50}

        fx_ep_id = "17214422-4570-4891-9831-2212614d04fa"  # TODO: This should not be hardcoded.

        task_uuid = invoke_solo_function(event=event, fx_eid=fx_ep_id, headers=self.fx_headers, func_id=pf_func)

        print(f"Headers: {self.fx_headers}")
        self.pf_task_id = task_uuid
        self.prefetch_status = "PROCESSING"

        pf_poll_th = threading.Thread(target=self.poll_prefetcher, args=())
        pf_poll_th.start()

    def poll_prefetcher(self):
        while True:

            print(self.pf_task_id)

            status_thing = remote_poll_batch(task_ids=[self.pf_task_id], headers=self.fx_headers)
            print(f"Prefetcher status: {status_thing}")

            print(type(status_thing))

            if "exception_caught" in status_thing:
                print(f"Caught funcX error: {status_thing['exception_caught']}")
                print(f"Putting the tasks back into active queue for retry")
            elif 'status' in status_thing[self.pf_task_id] and \
                    status_thing[self.pf_task_id]['status'].lower() == 'success':
                print("HEY")
                self.prefetch_status = "SUCCEEDED"
                print(f"[PREFETCH-POLL] Success signal received. Terminating...")
                return
            time.sleep(2)

    # TODO: Add a preliminary loop-polling 'status check' on the endpoint that returns a noop
    # TODO: And do it here in the init. Should print something like "endpoint online!" or return error if not.
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
            # TODO: Make sure this comes in via the notebook.
            fx_ep = "71509922-996f-4559-b488-4588f06f0925"

            family_list = []
            # Now keeping filling our list of families until it is empty.
            while len(family_list) < self.batch_size and not self.families_to_process.empty():
                family_list.append(self.families_to_process.get())

            if len(family_list) == 0:
                # Here we check if the crawl is complete. If so, then we can start the teardown checks.
                status_dict = get_crawl_status(self.crawl_id)

                if status_dict['crawl_status'] in ["SUCCEEDED", "FAILED"]:

                    # Checking second time due to narrow race condition.
                    # TODO: Add a prefetcher condition.
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
                    self.current_batch.append({"event": {"family_batch": family_batch,  # TODO: Tyler, check this!
                                                         "creds": self.gdrive_token[0]},
                                               "func_id": ex_func_id})
                elif d_type == "HTTPS":
                    self.current_batch.append({"event": {"family_batch": family_batch.to_dict()},
                                               "func_id": ex_func_id})

                # try:
                print(f"Current batch: {self.current_batch}")
                task_ids = remote_extract_batch(self.current_batch, ep_id=fx_ep, headers=self.fx_headers)
                # except Exception as e:
                #     print(f"[SEND] Caught exception here: {e}")

                if type(task_ids) is dict:
                    print(f"Caught funcX error: {task_ids['exception_caught']}")
                    print(f"Putting the tasks back into active queue for retry")

                    for reject_fam in family_batch.families:
                        self.families_to_process.put(json.dumps(reject_fam.to_dict()))

                    print(f"Pausing for 10 seconds...")
                    time.sleep(10)
                    continue

                # print(f"Task IDs: {task_id}")

                for task_id in task_ids:
                    self.task_dict["active"].put(task_id)

                # Empty the batch! Everything in here has been sent :)
                self.current_batch = []

    def launch_poll(self):
        print("POLLER IS STARTING!!!!")
        po_thr = threading.Thread(target=self.poll_responses, args=())
        po_thr.start()

    # TODO: Smarter batching that takes into account file size.
    def get_next_families_loop(self):

        while True:
            # Step 1. Get ceiling(batch_size/10) messages down from queue.
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
                    message_body = message["Body"]

                    self.families_to_process.put(message_body)

                    del_list.append({'ReceiptHandle': message["ReceiptHandle"],
                                     'Id': message["MessageId"]})
                found_messages = True

            # Step 2. Delete the messages from SQS.
            if len(del_list) > 0:
                response = self.client.delete_message_batch(
                    QueueUrl=self.crawl_queue,
                    Entries=del_list)
                # TODO: Do the 200 check. Weird data format.
                # print(f"Delete response: {response}")
                # if response["HTTPStatusCode"] is not 200:
                #     raise ConnectionError("Could not delete messages from queue!")
                delete_messages = True

            # Simple because if the poller is done, then there's no point pulling down any work.
            if self.poll_status == "COMPLETED":
                print(f"[GET] Terminating thread to get more work.")
                print(f"[GET] Elapsed Send Batch time: {self.t_send_batch}")
                print(f"[GET] Elapsed Extract time: {time.time() - self.t_crawl_start}")

                return  # terminate the thread.

            # print("ARE WE HERE????")
            if not deleted_messages and not found_messages:
                print("FOUND NO NEW MESSAGES. SLEEPING THEN DOWNGRADING TO IDLE...")
                self.get_families_status = "IDLE"

            time.sleep(2)

    def launch_extract(self):
        # TODO: will soon need more than just 1 thread here.
        ex_thr = threading.Thread(target=self.send_families_loop, args=())
        ex_thr.start()

    def unpack_returned_family_batch(self, family_batch):
        fam_batch_dict = family_batch.to_dict()
        return fam_batch_dict

    def poll_responses(self):
        success_returns = 0
        mod_not_found = 0
        failed_returns = 0
        type_errors = 0

        self.poll_status = "RUNNING"

        while True:
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
            status_thing = remote_poll_batch(task_ids=tids_to_poll, headers=self.fx_headers)

            if "exception_caught" in status_thing:
                print(f"Caught funcX error: {status_thing['exception_caught']}")
                print(f"Putting the tasks back into active queue for retry")

                for reject_tid in tids_to_poll:
                    self.task_dict["active"].put(reject_tid)

                print(f"Pausing for 10 seconds...")
                time.sleep(10)
                continue

            for tid in status_thing:
                if "result" in status_thing[tid]:
                    res = self.fx_ser.deserialize(status_thing[tid]['result'])

                    if "family_batch" in res:
                        family_batch = res["family_batch"]
                        unpacked_metadata = self.unpack_returned_family_batch(family_batch)

                        # TODO: make this a regular feature for matio (so this code isn't necessary...)
                        if 'event' in unpacked_metadata:
                            family_batch = unpacked_metadata['event']['family_batch']
                            unpacked_metadata = family_batch.to_dict()

                        print(unpacked_metadata)

                        json_mdata = json.dumps(unpacked_metadata, cls=NumpyEncoder)
                        print(json_mdata)

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

                    success_returns += 1
                    self.logger.debug(f"Success Counter: {success_returns}")
                    self.logger.debug(f"Failure Counter: {failed_returns}")
                    self.logger.debug(f"Received response: {res}")

                    # TODO: return group_ids so that we may mark accordingly.
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

                elif "exception" in status_thing[tid]:
                    exc = self.fx_ser.deserialize(status_thing[tid]['exception'])
                    try:
                        exc.reraise()
                    except ModuleNotFoundError as e:
                        mod_not_found += 1
                        print(f"Num. ModuleNotFound: {mod_not_found}")

                    except Exception as e:
                        print(f"Caught exception: {e}")
                        print("Continuing!")
                        pass

                    failed_returns += 1
                    print(f"Exception caught: {exc}")

                else:
                    self.task_dict["active"].put(tid)

            t_since_last_poll = time.time() - t_last_poll
            t_poll_gap = self.poll_gap_s - t_since_last_poll

            if t_poll_gap > 0:
                time.sleep(t_poll_gap)
