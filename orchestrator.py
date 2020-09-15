
import os
import json
import time
import boto3
import logging
import threading
import numpy as np

from queue import Queue
from status_checks import get_crawl_status
from funcx.serialize import FuncXSerializer
from xtract_sdk.packagers import Family, FamilyBatch
from extractors.utils.batch_utils import remote_extract_batch, remote_poll_batch
from extractors import xtract_images, xtract_tabular, xtract_matio, xtract_keyword


# TODO: Put this dang numpy encoder in 1 place and import it everywhere it's needed.
class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)


# TODO: get rid of clutter: nondescriptive print statements, unused variables, unused functions.
#  TODO: Copied and pasted bits of code.

class Orchestrator:
    """ ADD HIGH OVERVIEW COMMENT HERE. """

    # TODO: Make source_eid and dest_eid default to None for the HTTPS case?
    def __init__(self, crawl_id, headers, funcx_eid,
                 mdata_store_path, source_eid=None, dest_eid=None, gdrive_token=None,
                 logging_level='debug', instance_id=None, extractor_finder='gdrive'):

        self.t_crawl_start = time.time()
        self.t_get_families = 0
        self.t_send_batch = 0

        self.extractor_finder = extractor_finder

        self.funcx_eid = funcx_eid
        self.func_dict = {"image": xtract_images.ImageExtractor(),
                          "images": xtract_images.ImageExtractor(),
                          "tabular": xtract_tabular.TabularExtractor(),
                          "text": xtract_keyword.KeywordExtractor(),
                          "matio": xtract_matio.MatioExtractor()}  # TODO: cleanup extra imgs

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

        self.logging_level = logging_level

        self.logger = logging.getLogger(__name__)
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.DEBUG)
        self.instance_id = instance_id

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
            self.xtract_queue_url = xtract_queue["QueueUrl"]  # TODO: ZOA -- this does nothing currently(but keep it in)
            self.validation_queue_url = validation_queue["QueueUrl"]  # TODO: sends finished metadata to validation service.
                                                                    # TODO: have third 'crawl' queue (elsewhere) that just holds all metadata from before this point
                                                                    # TODO:   aka the files/metadata we want to pull down and process.
        else:
            raise ConnectionError("Received non-200 status from SQS!")

        response = self.client.get_queue_url(
            QueueName=f'crawl_{self.crawl_id}',
            QueueOwnerAWSAccountId=os.environ["aws_account"]
        )

        self.crawl_queue = response["QueueUrl"]

        self.families_to_process = Queue()
        self.to_validate_q = Queue()

        # TODO: file left here for someone named Will. Remove it.
        self.will_file = open("will.mdata", "w")

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

    # TODO: Add a preliminary loop-polling 'status check' on the endpoint that returns a noop
    # TODO: And do it here in the init. Should print something like "endpoint online!" or return error if not.
    def validate_enqueue_loop(self, thr_id):

        print("[VALIDATE] In validation enqueue loop!")
        while True:
            insertables = []
            print(f"[VALIDATE] Length of validation queue: {self.to_validate_q.qsize()}")

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
            while (not self.to_validate_q.empty() and current_batch < 10):
                item_to_add = self.to_validate_q.get()
                insertables.append(item_to_add)
                current_batch += 1
            print(f"[VALIDATE] Current insertables: {insertables}")

            # try:
            ta = time.time()
            response = self.client.send_message_batch(QueueUrl=self.validation_queue_url,
                                                          Entries=insertables)

            # print(f"[VALIDATE] SQS response on insert: {response}")
            tb = time.time()

            self.t_send_batch += tb-ta

            # except Exception as e:  # TODO: too vague
            #     print(f"WAS UNABLE TO PROPERLY CONNECT to SQS QUEUE: {e}")

    def send_families_loop(self):
        # TODO: Zoa -- 'families' are collections of files and metadata.

        self.send_status = "RUNNING"

        while True:
            # TODO: Make sure this comes in via the notebook.
            fx_ep = "68bade94-bf58-4a7a-bfeb-9c6a61fa5443"

            print("Communicating with SQS to pull down new_files")
            family_list = []
            # Now keeping filling our list of families until it is empty.
            while len(family_list) < self.batch_size and not self.families_to_process.empty():
                family_list.append(self.families_to_process.get())

            print("Exited family loop!")
            print(f"Length of family_list: {len(family_list)}")

            if len(family_list) == 0:
                print("Length of new families is 0!")
                # Here we check if the crawl is complete. If so, then we can start the teardown checks.
                status_dict = get_crawl_status(self.crawl_id)

                print(f"Checking if crawl_status is SUCCEEDED or FAILED!: {status_dict['crawl_status']}")
                if status_dict['crawl_status'] in ["SUCCEEDED", "FAILED"]:
                    # family_list = self.get_next_families()
                    print(f"[SEND] Is idle?: {self.get_families_status}")
                    print(f"[SEND] Empty families to process?: {self.families_to_process.empty()}")
                    if self.families_to_process.empty() and self.get_families_status == "IDLE":  # Checking second time due to narrow race condition.
                        self.send_status = "SUCCEEDED"
                        print("Queue still empty -- terminating!")
                        return  # this should terminate thread, because there is nothing to process and queue empty
                    else:  # Something snuck in during the race condition... process it!
                        print("Discovered final output despite crawl termination. Processing...")

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
                    raise ValueError("Incorrect extractor_finder arg.")

                # TODO: add the decompression work and the hdf5/netcdf extractors!
                if extr_code is None or extr_code == 'hierarch' or extr_code == 'compressed':  # TODO: Any bookkeeping we need to do here?
                    continue

                extractor = self.func_dict[extr_code]
                ex_func_id = extractor.func_id

                # Putting into family batch -- we use funcX batching now, but no use rewriting...
                family_batch = FamilyBatch()
                family_batch.add_family(xtr_fam_obj)

                if d_type == "gdrive":
                    self.current_batch.append({"event": {"family_batch": family_batch,   # TODO: TYLER RIGHT HERE.
                                                         "creds": self.gdrive_token[0]},
                                               "func_id": ex_func_id})
                elif d_type == "HTTPS":
                    self.current_batch.append({"event": {"family_batch": family_batch.to_dict()},
                                               "func_id": ex_func_id})

                try:
                    task_ids = remote_extract_batch(self.current_batch, ep_id=fx_ep, headers=self.fx_headers)
                except Exception as e:
                    print(f"[SEND] Caught exception here: {e}")
                for task_id in task_ids:
                    self.task_dict["active"].put(task_id)

                print(f"Active task queue (local) size: {self.task_dict['active'].qsize()}")

                # Empty the batch! Everything in here has been sent :)
                self.current_batch = []
            # time.sleep(1)  # TODO: no.

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

                    # print(f"Messages in message body: {message_body}")

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
        # assert(self.batch_size==1 or self.batch_size%10==0)
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
                # p_empty = time.time()
                # print(p_empty)
                time.sleep(0.25)
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

            # TODO: why is this necessary?
            if status_thing is None:
                continue

            for tid in status_thing:

                if "result" in status_thing[tid]:
                    res = self.fx_ser.deserialize(status_thing[tid]['result'])

                    if "family_batch" in res:
                        family_batch = res["family_batch"]
                        unpacked_metadata = self.unpack_returned_family_batch(family_batch)
                        # print(type(unpacked_mdata))

                        # TODO: make this a regular feature for matio
                        # print(f"[CHECK HERE] {unpacked_metadata}")
                        if 'event' in unpacked_metadata:
                            family_batch = unpacked_metadata['event']['family_batch']
                            unpacked_metadata = family_batch.to_dict()

                        print(f"UNPACKED METADATA: {unpacked_metadata}")

                        try:
                            self.to_validate_q.put({"Id": str(self.file_count),
                                                    "MessageBody": json.dumps(unpacked_metadata, cls=NumpyEncoder)})
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
