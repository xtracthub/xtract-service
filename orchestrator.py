
import os
import json
import time
import boto3
import logging
import threading
import numpy as np

from queue import Queue
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


class Orchestrator:
    # TODO: Make source_eid and dest_eid default to None for the HTTPS case?
    def __init__(self, crawl_id, headers, funcx_eid,
                 mdata_store_path, source_eid=None, dest_eid=None, gdrive_token=None,
                 logging_level='debug', instance_id=None, extractor_finder='gdrive', fx_ep_list=None):

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
        self.fx_ep_list = fx_ep_list

        self.task_dict = {"active": Queue(), "pending": Queue(), "failed": Queue()}

        self.batch_size = 1
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

        response = self.client.get_queue_url(
            QueueName=f'crawl_{self.crawl_id}',
            QueueOwnerAWSAccountId=os.environ["aws_account"]
        )

        self.crawl_queue = response["QueueUrl"]

        self.families_to_process = Queue()
        self.to_validate_q = Queue()

        self.will_file = open("will.mdata", "w")

        self.sqs_push_threads = {}
        self.thr_ls = []
        self.commit_threads = 1
        for i in range(0, self.commit_threads):
            thr = threading.Thread(target=self.enqueue_loop, args=(i,))
            self.thr_ls.append(thr)
            thr.start()
            self.sqs_push_threads[i] = True
        print(f"Successfully started {len(self.sqs_push_threads)} SQS push threads!")

        # TODO: Add a preliminary loop-polling 'status check' on the endpoint that returns a noop
        # TODO: And do it here in the init. Should print something like "endpoint online!" or return error if not.
    def enqueue_loop(self, thr_id):

        print("In enqueue loop!")
        while True:
            insertables = []

            print("******* ENQUEUE LOOP ************")

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

                print("[Val Push]: sleeping for 5 seconds... ")
                time.sleep(5)
                continue

            self.sqs_push_threads[thr_id] = "ACTIVE"

            # Remove up to n elements from queue, where n is current_batch.
            current_batch = 1
            while not self.to_validate_q.empty() and current_batch < 1:
                insertables.append(self.to_validate_q.get())
                # self.active_commits -= 1  # TODO: do something with this.
                current_batch += 1
            print(f"Current batch: {current_batch}")

            # try:
            response = self.client.send_message_batch(QueueUrl=self.validation_queue_url,
                                                          Entries=insertables)
            print(f"SQS response on insert: {response}")
            # except Exception as e:  # TODO: too vague
            #     print(f"WAS UNABLE TO PROPERLY CONNECT to SQS QUEUE: {e}")

    def send_families_loop(self):

        while True:
            fx_ep = "68bade94-bf58-4a7a-bfeb-9c6a61fa5443"

            print("Communicating with SQS to pull down new_files")
            family_list = self.get_next_families()
            print(f"Family: {family_list}")

            # Cast list to FamilyBatch
            for family in family_list:

                # Get extractor out of each group
                if self.extractor_finder == 'matio':
                    d_type = "HTTPS"
                    extr_code = 'matio'
                    xtr_fam_obj = Family(download_type=d_type)

                    xtr_fam_obj.from_dict(json.loads(family))
                    xtr_fam_obj.headers = self.family_headers

                    # print(xtr_fam_obj.files)
                    # exit()

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

                print(f"Headers: {self.family_headers} \nPetrel Header: {self.headers['Petrel']}")

                if d_type == "gdrive":
                    self.current_batch.append({"event": {"family_batch": family_batch,
                                                         "creds": self.gdrive_token[0]},
                                               "func_id": ex_func_id})
                elif d_type == "HTTPS":
                    self.current_batch.append({"event": {"family_batch": family_batch},
                                               "func_id": ex_func_id})

                print(f"Current batch: {self.current_batch}")
                print(f"Batch size: {self.batch_size}")
                if len(self.current_batch) >= self.batch_size:

                    task_ids = remote_extract_batch(self.current_batch, ep_id=fx_ep, headers=self.fx_headers)
                    for task_id in task_ids:
                        self.task_dict["active"].put(task_id)

                    print(f"Active task queue (local) size: {self.task_dict['active'].qsize()}")

                    # Empty the batch! Everything in here has been sent :)
                    self.current_batch = []
            time.sleep(1)

    def launch_poll(self):
        print("POLLER IS STARTING!!!!")
        po_thr = threading.Thread(target=self.poll_responses, args=())
        po_thr.start()

    # TODO: Smarter batching that takes into account file size.
    def get_next_families(self):
        while True:
            try:
                # Step 1. Get ceiling(batch_size/10) messages down from queue.
                sqs_response = self.client.receive_message(
                    QueueUrl=self.crawl_queue,
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=20)

                family_list = []
                del_list = []
                for message in sqs_response["Messages"]:
                    message_body = message["Body"]

                    family_list.append(message_body)

                    del_list.append({'ReceiptHandle': message["ReceiptHandle"],
                                     'Id': message["MessageId"]})

                # Step 2. Delete the messages from SQS.
                if len(del_list) > 0:
                    response = self.client.delete_message_batch(
                        QueueUrl=self.crawl_queue,
                        Entries=del_list)
                    # TODO: Do the 200 check. Weird data format.
                    # print(f"Delete response: {response}")
                #     if response["HTTPStatusCode"] is not 200:
                #         raise ConnectionError("Could not delete messages from queue!")
                return family_list
            except:
                ConnectionError("Could not get new families! ")

    def launch_extract(self):

        max_threads = 1

        for i in range(0, max_threads):
            ex_thr = threading.Thread(target=self.send_families_loop(), args=())
            ex_thr.start()

    def unpack_returned_family_batch(self, family_batch):

        fam_batch_dict = family_batch.to_dict()
        return fam_batch_dict

    def poll_responses(self):
        success_returns = 0
        mod_not_found = 0
        failed_returns = 0
        type_errors = 0

        while True:

            if self.task_dict["active"].empty():
                self.logger.debug("No live IDs... sleeping...")
                p_empty = time.time()
                print(p_empty)
                time.sleep(2.5)
                continue

            # Here we pull all values from active task queue to create a batch of them!
            num_elem = self.task_dict["active"].qsize()
            tids_to_poll = []  # the batch

            for i in range(0, num_elem):

                ex_id = self.task_dict["active"].get()
                tids_to_poll.append(ex_id)

            # Send off task_ids to poll, retrieve a bunch of statuses.
            status_thing = remote_poll_batch(task_ids=tids_to_poll, headers=self.fx_headers)

            if status_thing is None:
                continue

            for tid in status_thing:

                if "result" in status_thing[tid]:

                    res = self.fx_ser.deserialize(status_thing[tid]['result'])

                    if "family_batch" in res:
                        family_batch = res["family_batch"]
                        unpacked_metadata = self.unpack_returned_family_batch(family_batch)
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

                    # TODO: Still need to return group_ids so we can mark accordingly...
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
