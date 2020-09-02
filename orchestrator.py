
import os
import json
import time
import boto3
import logging
import threading

from queue import Queue
from utils.pg_utils import pg_conn
from funcx.serialize import FuncXSerializer
from xtract_sdk.packagers import Family, FamilyBatch
from extractors.utils.batch_utils import remote_extract_batch, remote_poll_batch
from extractors import xtract_images, xtract_tabular, xtract_matio, xtract_keyword


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

        # self.image_func_id = "8274fe2f-a132-4a9e-b8e2-9ad891e997a1"
        self.fx_ser = FuncXSerializer()

        self.source_endpoint = source_eid
        self.dest_endpoint = dest_eid
        self.gdrive_token = gdrive_token
        self.fx_ep_list = fx_ep_list

        self.task_dict = {"active": Queue(), "pending": Queue(), "failed": Queue()}

        self.batch_size = 10
        self.crawl_id = crawl_id

        self.current_batch = []

        self.mdata_store_path = mdata_store_path

        self.headers = headers
        self.fx_headers = {"Authorization": f"Bearer {self.headers['FuncX']}", 'FuncX': self.headers['FuncX']}

        if 'Petrel' in self.headers:
            self.fx_headers['Petrel'] = self.headers['Petrel']

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

        # TODO: Add a preliminary loop-polling 'status check' on the endpoint that returns a noop
        # TODO: And do it here in the init. Should print something like "endpoint online!" or return error if not.

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
                    extr_code = 'matio'
                    # TODO: Create xtract_fam_obj here!

                # TODO: kick this logic for finding extractor into sdk/crawler.
                elif self.extractor_finder == 'gdrive':
                    d_type = 'gdrive'
                    xtr_fam_obj = Family(download_type=d_type)

                    xtr_fam_obj.from_dict(json.loads(family))
                    xtr_fam_obj.headers = self.headers
                    extr_code = xtr_fam_obj.groups[list(xtr_fam_obj.groups.keys())[0]].parser
                    # print(f"Extractor code: {extr_code}")

                    if extr_code is None:  # TODO: Any bookkeeping we need to do here?
                        continue

                else:
                    raise ValueError("Incorrect extractor_finder arg.")

                extractor = self.func_dict[extr_code]
                ex_func_id = extractor.func_id

                print(f"Extractor function ID: {ex_func_id}")

                # Putting into family batch -- we use funcX batching now, but no use rewriting...
                family_batch = FamilyBatch()
                family_batch.add_family(xtr_fam_obj)  # TODO: issue with matio here.

                self.current_batch.append({"event": {"family_batch": family_batch,
                                                     "creds": self.gdrive_token[0]},
                                           "func_id": ex_func_id})

                if len(self.current_batch) >= self.batch_size:

                    task_ids = remote_extract_batch(self.current_batch, ep_id=fx_ep, headers=self.fx_headers)
                    for task_id in task_ids:
                        self.task_dict["active"].put(task_id)

                    print(f"Queue size: {self.task_dict['active'].qsize()}")

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
                    print(f"Delete response: {response}")
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

        for family in family_batch.families:
            # print(f"Family Metadata: {family.metadata}")
            if len(family.metadata) > 0:
                print(f"Nonempty family metadata: {family.metadata}")
            else:
                print(f"Empty Family Metadata: {family.metadata}")

            for group in family.groups:
                # TODO: use for debugging matio.
                # print(f"Group Metadata: {family.groups[group].metadata}")
                pass

    def poll_responses(self):
        success_returns = 0
        mod_not_found = 0
        failed_returns = 0

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

                # print(f"[POLLER]: Pulled {i} items!")
                ex_id = self.task_dict["active"].get()
                tids_to_poll.append(ex_id)

            # Send off task_ids to poll, retrieve a bunch of statuses.
            status_thing = remote_poll_batch(task_ids=tids_to_poll, headers=self.fx_headers)

            for tid in status_thing:

                if "result" in status_thing[tid]:

                    res = self.fx_ser.deserialize(status_thing[tid]['result'])

                    if "family_batch" in res:
                        family_batch = res["family_batch"]
                        unpacked_metadata = self.unpack_returned_family_batch(family_batch)

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
                    # TODO: bring back.
                    try:
                        exc.reraise()
                    except ModuleNotFoundError as e:
                        mod_not_found += 1
                        print(f"Num. ModuleNotFound: {mod_not_found}")

                    failed_returns += 1
                    print(f"Exception caught: {exc}")

                else:
                    self.task_dict["active"].put(tid)
