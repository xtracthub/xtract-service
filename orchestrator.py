
import os
import json
import time
import boto3
import logging
import requests
import threading

from queue import Queue
from funcx.serialize import FuncXSerializer
from xtract_sdk.packagers.family import Family

from utils.pg_utils import pg_conn
from utils.fx_utils import serialize_fx_inputs

from extractors import xtract_images, xtract_tabular, xtract_matio, xtract_keyword


class Orchestrator:
    # TODO: Make source_eid and dest_eid default to None for the HTTPS case?
    def __init__(self, crawl_id, headers, funcx_eid,
                 mdata_store_path, source_eid=None, dest_eid=None, gdrive_token=None, logging_level='debug', instance_id=None):

        self.funcx_eid = funcx_eid
        self.func_dict = {"image": xtract_images.ImageExtractor(),
                          "tabular": xtract_tabular.TabularExtractor(),
                          "text": xtract_keyword.KeywordExtractor(),
                          "matio": xtract_matio.MatioExtractor()}

        # self.image_func_id = "8274fe2f-a132-4a9e-b8e2-9ad891e997a1"
        self.fx_ser = FuncXSerializer()

        self.source_endpoint = source_eid
        self.dest_endpoint = dest_eid
        self.gdrive_token = gdrive_token

        self.task_dict = {"active": Queue(), "pending": Queue(), "failed": Queue()}

        self.max_active_families = 100
        self.families_limit = 2
        self.crawl_id = crawl_id

        self.mdata_store_path = mdata_store_path

        self.conn = pg_conn()
        self.headers = headers
        self.fx_headers = {"Authorization": f"Bearer {self.headers['FuncX']}", 'FuncX': self.headers['FuncX']}

        if 'Petrel' in self.headers:
            self.fx_headers['Petrel'] = self.headers['Petrel']

        self.get_url = 'https://funcx.org/api/v1/{}/status'
        self.post_url = 'https://funcx.org/api/v1/submit'

        self.logging_level = logging_level

        self.logger = logging.getLogger(__name__)
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.DEBUG)
        self.instance_id = instance_id

        self.sqs_base_url = "https://sqs.us-east-1.amazonaws.com/576668000072/"
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
            # TODO: env variable
            QueueOwnerAWSAccountId='576668000072'
        )

        self.crawl_queue = response["QueueUrl"]

        # TODO: Add a preliminary loop-polling 'status check' on the endpoint that returns a noop
        # TODO: And do it here in the init.

    def pack_and_submit_map(self):

        while True:
            fx_ep = "82ceed9f-dce1-4dd1-9c45-6768cf202be8"

            # task_dict = {"active": Queue(), "pending": Queue(), "results": [], "failed": Queue()}

            # TODO: fixed at 0.
            # TODO: Remove GDrive cred from token.

            family_list = self.get_next_families()

            for family in family_list:
                # print("This is the raw family")
                family = json.loads(family)
                # TODO: Stretch this out for beyond GDrive (shouldn't just look at 1)
                # extractor_name = family['groups'][0]["parser"]


                family["headers"] = self.headers

                d_type = family["download_type"]
                xtr_fam_obj = Family(download_type=d_type)
                # xtr_fam_obj = Family(download_type="gdrive")

                print(family)
                # exit()

                xtr_fam_obj.from_dict(family)
                print(f"Files: {xtr_fam_obj.files}")
                print(f"Groups: {xtr_fam_obj.groups}")
                # print(xtr_fam_obj.groups)
                # exit()
                # print(family)

                # exit()
                # TODO: fix. Maybe two level ('family' extractor trumps individual.)

                if d_type == "gdrive":
                    extr_code = xtr_fam_obj.groups[list(xtr_fam_obj.groups.keys())[0]].parser
                    # extractor = self.func_dict[extr_code]
                    extractor = self.func_dict['image']

                print("SENDING OFF TASK!!!")
                task_id = extractor.remote_extract_solo({"families": [xtr_fam_obj], "gdrive": self.gdrive_token[0]}, fx_ep,
                                                        headers=self.fx_headers)


                # TODO: Bring back for Google Drive.
                # data = {'gdrive': self.gdrive_token[0], 'file_id': file_id, 'is_gdoc': is_gdoc, 'extension': family["extension"]}

                self.task_dict["active"].put(task_id)

                print(f"Queue size: {self.task_dict['active'].qsize()}")
                print(task_id)
                # print("Exiting main loop...")
                # exit()

            failed_counter = 0
            break

    def launch_poll(self):
        print("POLLER IS STARTING!!!!")
        po_thr = threading.Thread(target=self.poll_responses, args=())
        po_thr.start()

    # TODO: Smarter batching that takes into account file size.
    def get_next_families(self):
        while True:
            try:
                # Step 1. Get ceiling(batch_size/10) messages down from queue.
                t_families_get_start = time.time()
                sqs_response = self.client.receive_message(
                    QueueUrl=self.crawl_queue,
                    MaxNumberOfMessages=10,  # TODO: Change back to 10.
                    WaitTimeSeconds=20)

                family_list = []
                del_list = []
                for message in sqs_response["Messages"]:
                    message_body = message["Body"]

                    family_list.append(message_body)

                    del_list.append({'ReceiptHandle': message["ReceiptHandle"],
                                     'Id': message["MessageId"]})

                # print(family_list)
                # exit()

                # Step 2. Delete the messages from SQS.
                if len(del_list) > 0:
                    response = self.client.delete_message_batch(
                        QueueUrl=self.crawl_queue,
                        Entries=del_list)
                    # TODO: Do the 200 check. WEird data format.
                    # if response["HTTPStatusCode"] is not 200:
                    #     raise ConnectionError("Could not delete messages from queue!")
                # continue
                time.sleep(0.5)
                return family_list
            except:
                ConnectionError("Could not get new families! ")

    def launch_extract(self):

        max_threads = 1

        for i in range(0, max_threads):
            ex_thr = threading.Thread(target=self.pack_and_submit_map(), args=())
            ex_thr.start()

    def poll_responses(self):
        success_returns = 0
        mod_not_found = 0
        failed_returns = 0
        poll_start = time.time()
        while True:
            if self.task_dict["active"].empty():
                self.logger.debug("No live IDs... sleeping...")
                p_empty = time.time()
                print(p_empty)
                time.sleep(2.5)
                continue

            num_elem = self.task_dict["active"].qsize()
            for _ in range(0, num_elem):
                ex_id = self.task_dict["active"].get()

                # TODO: Switch this to batch gets.
                status_thing = requests.get(self.get_url.format(ex_id), headers=self.fx_headers)

                try:
                    status_thing = json.loads(status_thing.content)
                except json.decoder.JSONDecodeError as e:
                    self.logger.error(e)
                    self.logger.error("Status unloadable. Marking as failed.")
                    continue
                    # TODO. What exactly SHOULD I do?

                self.logger.debug(f"Status: {status_thing}")

                if "result" in status_thing:
                    try:
                        res = self.fx_ser.deserialize(status_thing['result'])
                        print("RESULT! ")
                        print(res)

                        # TODO: Uncomment to test validation
                        # import pickle
                        # with open("example_mdata.pkl", "wb") as f:
                        #     pickle.dump(res, f)
                        #
                        # print("WE DUMPED THE PICKLE!")
                        # exit()

                        # TODO: Take out only the 'valid' metadata.

                    except ModuleNotFoundError as e:
                        mod_not_found += 1
                        print(e)
                        print(f"NUM MOD NOT FOUND: {mod_not_found}")
                        continue
                    success_returns += 1
                    self.logger.debug(f"Success Counter: {success_returns}")
                    self.logger.debug(f"Failure Counter: {failed_returns}")
                    self.logger.debug(f"Received response: {res}")

                    # TODO: Still need to return group_ids so we can mark accordingly...
                    if type(res) is not dict:
                        continue

                    print("Do we ever make it down here???")

                    print("metadata" in res)
                    if "metadata" in res:
                        print("WAHOOOOOOOOOOOOO")

                elif "exception" in status_thing:
                    exc = self.fx_ser.deserialize(status_thing['exception'])
                    # TODO: bring back.
                    exc.reraise()

                    print(f"Exception caught: {exc}")

                else:
                    self.task_dict["active"].put(ex_id)
