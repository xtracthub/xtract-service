
import os
import json
import time
import boto3
import logging
import requests
import threading

from queue import Queue
from funcx.serialize import FuncXSerializer

from utils.pg_utils import pg_conn
from utils.fx_utils import serialize_fx_inputs


class Orchestrator:
    # TODO: Make source_eid and dest_eid default to None for the HTTPS case?
    def __init__(self, crawl_id, headers, funcx_eid,
                 mdata_store_path, source_eid=None, dest_eid=None, gdrive_token=None, logging_level='debug', instance_id=None):

        self.funcx_eid = funcx_eid
        self.func_dict = {"image": "6ce9b5cf-1847-4339-b204-2db113034113",
                          "tabular": "aa7c44ea-340f-4829-bcc6-6f9ad372f3d0",
                          "text": "08cf53cd-e0a8-440c-bd1e-4003601a76c4"}

        # self.image_func_id = "8274fe2f-a132-4a9e-b8e2-9ad891e997a1"
        self.fx_ser = FuncXSerializer()
        self.tabular_func_id = 0
        self.bert_func_id = 0
        self.keyword_func_id = 0

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

        self.get_url = 'https://dev.funcx.org/api/v1/{}/status'
        self.post_url = 'https://dev.funcx.org/api/v1/submit'

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

        num_none = 0
        num_tabular = 0
        num_keyword = 0
        num_images = 0
        while True:
            post_url = 'https://dev.funcx.org/api/v1/submit'
            fx_ep = "82ceed9f-dce1-4dd1-9c45-6768cf202be8"

            #task_dict = {"active": Queue(), "pending": Queue(), "results": [], "failed": Queue()}

            # TODO: fixed at 0.
            # TODO: Remove GDrive cred from token.

            family_list = self.get_next_families()

            for family in family_list:
                family = json.loads(family)

                file_id = family["id"]
                extractor = family["extractor"]
                is_gdoc = True if 'google' in family["mimeType"] else False

                if extractor == "text":
                    num_keyword += 1
                    func = self.func_dict["text"]
                elif extractor == "images":
                    num_images += 1
                    func = self.func_dict["image"]
                elif extractor == "tabular":
                    num_tabular += 1
                    func = self.func_dict["tabular"]
                elif extractor == None:
                    num_none += 1
                    print("NoneType -- skipping! ")
                    continue
                else:
                    print(extractor)
                    raise ValueError

                data = {'gdrive': self.gdrive_token[0], 'file_id': file_id, 'is_gdoc': is_gdoc, 'extension': family["extension"]}

                res = requests.post(url=post_url,
                                            headers=self.fx_headers,
                                            json={'endpoint': fx_ep,
                                                  'func': func,
                                                  'payload': serialize_fx_inputs(
                                                      event=data)})
                # print(res.content)
                if res.status_code == 200:
                    task_uuid = json.loads(res.content)['task_uuid']
                    self.task_dict["active"].put(task_uuid)
                    print(f"Queue size: {self.task_dict['active'].qsize()}")

            failed_counter = 0

    def launch_poll(self):
        print("POLLER IS STARTING!!!!")
        po_thr = threading.Thread(target=self.poll_responses, args=())
        po_thr.start()


            # while True:
            #
            #     if task_dict["active"].empty():
            #         print("Active task queue empty... sleeping... ")
            #         time.sleep(0.5)
            #         break  # This should jump out to main_loop
            #
            #     cur_tid = task_dict["active"].get()
            #     print(cur_tid)
            #     status_thing = requests.get(get_url.format(cur_tid), headers=self.fx_headers).json()
            #
            #     if 'result' in status_thing:
            #         result = fx_ser.deserialize(status_thing['result'])
            #         print(f"Result: {result}")
            #
            #     elif 'exception' in status_thing:
            #         print(f"Exception: {fx_ser.deserialize(status_thing['exception'])}")
            #         # break
            #     else:
            #         task_dict["active"].put(cur_tid)


    # TODO: Smarter batching that takes into account file size.
    def get_next_families(self):
        while True:
            try:
                # Step 1. Get ceiling(batch_size/10) messages down from queue.
                t_families_get_start = time.time()
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
                    # TODO: Do the 200 check. WEird data format.
                    # if response["HTTPStatusCode"] is not 200:
                    #     raise ConnectionError("Could not delete messages from queue!")
                #continue
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
        failed_returns = 0

        total_success = 0

        success_keyword = 0
        success_tabular = 0
        success_images = 0

        tot_keyword_trans = 0
        tot_images_trans = 0
        tot_tabular_trans = 0

        tot_keyword_tot = 0
        tot_images_tot = 0
        tot_tabular_tot = 0

        mod_not_found = 0
        poll_start = time.time()
        while True:

            if success_keyword > 0:
                print("Average Keyword Stats")
                print(f"Transfer: {tot_keyword_trans/success_keyword} | Total: {tot_keyword_tot/success_keyword}")

            if success_images > 0:
                print("Average Image Stats")
                print(f"Transfer: {tot_images_trans / success_images} | Total: {tot_images_tot / success_images}")

            if success_tabular >0:
                print("Average Tabular Stats")
                print(f"Transfer: {tot_tabular_trans / success_tabular} | Total: {tot_tabular_tot / success_tabular}")

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
                    except ModuleNotFoundError as e:
                        mod_not_found +=1
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



                        if "tabular" in res["metadata"]:
                            if res["metadata"]["tabular"] != {}:
                                 print("GOOD TABULAR!!!")
                                 tot_tabular_trans += res["trans_time"]
                                 tot_tabular_tot += res["tot_time"]

                                 success_tabular += 1
                                 total_success += 1


                        elif "keywords" in res["metadata"]:
                            if res["metadata"]["keywords"] is not None and res["metadata"]["keywords"] != {}:
                                  print("GOOD KEYWORD!!!")
                                  tot_keyword_trans += res["trans_time"]
                                  tot_keyword_tot += res["tot_time"]

                                  success_keyword += 1
                                  total_success += 1

                        elif "image-sort" in res["metadata"]:
                            if res["metadata"]["image-sort"] != {}:
                                print("GOOD IMAGES!!!")
                                tot_images_trans += res["trans_time"]
                                tot_images_tot += res["tot_time"]
                                success_images += 1
                                total_success += 1

                    else:
                        print("else")
                        print(f"Here's the res: {res}")

                        # group_coll = res["metadata"][fam_id]["metadata"]
                        # trans_time = res["metadata"][fam_id]["metadata"]



                else:
                    self.task_dict["active"].put(ex_id)


#
#     def pack_and_submit_map(self, data):
#
#         # extractor_type = data["extractor"]
#         # print("IN PACK_AND_SUBMIT")
#         # print(data)
#         # extractor_type = data["extractor"]
#         # print(extractor_type)
#         # print(data)
#         # print("HERE'S THE DATA")
#         # data = serialize_fx_inputs(data)
#         # print("HERE GOES ROUND 2!!!")
#         # data = serialize_fx_inputs(data)
#         #print(data)
#
#         # data2 = pickle.dumps(data[0])
#         # exit()
#
#         fn_id = "0671c0ef-d9d4-4382-89f1-6f4e56f45720"
#         fx_ep = "82ceed9f-dce1-4dd1-9c45-6768cf202be8"
#         post_url = 'https://dev.funcx.org/api/v1/submit'
#
#         headers = {'Authorization': 'Bearer AgK8Dkvx90XY5qX2Vajxn0boaoedlOyJaXbVvmWjnaoybo3QlMieCQz48e67PydOVmpke1734kkb8vC1NMymVu7w3n', 'Transfer': 'Ag8496kzod33gmepjDNXVxNYVP2wkJeYO1rnVqpjgnyEPXxa15I8Cv9wnGGQaxbed4YmXVYBKD9wyKt7KPgNvcoqlv', 'FuncX': 'AgK8Dkvx90XY5qX2Vajxn0boaoedlOyJaXbVvmWjnaoybo3QlMieCQz48e67PydOVmpke1734kkb8vC1NMymVu7w3n', 'Petrel': 'AgmJJ674z4wEvj1o4Jmwar78kDj2QrkdVxaKkeaDD6pgkEJjlyH8C0zbkEvq3GKWgykEydwGGJG1qVC8rnE7XHgGEJ'}
#         t_launch_times = {}
#         task_dict = {"active": Queue(), "pending": Queue(), "results": [], "failed": Queue()}
#
#         data = {
#             'gdrive_pkl': b'\x80\x03cgoogle.oauth2.credentials\nCredentials\nq\x00)\x81q\x01}q\x02(X\x05\x00\x00\x00tokenq\x03X\xab\x00\x00\x00ya29.a0AfH6SMDkmvH9GI9EL6eKYaQo5OUF4c9Um3oIqgel5IN-Fc9nJqIbke9PTGPc3BuJvWhSuKuWT9NnXZmy9PIN1xMi7rkhCH_gY8bQttlPUEkBJ7HoKi_xPj1nU_pd3qQMnQvyV7oi_CeAaf_LTBrsK22x--WK7wA5cbjFq\x04X\x06\x00\x00\x00expiryq\x05cdatetime\ndatetime\nq\x06C\n\x07\xe4\x06\x08\x152\x1f\x08\x89\x05q\x07\x85q\x08Rq\tX\x0e\x00\x00\x00_refresh_tokenq\nXg\x00\x00\x001//0fYtW4N1qkLF2CgYIARAAGA8SNwF-L9Ir8QaekPkzMwhqN8orPQxeV_ayzLVwFmIHDsW4fmVRU_pj2zJZew1j3dqSY_XywTP_S3gq\x0bX\t\x00\x00\x00_id_tokenq\x0cNX\x07\x00\x00\x00_scopesq\r]q\x0e(X7\x00\x00\x00https://www.googleapis.com/auth/drive.metadata.readonlyq\x0fX%\x00\x00\x00https://www.googleapis.com/auth/driveq\x10eX\n\x00\x00\x00_token_uriq\x11X#\x00\x00\x00https://oauth2.googleapis.com/tokenq\x12X\n\x00\x00\x00_client_idq\x13XH\x00\x00\x00364500245041-r1eebsermd1qp1qo68a3qp09hhpc5dfi.apps.googleusercontent.comq\x14X\x0e\x00\x00\x00_client_secretq\x15X\x18\x00\x00\x00XMGaOYAeBxLz9ZWvsVQCvWtkq\x16X\x11\x00\x00\x00_quota_project_idq\x17NubN\x86q\x18.',
#             'file_id': '1zAJJy4bFQ2ZANV7W3iv7WdpZWRBp8iEN'}
#
#         res = requests.post(url=post_url,
#                             headers=headers,
#                             json={'endpoint': fx_ep,
#                                   'func': fn_id,
#                                   'payload': serialize_fx_inputs(
#                                       event=data)
#                                   }
#                             )
#
#         print(res.content)
#         if res.status_code == 200:
#             task_uuid = json.loads(res.content)['task_uuid']
#             task_dict["active"].put(task_uuid)
#             t_launch_times[task_uuid] = time.time()
#
#
#         # print(pack_map)
#         # print(f"Length of pack_map (from above): {len(pack_map)}")
#         # for item in pack_map:
#         #     self.task_dict["active"].put(item)
#
#         if res.status_code == 200:
#             task_uuid = json.loads(res.content)['task_uuid']
#             print("TASK ID!")
#             print(task_uuid)
#             self.task_dict["active"].put(task_uuid)
#         exit()
#
#     def send_files(self):
#         # Just keep looping in case something comes up in crawl.
#         while True:
#             data = {'inputs': [], "transfer_token": self.headers["Transfer"], "source_endpoint": 'e38ee745-6d04-11e5-ba46-22000b92c6ec', "dest_ep": "1adf6602-3e50-11ea-b965-0e16720bb42f"}
#
#             # TODO: should be, like, get families (and those families can have groups in them)
#             # TODO: SHOULD package the exact same way as the family-test example.
#
#             families = self.get_next_families()
#
#             funcx_fam_ship_list = []
#
#             print(len(families))
#             print(type(families))
#
#             for family in families:
#
#                 family = json.loads(family)
#
#                 # TODO: In the case of Google Drive (should do on crawler-side)
#                 if 'family_id' not in family:
#
#                     family['family_id'] = str(uuid4())
#
#                 fid = family["family_id"]
#                 logging.info(f"Processing family ID: {fid}")
#
#                 fam_dict = {"family_id": fid, "files": {}, "groups": {}}
#
#                 # TODO: special wrapping for gdrive again.
#                 if not 'groups' in family:
#                     family["groups"] = {}
#
#                     print("ITEM AND FAMILY")
#                     print(family)
#
#                     group_id = str(uuid4())
#                     family["groups"][group_id] = family
#                     family["groups"][group_id]["parser"] = family["extractor"]
#                     family["groups"][group_id]["files"] = [family["id"]]
#
#                 for group in family["groups"]:
#
#                     gid = group
#                     gr = family["groups"][group]
#                     parser = gr["parser"]
#                     print(parser)
#                     files = gr["files"]
#
#                     self.logger.debug(f"Processing GID: {gid}")
#
#                     group = {'group_id': gid, 'files': [], 'parser': parser}
#
#                     # Take all of the files and append them to our payload.
#                     # TODO: There is no longer 'old_mdata'.
#
#                     # TODO: Bring this all back for Globus
#                     # for f_obj in files:
#                     #     id_count = 0
#                     #
#                     #     # Keep matching here to families.
#                     #     # TODO: Uncomment for Petrel
#                     #     # TODO: Allow endpoint_id to be a URL.
#                     #     # file_url = f'https://{self.source_endpoint}.e.globus.org{f_obj}'
#                     #     file_url = f'https://data.materialsdatafacility.org{f_obj}'
#                     #     payload = {
#                     #         'url': file_url,
#                     #         'headers': self.fx_headers,
#                     #         'file_id': id_count}
#                     #     id_count += 1
#                     #
#                     #     if file_url not in fam_dict['files']:
#                     #         fam_dict['files'][file_url] = payload
#                     #
#                     #     group["files"].append(file_url)
#                     #     data["transfer_token"] = self.headers['Transfer']
#
#                     #### TODO: EVERYTHING IN THIS BOX IS GOOGLE DRIVE REQUEST CONSTRUCTION ZONE ####
#
#                     for filename in family["groups"][gid]["files"]:
#                         print(filename)
#
#                     print("HOORAY!")
#                     # exit()
#
#                     # TODO: I should need these lines when not HTTPS!
#                     # data["source_endpoint"] = self.source_endpoint
#                     # data["dest_endpoint"] = self.dest_endpoint
#                     ###################################################
#
#                     fam_dict["groups"][gid] = group
#                     # data["inputs"].append(fam_dict)
#
#                     # TODO: if GDRIIVE
#                 data["inputs"] = []
#                 data["file_id"] = family["id"]
#                 data["gdrive_pkl"] = pickle.dumps((self.gdrive_token, None))
#                 data["extractor"] = family["extractor"]
#                 funcx_fam_ship_list.append(data)
#
#
#             print(f"Length of family shipping list: {len(funcx_fam_ship_list)}")
#             # TODO: What's the failure case here?
#             self.pack_and_submit_map(funcx_fam_ship_list)
#
#             print("EXITED PACK AND SUBMIT")
#
#             while True:
#                 if self.task_dict["active"].qsize() < self.max_active_families:
#                     print("BREAKING")
#                     break
#                 else:
#                     print(self.task_dict["active"].qsize())
#                     print("SPINNING IN PLACE!")
#                     time.sleep(3)
#                     pass
#

#
#     def launch_poll(self):
#         po_thr = threading.Thread(target=self.poll_responses, args=())
#         po_thr.start()
# #