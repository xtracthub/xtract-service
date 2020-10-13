
from queue import Queue
import globus_sdk
import boto3
import json
import time
from random import randint
import os


class GlobusPoller():

    def __init__(self, crawl_id):

        self.crawl_id = crawl_id
        
        self.transfer_check_queue = Queue()

        self.client_id = "83cd643f-8fef-4d4b-8bcf-7d146c288d81"

        self.data_source = "e38ee745-6d04-11e5-ba46-22000b92c6ec"
        self.data_dest = "af7bda53-6d04-11e5-ba46-22000b92c6ec"
        self.data_path = "/project2/chard/skluzacek/data-to-process/"

        self.file_counter = randint(100000, 999999)

        self.max_gb = 0.005

        self.last_batch = False
        bytes_in_kb = 1024
        bytes_in_mb = bytes_in_kb * 1024
        bytes_in_gb = bytes_in_mb * 1024

        self.total_bytes = bytes_in_gb * self.max_gb  # TODO: pop this out to class arg.
        self.block_size = self.total_bytes / 5

        self.client = None
        self.tc = None

        self.login()

        print("Getting SQS client from boto3...")
        self.sqs_client = boto3.client('sqs',
                              aws_access_key_id=os.environ["aws_access"],
                              aws_secret_access_key=os.environ["aws_secret"],
                              region_name='us-east-1')

        crawl_q_response = self.sqs_client.get_queue_url(
            QueueName=f'crawl_{self.crawl_id}',
            QueueOwnerAWSAccountId=os.environ["aws_account"]
        )

        self.crawl_queue_url = crawl_q_response["QueueUrl"]

        transferred_q_response = self.sqs_client.get_queue_url(
            QueueName=f'transferred_{self.crawl_id}',
            QueueOwnerAWSAccountId=os.environ["aws_account"]
        )
        self.transferred_queue_url = transferred_q_response

    def login(self):
        self.client = globus_sdk.NativeAppAuthClient(self.client_id)
        self.client.oauth2_start_flow(refresh_tokens=True)

        print('Please go to this URL and login: {0}'
              .format(self.client.oauth2_get_authorize_url()))

        authorize_url = self.client.oauth2_get_authorize_url()
        print('Please go to this URL and login:\n {0}'.format(authorize_url))

        # this is to work on Python2 and Python3 -- you can just use raw_input() or
        # input() for your specific version
        get_input = getattr(__builtins__, 'raw_input', input)
        auth_code = get_input(
            'Please enter the code you get after login here: ').strip()
        token_response = self.client.oauth2_exchange_code_for_tokens(auth_code)

        globus_auth_data = token_response.by_resource_server['auth.globus.org']
        globus_transfer_data = token_response.by_resource_server['transfer.api.globus.org']

        # most specifically, you want these tokens as strings
        AUTH_TOKEN = globus_auth_data['access_token']
        TRANSFER_TOKEN = globus_transfer_data['access_token']

        # a GlobusAuthorizer is an auxiliary object we use to wrap the token. In
        # more advanced scenarios, other types of GlobusAuthorizers give us
        # expressive power
        authorizer = globus_sdk.AccessTokenAuthorizer(TRANSFER_TOKEN)
        self.tc = globus_sdk.TransferClient(authorizer=authorizer)

    def get_size(self, start_path = '.'):
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(start_path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                # skip if it is symbolic link
                if not os.path.islink(fp):
                    total_size += os.path.getsize(fp)

        return total_size

    def get_new_families(self):
        sqs_response = self.sqs_client.receive_message(
            QueueUrl=self.crawl_queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=5)

        size_of_fams = 0

        # print(sqs_response)
        # print(f"Number of SQS messages: {sqs_response['Messages']}")

        if "Messages" in sqs_response and len(sqs_response["Messages"]) > 0:

            del_list = []
            for item in sqs_response["Messages"]:
                family = json.loads(item["Body"])

                fam_id = family["family_id"]

                tot_fam_size = 0
                for file_obj in family["files"]:
                    file_size = file_obj["metadata"]["physical"]["size"]
                    tot_fam_size += file_size

                self.family_map[fam_id] = family
                self.local_transfer_queue.put(fam_id)
                self.fam_to_size_map[fam_id] = tot_fam_size

                size_of_fams += tot_fam_size

                del_list.append({'ReceiptHandle': item["ReceiptHandle"],
                                     'Id': item["MessageId"]})
            if len(del_list) > 0: 
                response = self.sqs_client.delete_message_batch(QueueUrl=self.crawl_queue_url, Entries=del_list)
                print(response)              
        else:
            self.last_batch = True
            return None
        return size_of_fams


    def main_poller_loop(self):

        self.family_map = dict()
        self.fam_to_size_map = dict()
        self.local_transfer_queue = Queue()
        self.local_update_queue = Queue()
        self.transfer_map = dict()

        self.current_batch = []
        self.current_batch_bytes = 0

        self.bytes_in_flight = 0

        need_more_families = True

        while True:
            t0 = time.time()
            num_bytes = self.get_size()
            t1 = time.time()

            print(f"Need more families: {need_more_families}")
            if need_more_families:
                total_size = self.get_new_families()

                if total_size is None:
                    total_size = 0
                    print("No new messages! Continuing... ")

            print(f"Total time to get folder size: {t1-t0} seconds")
            print("Made it here!")
            print(f"Num bytes: {num_bytes}")
            print(f"Total bytes: {self.total_bytes}")
            print(f"Is queue empty?: {self.local_transfer_queue.empty()}")

            # time.sleep(2)  

            # Check if we are under capacity and there's more queue elements to grab.
            print(f"local_transfer_queue.empty()?: {self.local_transfer_queue.empty()}")
            while num_bytes < self.total_bytes and not self.local_transfer_queue.empty():
                need_more_families = True

                cur_fam_id = self.local_transfer_queue.get()
                cur_fam_size = self.fam_to_size_map[cur_fam_id]

                self.current_batch.append(self.family_map[cur_fam_id])
                self.current_batch_bytes += cur_fam_size

                # TODO: what if one file is larger than the entire batch size?

            print(f"Current batch Size: {self.current_batch_bytes}")
            print(f"Block Size: {self.block_size}")
            if self.current_batch_bytes >= self.block_size or (self.last_batch and len(self.current_batch) > 0):
                # TODO: now we need to send the batch.
                print("Generating a batch transfer object...")
                time.sleep(5)
                tdata = globus_sdk.TransferData(self.tc,
                                                self.data_source,
                                                self.data_dest,
                                                label="Xtract attempt",
                                                sync_level="checksum")
                
                fid_list = []
                for family_to_trans in self.current_batch:

                    # Create a directory (named by family_id) into which we want to place our families.
                    fam_dir = '/project2/chard/skluzacek/data_to_process/{}'.format(family_to_trans['family_id'])
                    os.makedirs(fam_dir, exist_ok=True)
                    for file_obj in family_to_trans['files']:
                        file_path = file_obj['path']
                        file_name = file_obj['path'].split('/')[-1]

                        tdata.add_item(file_path, f"{fam_dir}/{file_name}")
                        fid_list.append(family_to_trans['family_id'])

                        # TODO: add so we can poll Globus jobs.
                transfer_result = self.tc.submit_transfer(tdata)
                print(f"Transfer result: {transfer_result}")
                gl_task_id = transfer_result['task_id']
                self.transfer_check_queue.put(gl_task_id)
                
                self.transfer_map[gl_task_id] = fid_list

                self.current_batch = []
                self.current_batch_bytes = 0

            else:
                need_more_families = True

            gl_task_tmp_ls = []
            while not self.transfer_check_queue.empty():
                gl_tid = self.transfer_check_queue.get()
                res = self.tc.get_task(gl_tid)
                print(res)
                if res['status'] != "SUCCEEDED":
                    gl_task_tmp_ls.append(gl_tid)
                else:
                    # TODO: Get the families associated with the transfer task. 
                    fids = self.transfer_map[gl_tid]
                    print(f"These are the fids: {fids}")

                    insertables = []
                    for fid in fids:
                        self.file_counter += 1
                        family_object = {"Id": self.file_counter, "MessageBody": json.dumps(self.family_map[fid])}

                        insertables.append(family_object)

                    response = self.client.send_message_batch(QueueUrl=self.transferred_queue_url,
                                                          Entries=insertables)

                    print("Response for crawl queue. ")
                
            for gl_tid in gl_task_tmp_ls:
                self.transfer_check_queue.put(gl_tid)

            if self.last_batch and self.transfer_check_queue.empty():
                print("No more Transfer tasks and incoming queue empty")
                exit()
            

crawl_id = "4bc5112f-f9be-442d-8ba9-df846aa0503f"
g = GlobusPoller(crawl_id=crawl_id)
g.main_poller_loop()

