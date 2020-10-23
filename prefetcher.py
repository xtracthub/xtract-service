

def globus_poller_funcx(event):

    from queue import Queue
    import globus_sdk
    import boto3
    import json
    import time
    from random import randint
    import os
    import csv
    import datetime
    from get_dir_size import get_data_dir_size
    import threading

    class GlobusPoller():

        def __init__(self, transfer_token, crawl_id, data_source, data_dest, data_path, max_gb):

            # TODO 1: pass this in.
            self.transfer_token = transfer_token
            # self.transfer_token =

            # TODO 2: pass this in.
            self.crawl_id = crawl_id

            self.transfer_check_queue = Queue()

            self.client_id = "83cd643f-8fef-4d4b-8bcf-7d146c288d81"

            # TODO 3: pass this in.
            # self.data_source = "e38ee745-6d04-11e5-ba46-22000b92c6ec"  # MDF@Petrel
            self.data_source = data_source
            # self.data_source = "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec"  # MDF@NCSA

            # TODO 4: pass this in.
            # self.data_dest = "af7bda53-6d04-11e5-ba46-22000b92c6ec"
            self.data_dest = data_dest

            # TODO 5: pass this in.
            # self.data_path = "/project2/chard/skluzacek/data-to-process/"
            self.data_path = data_path

            self.file_counter = randint(100000, 999999)

            # TODO 6: pass this in.
            # max_gb = 0.05
            self.max_gb = max_gb

            self.last_batch = False
            bytes_in_gb = 1024 * 1024 * 1024

            self.max_bytes = bytes_in_gb * max_gb  # TODO: pop this out to class arg.
            self.block_size = self.max_bytes / 5

            self.client = None
            self.tc = None

            self.family_map = dict()
            self.fam_to_size_map = dict()
            self.local_transfer_queue = Queue()
            self.local_update_queue = Queue()
            self.transfer_map = dict()

            self.current_batch = []
            self.current_batch_bytes = 0

            self.get_globus_tc(self.transfer_token)

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
            self.transferred_queue_url = transferred_q_response["QueueUrl"]

            # Get rid of log file if it already exists. # TODO: have timestamped log folders for each crawl/transfer.
            if os.path.exists("folder_size.csv"):
                os.remove("folder_size.csv")

            # Now create a fresh file here.
            with open("folder_size.csv", "w") as g:
                g.close()

            self.cur_data_folder_size = 0

            print("Starting thread to get size!")
            get_size_thr = threading.Thread(target=self.get_size, args=())
            get_size_thr.start()
            print("Successfully started thread!")

        def get_globus_tc(self, TRANSFER_TOKEN):

            authorizer = globus_sdk.AccessTokenAuthorizer(TRANSFER_TOKEN)
            self.tc = globus_sdk.TransferClient(authorizer=authorizer)

        def get_size(self):
            # bytes    #kilo  #mega
            num_mbytes = get_data_dir_size() / 1024 / 1024
            cur_time = datetime.datetime.now()
            cur_read_time = cur_time.strftime("%Y-%m-%d %H:%M:%S")

            self.cur_data_folder_size = num_mbytes * 1024 * 1024  # Convert back to bytes.
            with open("folder_size.csv", 'a') as f:
                writer = csv.writer(f)
                writer.writerow([cur_read_time, num_mbytes])

            print(f"************** SIZE IN MB: {self.cur_data_folder_size} ***********************")

        def get_new_families(self):
            sqs_response = self.sqs_client.receive_message(
                QueueUrl=self.crawl_queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=5)

            size_of_fams = 0

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
                return
            return size_of_fams

        def main_poller_loop(self):

            need_more_families = True

            while True:
                print(f"Need more families: {need_more_families}")
                if need_more_families:
                    total_size = self.get_new_families()

                    if total_size is None:
                        total_size = 0
                        print("No new messages! Continuing... ")

                print(f"Current Data folder size (MB): {self.cur_data_folder_size}")

                # Check if we are under capacity and there's more queue elements to grab.
                print(f"local_transfer_queue.empty()?: {self.local_transfer_queue.empty()}")

                print(f"[Tyler 1] Folder size: {self.cur_data_folder_size}")
                print(f"[Tyler 2] Maximum bytes: {self.max_bytes}")
                while self.cur_data_folder_size < self.max_bytes and not self.local_transfer_queue.empty():
                    print("INSIDE THIS LOOP")
                    print(f"Update queue size: {self.local_transfer_queue.qsize()}")
                    need_more_families = True

                    cur_fam_id = self.local_transfer_queue.get()
                    cur_fam_size = self.fam_to_size_map[cur_fam_id]

                    self.current_batch.append(self.family_map[cur_fam_id])
                    self.current_batch_bytes += cur_fam_size

                    # TODO: what if one file is larger than the entire batch size?

                print(f"Current batch Size: {self.current_batch_bytes}")
                print(f"Block Size: {self.block_size}")

                if self.current_batch_bytes >= self.block_size or (self.last_batch and len(self.current_batch) > 0):
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

                            # TODO: is this actually mutable?
                            file_obj['path'] = f"{fam_dir}/{file_name}"

                            tdata.add_item(file_path, f"{fam_dir}/{file_name}")
                        fid_list.append(family_to_trans['family_id'])

                        # Now we do the same for groups unless we want to #TODO move this upstream.
                        for group_obj in family_to_trans['groups']:
                            for file_obj in group_obj['files']:
                                file_path = file_obj['path']
                                file_name = file_obj['path'].split('/')[-1]

                                file_obj['path'] = f"{fam_dir}/{file_name}"

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

                        insertables_batch = []
                        insertables = []
                        max_insertables = 10  # SQS can only upload 10 messages at a time.

                        for fid in fids:
                            self.file_counter += 1

                            print(f"Family: {self.family_map[fid]}")
                            family_object = {"Id": str(self.file_counter), "MessageBody": json.dumps(self.family_map[fid])}
                            insertables.append(family_object)

                            if len(insertables) == 6:
                                insertables_batch.append(insertables)
                                insertables = []  # reset to be empty since we dumped into the batch.

                        # Now catch case where batch isn't full BUT we still need to put them into list for push to SQS.
                        if len(insertables) > 0:
                            insertables_batch.append(insertables)

                        for insertables in insertables_batch:
                            response = self.sqs_client.send_message_batch(QueueUrl=self.transferred_queue_url,
                                                                          Entries=insertables)

                            print(f"Response for transferred queue: {response}")
                        time.sleep(2)

                for gl_tid in gl_task_tmp_ls:
                    self.transfer_check_queue.put(gl_tid)

                if self.last_batch and self.transfer_check_queue.empty():
                    print("No more Transfer tasks and incoming queue empty")
                    exit()
                print(f"Broke out of loop. Sleeping for 5 seconds...")
                time.sleep(5)

    poller = GlobusPoller(event['transfer_token'],
                          event['crawl_id'],
                          event['data_source'],
                          event['data_dest'],
                          event['data_path'],
                          event['max_gb'])
    poller.main_poller_loop()


from funcx import FuncXClient

fxc= FuncXClient()

ep_id = "17214422-4570-4891-9831-2212614d04fa"

# register function
fn_uuid = fxc.register_function(globus_poller_funcx, ep_id,
                                description="I wrote this when Matt said he was undecided during 2020 election")

crawl_id = "9473948a-be98-4c7f-963b-c69bc240425e"

data_source = "e38ee745-6d04-11e5-ba46-22000b92c6ec"
transfer_token = 'AgGdePJzb81r61NEKw09XYD83NeXkam8Q9QzVd9jz10w4obYwwU7CKeJx4Qxmaago8PgQNrQOWdY3EtQ2a3PoFvNlQ'

data_dest = "af7bda53-6d04-11e5-ba46-22000b92c6ec"
data_path = "/project2/chard/skluzacek/data-to-process/"

event = {'transfer_token': transfer_token,
         'crawl_id': crawl_id,
         'data_source': data_source,
         'data_dest': data_dest,
         'data_path': data_path,
         'max_gb': 0.05}

print(fn_uuid)

task_id = fxc.run(event, endpoint_id=ep_id, function_id=fn_uuid)
# launch function.
