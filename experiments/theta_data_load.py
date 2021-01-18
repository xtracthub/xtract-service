
import globus_sdk
import json
import boto3
import time
import csv
import os
from queue import Queue
import threading

# from tests.test_utils.native_app_login import globus_native_auth_login

# Get auth creds

# headers = globus_native_auth_login()

# Connect to AWS
crawl_id = "a36bd106-5f4e-489b-9d48-77ec46813433"

# source_eid = "e38ee745-6d04-11e5-ba46-22000b92c6ec"
# dest_eid = "af7bda53-6d04-11e5-ba46-22000b92c6ec"


# Connect to Globus
# authorizer = globus_sdk.AccessTokenAuthorizer(headers['Transfer'])
# tc = globus_sdk.TransferClient(authorizer=authorizer)

# connect to enormous crawl queue and pull down 200k families.
# This creates the extraction and validation queues on the Simple Queue Service.
sqs_base_url = "https://sqs.us-east-1.amazonaws.com/576668000072/"  # TODO: env var.

client = boto3.client('sqs',
                           aws_access_key_id=os.environ["aws_access"],
                           aws_secret_access_key=os.environ["aws_secret"], region_name='us-east-1')


q_prefix = "crawl"

response = client.get_queue_url(
    QueueName=f'{q_prefix}_{crawl_id}',
    QueueOwnerAWSAccountId=os.environ["aws_account"]
)

crawl_queue = response["QueueUrl"]

num_families_fetched = 0
families_fetched = []



while num_families_fetched < 2500000:

    sqs_response = client.receive_message(  # TODO: properly try/except this block.
                QueueUrl=crawl_queue,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=1)

    del_list = []
    found_messages = False
    deleted_messages = False

    if ("Messages" in sqs_response) and (len(sqs_response["Messages"]) > 0):

        if num_families_fetched % 10000 == 0:
            print(num_families_fetched)
        get_families_status = "ACTIVE"
        for message in sqs_response["Messages"]:
            num_families_fetched += 1
            message_body = message["Body"]

            family = json.loads(message_body)
            # print(family)
            # family_id = family['family_id']


            families_fetched.append(family)

            del_list.append({'ReceiptHandle': message["ReceiptHandle"],
                             'Id': message["MessageId"]})

        # Step 2. Delete the messages from SQS.
        """
        if len(del_list) > 0:
            client.delete_message_batch(
                QueueUrl=crawl_queue,
                Entries=del_list)
        """
time_to_quit = False
# total_families_processed = 0
# print("Moving into transfer phase... ")

"""
all_families = []
while True:

    if time_to_quit:
        break

    cur_batch_size = 0

    # Create batches of 10k families at a time.
    tdata = globus_sdk.TransferData(transfer_client=tc,
                                    source_endpoint=source_eid,
                                    destination_endpoint=dest_eid)

    # csv.

    for i in range(10000):

        family = families_fetched.get()
        family_id = family['family_id']

        all_families.append(family)

        for file_obj in family['files']:

            old_path = file_obj['path']
            fname = old_path.split('/')[-1]

            tdata.add_item(file_obj['path'], f"/project2/chard/skluzacek/data_to_process/{family_id}/{fname}")
            total_families_processed += 1

            if total_families_processed > 199999:
                time_to_quit = True
                break

    print(f"TRANSFERRING!...{total_families_processed}")
    tc.submit_transfer(tdata)
    time.sleep(5)
"""
print("Writing data to file")
with open("tyler_all_files.json", "w") as f:
    json.dump(families_fetched, f)




