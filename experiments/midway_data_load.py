
import globus_sdk
import json
import boto3
import time
import csv
import os
from queue import Queue
from tests.test_utils.native_app_login import globus_native_auth_login

# Get auth creds

headers = globus_native_auth_login()

# Connect to AWS
# crawl_id = "6f2defa8-e2d8-437a-b4da-fe35dc0ac7ac"

source_eid = "e38ee745-6d04-11e5-ba46-22000b92c6ec"
dest_eid = "3f72418a-5cfc-11eb-a468-0e095b4c2e55"


# Connect to Globus
authorizer = globus_sdk.AccessTokenAuthorizer(headers['Transfer'])
tc = globus_sdk.TransferClient(authorizer=authorizer)

# # connect to enormous crawl queue and pull down 200k families.
# # This creates the extraction and validation queues on the Simple Queue Service.
# sqs_base_url = "https://sqs.us-east-1.amazonaws.com/576668000072/"  # TODO: env var.
#
# client = boto3.client('sqs',
#                            aws_access_key_id=os.environ["aws_access"],
#                            aws_secret_access_key=os.environ["aws_secret"], region_name='us-east-1')
#
#
# q_prefix = "crawl"
#
# response = client.get_queue_url(
#     QueueName=f'{q_prefix}_{crawl_id}',
#     QueueOwnerAWSAccountId=os.environ["aws_account"]
# )
#
# crawl_queue = response["QueueUrl"]

with open('tyler_20k.json', 'r') as f:
    ty_20k = json.load(f)


num_families_fetched = 0
families_fetched = Queue()

for item in ty_20k:
    families_fetched.put(item)

    del_list = []
    found_messages = False
    deleted_messages = False


time_to_quit = False
total_families_processed = 0
print("Moving into transfer phase... ")


all_families = []

num_under_1mb = 0
under_1mb_ls = []

while True:

    if time_to_quit:
        break

    cur_batch_size = 0

    # Create batches of 10k families at a time.
    tdata = globus_sdk.TransferData(transfer_client=tc,
                                    source_endpoint=source_eid,
                                    destination_endpoint=dest_eid)

    for i in range(2000):

        family = families_fetched.get()
        family_id = family['family_id']

        all_families.append(family)

        family_size = 0
        for file_obj in family['files']:

            family_size += file_obj['metadata']['physical']['size']

            old_path = file_obj['path']
            fname = old_path.split('/')[-1]

            # tdata.add_item(file_obj['path'], f"/home/tskluzac/data_to_process/{family_id}/{fname}")
            total_families_processed += 1

            if total_families_processed > 20005:
                time_to_quit = True
                break

        f_mb = family_size/1024/1024
        print(f"Family size (MB): {f_mb}")

        if f_mb < 0.0025:
            num_under_1mb += 1
            under_1mb_ls.append(family)
            under_1mb_ls.append(family)
            under_1mb_ls.append(family)
            under_1mb_ls.append(family)
            under_1mb_ls.append(family)
        # if f_mb < 0.005:
        #     num_under_2mb += 1

    print(f"TRANSFERRING!...{total_families_processed}")
    # tc.submit_transfer(tdata)
    # time.sleep(5)

print(num_under_1mb)
# print(num_under_2mb)

print("Writing data to file")
with open("tyler_2p5kb.json", "w") as f:
    json.dump(all_families, f)
