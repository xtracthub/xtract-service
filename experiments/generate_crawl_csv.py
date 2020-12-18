
import os
import math
import boto3
import json
import csv

crawl_id = "f023ace7-a127-434a-bfaa-15f4e734ed2b"


total_messages = 212369900


client = boto3.client('sqs',
                      aws_access_key_id=os.environ["aws_access"],
                      aws_secret_access_key=os.environ["aws_secret"],
                      region_name='us-east-1')


# if test_queue["ResponseMetadata"]["HTTPStatusCode"] == 200:
# test_queue_url = test_queue["QueueUrl"]


response = client.get_queue_url(
            QueueName=f'crawl_{crawl_id}',
            QueueOwnerAWSAccountId=os.environ["aws_account"]
        )
val_queue_url = response["QueueUrl"]

with open("crawl_data.csv", "w") as f:
    writer = csv.writer(f)

    for i in range(math.ceil(total_messages/10)):

        if i % 1000 == 0: 
            print(i) 
        # print(i)
        # TODO:  Pull from the crawl_id queue.
        sqs_response = client.receive_message(
            QueueUrl=val_queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=5)

        delete_batch = []
        message_batch = []
        if len(sqs_response["Messages"]) > 0:

            for i in range(len(sqs_response["Messages"])):
                #if i%1000 == 0: 
                #    print(i)
                # print(i)
                message = sqs_response["Messages"][i]

                id = message['MessageId']
                body = message['Body']

                # print(body)

                # print(type(body))
                # exit()

                new_msg = {"Id": id, "MessageBody": body}
                message_batch.append(new_msg)
                family = json.loads(body)
                #print(family)

                crawl_timestamp = family['metadata']['crawl_timestamp']
                family_id = family['family_id']

                # total_files = 0
                total_file_size = 0


                all_files = family['files']
                total_files = len(all_files)
                for file_obj in all_files:
                    total_file_size += file_obj['metadata']['physical']['size']
                    # print(file_size)


                #print(total_file_size)
                #print(total_files)

                writer.writerow([family_id, crawl_timestamp, total_files, total_file_size])

                # exit()

                del_info = {'ReceiptHandle': message["ReceiptHandle"],
                            'Id': message["MessageId"]}
                delete_batch.append(del_info)

            # We need to delete from the real queue because otherwise we'll continuously select same file
            response = client.delete_message_batch(
                QueueUrl=val_queue_url,
                Entries=delete_batch)

