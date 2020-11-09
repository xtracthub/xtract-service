
import os
import math
import boto3
import json

crawl_id = "7aff79a5-3536-4cc8-9092-37769056141c"


total_messages = 200000


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


for i in range(math.ceil(total_messages/10)):

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
            print(family)

            crawl_timestamp = family['metadata']['crawl_timestamp']
            
            # total_files = 0
            total_file_size = 0
            
            
            all_files = family['files']
            total_files = len(all_files)
            for file_obj in all_files:
                total_file_size += file_obj['metadata']['physical']['size']
                # print(file_size)


            print(total_file_size)
            print(total_files)
            exit()

        del_info = {'ReceiptHandle': message["ReceiptHandle"],
                    'Id': message["MessageId"]}
        delete_batch.append(del_info) 

        

        #response1 = client.send_message_batch(QueueUrl=test_queue_url,
        #                                      Entries=message_batch)

        #response2 = client.send_message_batch(QueueUrl=val_queue_url,
        #                                      Entries=message_batch)

        # We need to delete from the real queue because otherwise we'll continuously select same file
        response = client.delete_message_batch(
            QueueUrl=val_queue_url,
            Entries=delete_batch)

