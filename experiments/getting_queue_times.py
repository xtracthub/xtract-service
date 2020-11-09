

import boto3
import json
import os
import csv

crawl_id = "f74bbed6-b6ab-4656-87b0-c7bdfbfe0803"


# Make sure we can connect to the live test queue url.
client = boto3.client('sqs',
                      aws_access_key_id=os.environ["aws_access"],
                      aws_secret_access_key=os.environ["aws_secret"],
                      region_name='us-east-1')
print(f"Creating queue for crawl_id: {crawl_id}")
test_queue = client.create_queue(QueueName=f"validate_queue_times_1")

# if test_queue["ResponseMetadata"]["HTTPStatusCode"] == 200:
test_queue_url = test_queue["QueueUrl"]


response = client.get_queue_url(
            QueueName=f'validate_{crawl_id}',
            QueueOwnerAWSAccountId=os.environ["aws_account"]
        )
val_queue_url = response["QueueUrl"]


for i in range(100):

    print(i)
    # TODO:  Pull from the crawl_id queue.
    sqs_response = client.receive_message(
        QueueUrl=val_queue_url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=5)

    if len(sqs_response["Messages"]) > 0:
        message = sqs_response["Messages"][0]

        id = message['MessageId']
        body = json.dumps(message['Body'])

        new_msg = {"Id": id, "MessageBody": body}

        x = json.loads(body)

        exit()

        del_info = {'ReceiptHandle': message["ReceiptHandle"],
                                     'Id': message["MessageId"]}

        response1 = client.send_message_batch(QueueUrl=val_queue_url,
                                             Entries=[new_msg])

        response2 = client.send_message_batch(QueueUrl=test_queue_url,
                                             Entries=[new_msg])

        # We need to delete from the real queue because otherwise we'll continuously select same file
        response = client.delete_message_batch(
            QueueUrl=val_queue_url,
            Entries=[del_info])

print("COMPLETED!")
