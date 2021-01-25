# This is a questionable test file

from funcx.sdk.client import FuncXClient

fxc = FuncXClient()

location = '039706667969.dkr.ecr.us-east-1.amazonaws.com/xtract-images'
description = 'JSON/XML Extractor'
container_type = 'docker'
name = "xtract/xtract-images"
container_uuid = fxc.register_container(name, location, description, container_type)


func_uuid = fxc.register_function("jsonxml_test", func, "jsonxml_test",
                                  description="A test function for the jsonxml extractor.",
                                  container=container_uuid)
# print(func_uuid)
print(func_uuid)

from fair_research_login import NativeClient

client = NativeClient(client_id='7414f0b4-7d05-4bb6-bb00-076fa3f17cf5')
tokens = client.login(
    requested_scopes=['https://auth.globus.org/scopes/56ceac29-e98a-440a-a594-b41e7a084b62/all'])
auth_token = tokens["petrel_https_server"]['access_token']
headers = {'Authorization': f'Bearer {auth_token}'}

data = {'inputs': []}

payload = {
    # TODO: Add a JSONXML file here.
    'url': 'https://e38ee745-6d04-11e5-ba46-22000b92c6ec.e.globus.org/MDF/mdf_connect/prod/data/nist_ip_v1/interchange/potential.1989--Adams-J-B--Au.xml',
    'headers': headers, 'file_id': "Skrrrrrrt"}
print("Payload is {}".format(payload))

for i in range(2):
    data['inputs'].append(payload)

endpoint_uuid = 'a92945a1-2778-4417-8cd1-4957bc35ce66'  # DLHub endpoint for testing

res_list = []
for i in range(1,2):
    res = fxc.run(data, endpoint_uuid, func_uuid, asynchronous=True)
    res_list.append(res)

print("Waiting for result...")
# result = res.result()
import time
while True:

    for res in res_list:
        print(fxc.get_task_status(res))
    time.sleep(2)