
from flask import Flask, request
from globus_sdk.exc import GlobusAPIError, TransferAPIError
from globus_sdk import (NativeAppAuthClient, TransferClient, RefreshTokenAuthorizer, AccessTokenAuthorizer)
import json
import psycopg2
import sys
import os
import globus_sdk

application = Flask(__name__)

source_endpoint = '1c115272-a3f2-11e9-b594-0e56e8fd6d5a'
dest_endpoint = '5113667a-10b4-11ea-8a67-0e35e66293c2'

try:
    conn = psycopg2.connect(host=os.environ["XTRACT_DB"], database="xtractdb",
                            user="xtract", port=5432, password=os.environ["XTRACT_PASS"])
except Exception as e:
    print("Cannot connect to database")
    raise e



@application.route('/', methods=['POST'])
def crawl_repo():

    r = request.json

    # endpoint_id = r['eid']
    crawl_id = r['crawl_id']
    transfer_token = r['Transfer']

    print(transfer_token)
    print(f"Beginning processing for crawl_id: {crawl_id}")
    # print(f"Sending data to endpoint ID: {}")

    authorizer = AccessTokenAuthorizer(transfer_token)
    tc = TransferClient(authorizer=authorizer)

    cur = conn.cursor()
    eligible_files_query = f"SELECT group_id FROM groups WHERE status='crawled' and crawl_id='{crawl_id}' LIMIT 5;"
    cur.execute(eligible_files_query)

    mdata_to_transfer = []

    for gid in cur.fetchall():
        get_mdata_query = f"SELECT metadata from group_metadata where group_id='{gid[0]}';"
        cur.execute(get_mdata_query)

        gid = cur.fetchone()
        mdata_to_transfer.append(gid[0])

    # TODO: Start here. 
    os.makedirs(f'mdata_to_transfer/{crawl_id}', exist_ok=True)

    for item in mdata_to_transfer:
        with open(f"{crawl_id}/{item['group_id']}.mdata", 'w') as f:
            json.dump(item, f)

    tdata = globus_sdk.TransferData(tc, source_endpoint,
                                    dest_endpoint,
                                    label='nice transfer')

    f

    tdata.add_item('~/PycharmProjects/xtracthub-service/poop.txt', f'/home/ubuntu/{crawl_id}/poop.txt')

    # Ensure endpoints are activated
    tc.endpoint_autoactivate(source_endpoint)
    tc.endpoint_autoactivate(dest_endpoint)

    submit_result = tc.submit_transfer(tdata)
    print(submit_result)

    return "Nice"

if __name__ == '__main__':
    application.run(debug=True, port=50001)
