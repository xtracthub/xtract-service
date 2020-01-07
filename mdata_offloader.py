
from flask import Flask, request
from globus_sdk import (TransferClient, AccessTokenAuthorizer)
import json
import psycopg2
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
    eligible_files_query = f"SELECT group_id FROM groups WHERE status='EXTRACTED' and crawl_id='{crawl_id}' LIMIT 2;"
    cur.execute(eligible_files_query)

    mdata_to_transfer = []

    os.makedirs(f'/Users/tylerskluzacek/mdata_to_transfer/{crawl_id}', exist_ok=True)
    for gid in cur.fetchall():
        get_mdata_query = f"SELECT metadata from group_metadata where group_id='{gid[0]}';"
        cur.execute(get_mdata_query)

        mdata = cur.fetchone()
        print(mdata[0])
        mdata_to_transfer.append(mdata[0])

    for item in mdata_to_transfer:
        print(item['group_id'])
        with open(os.path.expanduser(f"~/mdata_to_transfer/{crawl_id}/{item['group_id']}.mdata"), 'w') as f:
            json.dump(item, f)

    tdata = globus_sdk.TransferData(tc, source_endpoint,
                                    dest_endpoint,
                                    label='nice transfer')

    tdata.add_item(os.path.expanduser('~/mdata_to_transfer'), f'/home/ubuntu', recursive=True)

    # Ensure endpoints are activated
    tc.endpoint_autoactivate(source_endpoint)
    tc.endpoint_autoactivate(dest_endpoint)

    submit_result = tc.submit_transfer(tdata)
    return "[200] Submitted"


if __name__ == '__main__':
    application.run(debug=True, port=50001)
