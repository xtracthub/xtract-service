# This file contains all of the code for Zoa's compression rate study.

import csv
import time
import os
import shutil
import requests
from fair_research_login import NativeClient

import globus_sdk

from test_decompress import decompress

numfail = 0
skip_count = 20

# This is the Xtract repo on Petrel. 
data_source = "4f99675c-ac1f-11ea-bee8-0e716405a293"
data_dest = "af7bda53-6d04-11e5-ba46-22000b92c6ec"

data_store_path = "/project2/chard/skluzacek/files_to_decomp"

# This is the Jetstream instance
#data_dest = "dbc62586-12f8-11eb-abe1-0213fe609573"

# This is Tyler's macbook (for debuggin)
# data_dest = "1c115272-a3f2-11e9-b594-0e56e8fd6d5a"

# 1. Here we will read a list of compressed files that I previously created for UMich.
#    I should note that there are ~800 GB of compressed data.
with open("UMICH-07-17-2020-CRAWL.csv", "r") as f:
    csv_reader = csv.reader(f, delimiter=',')
    next(csv_reader)

    # # Each row is a list of all elements in order
    # for row in csv_reader:
    #     print(row)

    for _ in range(skip_count):
        next(csv_reader)

    # 2. Authenticate with Globus (just one time) so that we can transfer files here.
    client = NativeClient(client_id='7414f0b4-7d05-4bb6-bb00-076fa3f17cf5')
    tokens = client.login(
        requested_scopes=['https://auth.globus.org/scopes/56ceac29-e98a-440a-a594-b41e7a084b62/all',
                          'urn:globus:auth:scope:transfer.api.globus.org:all',
                          "https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all",
                          "urn:globus:auth:scope:data.materialsdatafacility.org:all",
                          'email', 'openid'],
        no_local_server=True,
        no_browser=True)

    auth_token = tokens["petrel_https_server"]['access_token']
    transfer_token = tokens['transfer.api.globus.org']['access_token']

    headers = {'Authorization': f"Bearer {auth_token}", 'Transfer': auth_token, 'Petrel': auth_token}
    print(f"Headers: {headers}")

    authorizer = globus_sdk.AccessTokenAuthorizer(transfer_token)
    tc = globus_sdk.TransferClient(authorizer=authorizer)

    ### FOR-LOOP ...
    # 3. Scan through the files
    # TYLER: 3/n
    # base_url = "https://4f99675c-ac1f-11ea-bee8-0e716405a293.e.globus.org"

    files_processed = 0
    for row in csv_reader:

        if row[3] != "compressed":
            continue

        petrel_path = row[0]
        file_size = row[1]
        extension = row[2]

        # Filename is the thing after the last '/'
        filename = row[0].split('/')[-1]
        print(f"Retrieving file: {filename}; Size: {file_size}")

        # file_path = base_url + petrel_path

        # 4. Transfer each file (one-at-a-time)

        # TYLER: 1/n Zoa, this is your code. Just commenting out so I can add my own.
        # try:
        #     t_s = time.time()
        #     r = requests.get(file_path, headers=headers)
        #     t_e = time.time()
        # except Exception as e:
        #     print(e)
        #     continue

        tdata = globus_sdk.TransferData(tc,
                                        data_source,
                                        data_dest,
                                        label="Xtract attempt")
                                        #sync_level="checksum")

        cur_dir = os.getcwd()
        new_file_path = os.path.join(data_store_path, filename)

        # new_file_path = new_file_path.replace(")", "")
        # new_file_path = new_file_path.replace("(", "")

        tdata.add_item(petrel_path, new_file_path)

        transfer_result = tc.submit_transfer(tdata)
        tid = transfer_result['task_id']
        # print(transfer_result)
        # exit()

        t0 = time.time()
        while True:
            res = tc.get_task(tid)
            if res['status'] != "SUCCEEDED":
                time.sleep(1)
            else: 
                break
        # Tyler: 2/n
        # print(f"Time to download: {t_e - t_s}")
        # files_processed += 1
        # print(f"Number of files downloaded: {files_processed}")

        # with open(filename, 'wb') as g:
        #     g.write(r.content)

        t1 = time.time()
        print("Successfully retrieved file! ")
        print(f"Total transfer time: {t1-t0}")

        # 5. For each transferred file, collect size/extension information about each file (done above)

        # 6. Decompress the file.
        decomp_folder_name = os.path.join(data_store_path, "decompressed_file")
        compressed_filename = os.path.join(data_store_path, filename)
       
        os.makedirs(decomp_folder_name, exist_ok=True) 
        decompress(os.path.join(data_store_path, filename), decomp_folder_name)
        print(f"Compressed filename: {compressed_filename}")
        print(f"Data decompressed to: {decomp_folder_name}")
        # exit()

        # 7. Collect size of decompressed file
        # decomp_size = os.path.getsize(filename[:-len("." + extension)])
        decomp_size = 0
        start_path = decomp_folder_name  # To get size of current directory
        for path, dirs, files in os.walk(start_path):
            for f in files:
                try:
                    fp = os.path.join(path, f)
                    #fp = fp.replace(')', '\\)')
                    #fp = fp.replace('(', '\\(') 
                    if not os.path.isfile(fp):
                        continue
                    decomp_size += os.path.getsize(fp)
                except FileNotFoundError as e:
                    print(e)
                    numfail += 1
                    print(f"failures: {numfail}")
                    continue

        # 8. Write the info to our CSVs.
        with open('decompression_info.csv', 'a') as csvfile:
            filewriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            filewriter.writerow([petrel_path, extension, file_size, decomp_size])

        print("wrote to csv")

        # 9. Delete the file (from your local computer)
        os.remove(os.path.join(data_store_path, filename))
        shutil.rmtree(decomp_folder_name)

