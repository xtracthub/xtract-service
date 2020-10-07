# This file contains all of the code for Zoa's compression rate study.

import csv
import time
import os
import requests
from fair_research_login import NativeClient

from test_decompress import decompress

# 1. Here we will read a list of compressed files that I previously created for UMich.
#    I should note that there are ~800 GB of compressed data.
with open("UMICH-07-17-2020-CRAWL.csv", "r") as f:
    csv_reader = csv.reader(f, delimiter=',')
    next(csv_reader)

    # # Each row is a list of all elements in order
    # for row in csv_reader:
    #     print(row)

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

    ### FOR-LOOP ...
    # 3. Scan through the files
    base_url = "https://4f99675c-ac1f-11ea-bee8-0e716405a293.e.globus.org"

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

        file_path = base_url + petrel_path

        # 4. Transfer each file (one-at-a-time)
        try:
            t_s = time.time()
            r = requests.get(file_path, headers=headers)
            t_e = time.time()
        except Exception as e:
            print(e)
            continue

        print(f"Time to download: {t_e - t_s}")
        files_processed += 1
        print(f"Number of files downloaded: {files_processed}")

        with open(filename, 'wb') as g:
            g.write(r.content)

        print("successfully retrieved file! ")

        # 5. For each transferred file, collect size/extension information about each file (done above)

        # 6. Decompress the file.
        decompress(filename, "compress_folder")

        # 7. Collect size of decompressed file
        # decomp_size = os.path.getsize(filename[:-len("." + extension)])
        decomp_size = 0
        start_path = 'compress_folder'  # To get size of current directory
        for path, dirs, files in os.walk(start_path):
            for f in files:
                fp = os.path.join(path, f)
                decomp_size += os.path.getsize(fp)

        # 8. Write the info to our CSVs.
        with open('decompression_info.csv', 'w') as csvfile:
            filewriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            filewriter.writerow([file_path, extension, file_size, decomp_size])

        print("wrote to csv")

        # 9. Delete the file (from your local computer)

        # os.remove(filename)

