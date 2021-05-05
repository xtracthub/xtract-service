
import csv
from random import sample, seed
import globus_sdk
import time
# import mdf_toolbox
from tests.test_utils.native_app_login import globus_native_auth_login

seed(7)

TRANSFER_TOKEN = globus_native_auth_login()['Transfer']

system = "MIDWAY2"

source_ep = "af7bda53-6d04-11e5-ba46-22000b92c6ec" # Midway
# source_ep = "4f99675c-ac1f-11ea-bee8-0e716405a293"  # Petrel
dest_eps = ["d39468c8-9a48-11eb-92cd-6b08dd67ff48",
            "42510258-9a49-11eb-92cd-6b08dd67ff48",
            "33b0b672-995d-11eb-95ff-491d66228b9d",
            "6e4af7d8-9a49-11eb-92cd-6b08dd67ff48"]

num_robins = len(dest_eps)

authorizer = globus_sdk.AccessTokenAuthorizer(TRANSFER_TOKEN)
tc = globus_sdk.TransferClient(authorizer=authorizer)

csv_path = "/Users/tylerskluzacek/Desktop/hpdc_04_09.csv"

all_families = []
with open(csv_path, 'r') as f1:
    csv_reader = csv.reader(f1)

    for line in csv_reader:
        # Note that line[1] is # files in a family.
        all_families.append(line[0])

# Let's consider only 100k files to start. (seed fixed at 7)
x = sample(all_families, 60000)
batch_size = 15000

tdata_dict = {}
for ep in dest_eps:
    tdata = globus_sdk.TransferData(transfer_client=tc,
                                    source_endpoint=source_ep,
                                    destination_endpoint=ep)
    tdata_dict[ep] = tdata

current_batch_count = 0

# Iterate over indexes. ASSUMES DIVISIBLE BY 4.
file_ls = {"d39468c8-9a48-11eb-92cd-6b08dd67ff48": [],
           "42510258-9a49-11eb-92cd-6b08dd67ff48": [],
           "33b0b672-995d-11eb-95ff-491d66228b9d": [],
           "6e4af7d8-9a49-11eb-92cd-6b08dd67ff48": []}
i = 0
while True:
    # print(i, i+1, i+2, i+3)
    sublist = x[i:i+num_robins]
    print(sublist)

    j = 0
    for dest_ep in tdata_dict:
        print(i, j)
        #time.sleep(0.1)
        cur_item = sublist[j]
        # Now get the path at the destination...
        family_folder = cur_item.split('/')[-1]

        if system == "PETREL":
            cur_item = cur_item.replace('/project2/chard/skluzacek', '')

        dest_path = f"/home/tskluzac/DATA/{family_folder}"
        tdata_dict[dest_ep].add_item(source_path=cur_item,
                                     destination_path=dest_path,
                                     recursive=True,
                                     external_checksum=None)
        file_ls[dest_ep].append(dest_path)
        j += 1

    current_batch_count += 1

    if current_batch_count >= batch_size:

        for dest_ep in tdata_dict:
            tdata = tdata_dict[dest_ep]
            print("SENDING BATCH!")
            tc.submit_transfer(tdata)

            # Now overwrite the tdata.
            tdata_dict[dest_ep] = globus_sdk.TransferData(transfer_client=tc,
                                                        source_endpoint=source_ep,
                                                        destination_endpoint=dest_ep)

        # Now reset the current batch to zero because we're empty...
        current_batch_count = 0
        time.sleep(10)

    i += num_robins
    if i >= 60000:
        import json
        for dest_ep in file_ls:
            with open(f'save_{dest_ep}', 'w') as f:
                json.dump(file_ls[dest_ep], f)
        break
