
from extractors.xtract_images import ImageExtractor
from xtract_sdk.packagers.family import Family
from xtract_sdk.packagers.family_batch import FamilyBatch

from test_utils.native_app_login import globus_native_auth_login, do_google_login_flow

from queue import Queue

img_extractor = ImageExtractor()
img_funcid = img_extractor.register_function()

# Image collection
img_1 = "1RbSdH_nI0EHvxFswpl1Qss7CyWXBHo-o"  # SC19 SCC photo
img_2 = "0B5nDSpS9a_3kQ0VaUFU4cGhZa3lnTWZoV2NLclVSejRaQVRV"  # clock photo

fam_1 = Family()
fam_2 = Family()
fam_1.add_group(files=[{"path": img_1, "is_gdoc": False, "mimeType": "image/jpg", "metadata": {}}], parser='image')
fam_2.add_group(files=[{"path": img_2, "is_gdoc": False, "mimeType": "image/jpg", "metadata": {}}], parser='image')

fam_batch = FamilyBatch()
fam_batch.add_family(fam_1)
fam_batch.add_family(fam_2)


headers = globus_native_auth_login()
creds = do_google_login_flow()

print(f"Successfully retrieved creds and headers!")

task_dict = {"active": Queue(), "pending": Queue(), "results": [], "failed": Queue()}

for i in range(1, 30):
    task_id = img_extractor.remote_extract_solo(event={'family_batch': fam_batch, 'creds': creds},
                                                fx_eid="68bade94-bf58-4a7a-bfeb-9c6a61fa5443",
                                                headers=headers)
    task_dict["active"].put(task_id)

import time
import requests
from funcx.serialize import FuncXSerializer
fx_ser = FuncXSerializer()
while True:

    get_url = 'https://api.funcx.org/v1/{}/status'

    if task_dict["active"].empty():
        print("Active task queue empty... sleeping... ")
        time.sleep(0.5)
        break  # This should jump out to main_loop

    cur_tid = task_dict["active"].get()
    print(cur_tid)
    status_thing = requests.get(get_url.format(cur_tid), headers=headers).json()

    if 'result' in status_thing:
        result = fx_ser.deserialize(status_thing['result'])
        # print(result)

        print(result['family_batch'].families[0].metadata)

        print(f"Result: {result}")

    elif 'exception' in status_thing:
        exception = fx_ser.deserialize(status_thing['exception'])
        print(f"Exception: {exception}")
        exception.reraise()

    else:
        task_dict["active"].put(cur_tid)
