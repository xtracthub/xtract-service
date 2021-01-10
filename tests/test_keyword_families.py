
from extractors.xtract_keyword import KeywordExtractor
from xtract_sdk.packagers.family import Family
from xtract_sdk.packagers.family_batch import FamilyBatch

from queue import Queue
from funcx import FuncXClient
from extractors.utils.batch_utils import remote_extract_batch
from test_utils.native_app_login import globus_native_auth_login, do_google_login_flow


fxc = FuncXClient()
fx_batch = fxc.create_batch()

img_extractor = KeywordExtractor()

img_funcid = img_extractor.register_function()

# Image collection
tab_1 = "11s1EWKAdqIgaEC0ksSKunrQq_WgSKyZH6l1yig-lFgA"  # Algorithms study guide gdoc
tab_2 = "1Yr88RgyYWWyuZzemtoI8nSFQLHh_XFb9"  # PDF resume (Lang)

fam_1 = Family()
fam_2 = Family()
fam_1.add_group(files=[{"path": tab_1, "is_gdoc": True, "mimeType": "text/plain", "metadata": {}}], parser='text')
fam_2.add_group(files=[{"path": tab_2, "is_gdoc": False, "mimeType": "application/pdf", "metadata": {}}], parser='text')

# Full version of file throwing error


# fam_2.add_group = {'files': [{'path': '1eTSkC46GUN4z-6mhFTJHsE9Z9EpmllMpCzdm-LLpuCQ', 'metadata': {'id': '1eTSkC46GUN4z-6mhFTJHsE9Z9EpmllMpCzdm-LLpuCQ', 'name': 'DCSL Info Session 1/24', 'mimeType': 'application/vnd.google-apps.presentation', 'webViewLink': 'https://docs.google.com/presentation/d/1eTSkC46GUN4z-6mhFTJHsE9Z9EpmllMpCzdm-LLpuCQ/edit?usp=drivesdk', 'shared': True, 'size': 0, 'is_gdoc': True, 'user_is_owner': False, 'shared_user_ids': ['03530326841615676301', '03711339970616793712', '09490290160418701680', '14453663582181058099', '09932392942899452680', '02136978862942460576', '12120473023176343867', '08122454148025534369'], 'parent': '1Qx9X7LzkMLdrEZNKZDTgQvjaZgxdqQBx', 'last_modified': 1591778133.558, 'extension': '', 'extractor': 'text'}, 'is_gdoc': True, 'mimeType': None, 'file_id': '2ed94934-2e41-49eb-a33f-ebe6698ee5ca'}, {'path': '1MEgpv9BO3yPbX0MPFy617OxM_SeIcIHp03DL3suQ7ZM', 'metadata': {'id': '1MEgpv9BO3yPbX0MPFy617OxM_SeIcIHp03DL3suQ7ZM', 'name': 'Learning Agreement | CDAC Summer Lab', 'mimeType': 'application/vnd.google-apps.document', 'webViewLink': 'https://docs.google.com/document/d/1MEgpv9BO3yPbX0MPFy617OxM_SeIcIHp03DL3suQ7ZM/edit?usp=drivesdk', 'shared': True, 'size': 0, 'is_gdoc': True, 'user_is_owner': False, 'shared_user_ids': ['03530326841615676301', '14453663582181058099', '09932392942899452680', '02136978862942460576', '12120473023176343867', '08122454148025534369'], 'parent': '1hVe6F5v_YduMMycSYKAR2BD7aEVDek0P', 'last_modified': 1591766119.419, 'extension': '', 'extractor': 'text'}, 'is_gdoc': True, 'mimeType': 'text/plain', 'file_id': '5d04e8a3-22eb-4d81-a4d5-60b84c327155'}]}
fam_2.add_group = {'files': [{'path': '1eTSkC46GUN4z-6mhFTJHsE9Z9EpmllMpCzdm-LLpuCQ',
                              'metadata': {},
                              'is_gdoc': True,
                              'mimeType': None,
                              'file_id': '2ed94934-2e41-49eb-a33f-ebe6698ee5ca'},
                             {'path': '1MEgpv9BO3yPbX0MPFy617OxM_SeIcIHp03DL3suQ7ZM',
                              'metadata': {},
                              'is_gdoc': True, 'mimeType': 'text/plain',
                              'file_id': '5d04e8a3-22eb-4d81-a4d5-60b84c327155'}]}


fam_batch = FamilyBatch()
fam_batch.add_family(fam_1)
fam_batch.add_family(fam_2)  # TODO: bring this back.


headers = globus_native_auth_login()
creds = do_google_login_flow()


print(f"Function ID: {img_funcid}")

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
exception_count = 0
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

        try:
            exception.reraise()
        except Exception as e:
            print(e)
            exception_count += 1
            print(f"Exception count: {exception_count}")

    else:
        task_dict["active"].put(cur_tid)
