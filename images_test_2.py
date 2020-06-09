
import funcx
import time
import json
import os
import requests
from container_lib.xtract_matio import serialize_fx_inputs
from container_lib.xtract_images import images_extract
from fair_research_login import NativeClient
from funcx.serialize import FuncXSerializer
from queue import Queue
from mdf_matio.validator import MDFValidator
from materials_io.utils.interface import ParseResult
from typing import Iterable, Set, List
from functools import reduce, partial
from mdf_matio.grouping import groupby_file, groupby_directory


from mdf_toolbox import dict_merge


# Standard Py Imports
import os
import json
import pickle
import requests

fxc = funcx.FuncXClient()
fx_ser = FuncXSerializer()


post_url = 'https://dev.funcx.org/api/v1/submit'

fx_ep = "82ceed9f-dce1-4dd1-9c45-6768cf202be8"
fn_id = "0671c0ef-d9d4-4382-89f1-6f4e56f45720"

headers = {
    'Authorization': 'Bearer AgK8Dkvx90XY5qX2Vajxn0boaoedlOyJaXbVvmWjnaoybo3QlMieCQz48e67PydOVmpke1734kkb8vC1NMymVu7w3n',
    'Transfer': 'Ag8496kzod33gmepjDNXVxNYVP2wkJeYO1rnVqpjgnyEPXxa15I8Cv9wnGGQaxbed4YmXVYBKD9wyKt7KPgNvcoqlv',
    'FuncX': 'AgK8Dkvx90XY5qX2Vajxn0boaoedlOyJaXbVvmWjnaoybo3QlMieCQz48e67PydOVmpke1734kkb8vC1NMymVu7w3n',
    'Petrel': 'AgmJJ674z4wEvj1o4Jmwar78kDj2QrkdVxaKkeaDD6pgkEJjlyH8C0zbkEvq3GKWgykEydwGGJG1qVC8rnE7XHgGEJ'}
t_launch_times = {}
task_dict = {"active": Queue(), "pending": Queue(), "results": [], "failed": Queue()}

# data["gdrive_pkl"] = pickle.dumps(auth_creds)
# data["file_id"] = "1zAJJy4bFQ2ZANV7W3iv7WdpZWRBp8iEN"

data = {'gdrive_pkl': b'\x80\x03cgoogle.oauth2.credentials\nCredentials\nq\x00)\x81q\x01}q\x02(X\x05\x00\x00\x00tokenq\x03X\xab\x00\x00\x00ya29.a0AfH6SMDkmvH9GI9EL6eKYaQo5OUF4c9Um3oIqgel5IN-Fc9nJqIbke9PTGPc3BuJvWhSuKuWT9NnXZmy9PIN1xMi7rkhCH_gY8bQttlPUEkBJ7HoKi_xPj1nU_pd3qQMnQvyV7oi_CeAaf_LTBrsK22x--WK7wA5cbjFq\x04X\x06\x00\x00\x00expiryq\x05cdatetime\ndatetime\nq\x06C\n\x07\xe4\x06\x08\x152\x1f\x08\x89\x05q\x07\x85q\x08Rq\tX\x0e\x00\x00\x00_refresh_tokenq\nXg\x00\x00\x001//0fYtW4N1qkLF2CgYIARAAGA8SNwF-L9Ir8QaekPkzMwhqN8orPQxeV_ayzLVwFmIHDsW4fmVRU_pj2zJZew1j3dqSY_XywTP_S3gq\x0bX\t\x00\x00\x00_id_tokenq\x0cNX\x07\x00\x00\x00_scopesq\r]q\x0e(X7\x00\x00\x00https://www.googleapis.com/auth/drive.metadata.readonlyq\x0fX%\x00\x00\x00https://www.googleapis.com/auth/driveq\x10eX\n\x00\x00\x00_token_uriq\x11X#\x00\x00\x00https://oauth2.googleapis.com/tokenq\x12X\n\x00\x00\x00_client_idq\x13XH\x00\x00\x00364500245041-r1eebsermd1qp1qo68a3qp09hhpc5dfi.apps.googleusercontent.comq\x14X\x0e\x00\x00\x00_client_secretq\x15X\x18\x00\x00\x00XMGaOYAeBxLz9ZWvsVQCvWtkq\x16X\x11\x00\x00\x00_quota_project_idq\x17NubN\x86q\x18.', 'file_id': '1zAJJy4bFQ2ZANV7W3iv7WdpZWRBp8iEN'}


res = requests.post(url=post_url,
                            headers=headers,
                            json={'endpoint': fx_ep,
                                  'func': fn_id,
                                  'payload': serialize_fx_inputs(
                                      event=data)
                                  }
                            )

print(res.content)
if res.status_code == 200:
    task_uuid = json.loads(res.content)['task_uuid']
    task_dict["active"].put(task_uuid)
    t_launch_times[task_uuid] = time.time()