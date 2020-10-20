

from utils.fx_utils import invoke_solo_function, post_url
from tests.test_utils.native_app_login import globus_native_auth_login

from funcx import FuncXClient
import time

fxc = FuncXClient()

import requests


def hello_world(event):
    return "HELLO, PREFETCHER!"


class Prefetcher:
    def __init__(self):
        self.fx_eid = "17214422-4570-4891-9831-2212614d04fa"

    def send_prefetch_task(self):

        headers = globus_native_auth_login()

        # Bust this out to a test class.
        func_id = fxc.register_function(function=hello_world, function_name='hw')
        print(func_id)

        #exit()


        task_uuid = invoke_solo_function(event=None, fx_eid=self.fx_eid, headers=headers, func_id=func_id)

        print(task_uuid)

        for i in range(100):
            try:
                x = fxc.get_result(task_uuid)
                print(x)
                time.sleep(3)
                break
            except Exception as e:
                print("Exception: {}".format(e))
                time.sleep(2)

pf = Prefetcher()
pf.send_prefetch_task()