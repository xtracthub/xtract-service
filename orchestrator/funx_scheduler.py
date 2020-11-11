from funcx.sdk.utils.batch import Batch
from utils.routes import fx_submit_url
import json
import requests


class FuncXScheduler:
    def __init__(self, funcx_eids, funcx_headers):
        """Scheduler for dispatching batches of items across
        multiple FuncX endpoints.

        Parameters
        ----------
        funcx_eids list(str) : List of FuncX endpoints to distribute
        batches across.
        funcx_headers dict(str, str) : Tokens to access FuncX endpoints.
        """
        self.funcx_eids = funcx_eids
        self.funcx_headers = funcx_headers
        self.failed_batches = []
        self.task_uuids = []

    def remote_extract_batch(self, items):
        """Takes items to send to FuncX and evenly distributes them
        amongst multiple endpoints.

        Parameters
        ----------
        items list() : List of items to distribute to FuncX endpoints.
        """
        num_items = len(items)
        num_eids = len(self.funcx_eids)

        for batch_number in range(num_items // num_eids):
            if batch_number < (num_items % num_eids):
                batch_size = (num_items // num_eids) + 1
            else:
                batch_size = (num_items // num_eids)

            batch_items = [items.pop() for _ in range(batch_size)]

            batch = Batch()
            for item in batch_items:
                func_id = item["func_id"]
                event = item["event"]

                batch.add(event, endpoint_id=self.funcx_eids[batch_number], function_id=func_id)

            payload = batch.prepare()
            response = requests.post(fx_submit_url, json=payload, headers=self.funcx_headers)

            try:
                response_dict = json.loads(response.content)
            except Exception:
                print(f"Caught {response.content}")
                self.failed_batches.extend(batch_items)
                continue

            if response_dict["status"] == "Success":
                self.task_uuids.extend(response_dict["task_uuids"])