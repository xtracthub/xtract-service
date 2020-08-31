
from funcx.sdk.utils.batch import Batch
import json
import requests

from utils.routes import fx_submit_url, fx_batch_poll_url


def remote_extract_batch(items_to_batch, ep_id, headers):
    batch = Batch()

    for item in items_to_batch:
        func_id = item["func_id"]
        event = item["event"]
        batch.add(event, endpoint_id=ep_id, function_id=func_id)

    data = batch.prepare()
    resp = requests.post(fx_submit_url, json=data, headers=headers)

    try:
        resp_dict = json.loads(resp.content)
    except json.JSONDecodeError:
        error_str =  f"Batch response is not valid JSON: {resp.content}"
        print(error_str)
        return error_str

    if resp_dict["status"] == "Success":
        return resp_dict["task_uuids"]


def remote_poll_batch(task_ids, headers):
    statuses = requests.post(url=fx_batch_poll_url, json={"task_ids": task_ids}, headers=headers)
    return json.loads(statuses.content)["results"]
