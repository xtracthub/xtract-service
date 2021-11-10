
from funcx.sdk.utils.batch import Batch
import json
import requests

from utils.routes import fx_submit_url, fx_batch_poll_url


def remote_extract_batch(items_to_batch, ep_id, headers):
    """
    Function to take a bunch of function-event pairs, an endpoint ID, and Auth headers,
    and batch + ship the function to the endpoint.

    :param items_to_batch: (dict) contains 'func_id' (str in UUID4()) and 'event' (dict)
    :param ep_id: the funcX endpoint to send batch.
    :param headers: Globus Auth headers for a user.
    :return:
    """

    batch = Batch()

    for item in items_to_batch:
        func_id = item["func_id"]
        event = item["event"]

        batch.add(event, endpoint_id=ep_id, function_id=func_id)

    data = batch.prepare()

    print(f"Pushing to funcX")
    resp = requests.post(fx_submit_url, json=data, headers=headers)

    try:
        resp_dict = json.loads(resp.content)
        print(f"RESPONSE: {resp_dict}")

    except Exception: # json.JSONDecodeError:  # TODO: bring back.
        error_str = f"Batch response is not valid JSON: {resp.content}"
        print("damn son")
        return {'exception_caught': error_str}

    if resp_dict["status"] == "Success":
        return resp_dict["task_uuids"]


def remote_poll_batch(task_ids, headers):
    statuses = requests.post(url=fx_batch_poll_url, json={"task_ids": task_ids}, headers=headers)

    try:
        return json.loads(statuses.content)["results"]
    except Exception as e:
        print(f"[POLL BATCH] Unable to load content from funcX poll. Caught: {e}")
        print(f"[POLL BATCH] Response received from funcX: {statuses.content}")
        return {'exception_caught': statuses.content}

