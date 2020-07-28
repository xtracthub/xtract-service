
from funcx import FuncXClient

fxc = FuncXClient()


# TODO: Batching happens external to the extractors
def remote_extract_batch(items_to_batch, ep_id):
    batch = fxc.create_batch()

    for item in items_to_batch:
        func_id = item["func_id"]
        event = item["event"]

        print(func_id)
        print(event)

        batch.add(event, endpoint_id=ep_id, function_id=func_id)
    task_ids = fxc.batch_run(batch)
    return task_ids


def remote_poll_batch(task_ids):
    statuses = fxc.get_batch_status(task_ids)
    return statuses


# TODO: Add util somewhere for creating a funcX batch.