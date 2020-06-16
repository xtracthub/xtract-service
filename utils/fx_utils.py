
import json
import requests

post_url = 'https://dev.funcx.org/api/v1/submit'

def serialize_fx_inputs(*args, **kwargs):
    from funcx.serialize import FuncXSerializer
    fx_serializer = FuncXSerializer()
    ser_args = fx_serializer.serialize(args)
    ser_kwargs = fx_serializer.serialize(kwargs)
    payload = fx_serializer.pack_buffers([ser_args, ser_kwargs])
    return payload


def invoke_solo_function(event, fx_eid, headers, func_id):
    # print(headers)
    res = requests.post(url=post_url,
                        headers=headers,
                        json={'endpoint': fx_eid,
                              'func': func_id,
                              'payload': serialize_fx_inputs(
                                  event=event)})
    if res.status_code == 200:
        content = json.loads(res.content)
        if "task_uuid" in content:
            task_uuid = content['task_uuid']
            return task_uuid
        else:
            print("NO TASK UUID INSIDE!!!")
            print(content)
    else:
        print("ERROR???")
        print(res.content)
        return res.content
    # return task_uuid

def run_batch_function(func_id, data_ls):
    pass