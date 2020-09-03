import funcx


fxc = funcx.FuncXClient()
print(fxc.base_url)

def hello_world():
    return "Hello World!"

func_uuid = fxc.register_function(hello_world)
print(func_uuid)

tutorial_endpoint = '68bade94-bf58-4a7a-bfeb-9c6a61fa5443'
res = fxc.run(endpoint_id=tutorial_endpoint, function_id=func_uuid)
print(res)

import time
while True:
    try:
        res = fxc.get_result(res)
        print(res)

    except Exception as e:
        print(e)

    time.sleep(2)



