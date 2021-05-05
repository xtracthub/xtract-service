
from funcx import FuncXClient
import time
from queue import Queue
from extractors.xtract_matio import matio_extract

fxc = FuncXClient()

# id_list = ['63525bf3-b894-4571-9976-fd675932db46']
# id_list = ['63525bf3-b894-4571-9976-fd675932db46']
# id_list = ['dbc7a749-f689-419c-b114-2f9eb8146496']
id_list = ['c8f24648-6b96-4d58-ac14-93ccf81da12c']

def sleep_func(file_ls):
    import time
    #
    # # for item in file_ls:
    # #     with open(item, 'r') as f:
    # #         f.close()
    #
    # time.sleep(sleep_s)
    return "hello, world!"


# func_id = fxc.register_function(function=sleep_func, function_name='hpdc_sleep_extractor')
container_uuid = fxc.register_container('/home/tskluzac/xtract-matio.img', 'singularity')
print("Container UUID: {}".format(container_uuid))
func_id = fxc.register_function(matio_extract,
                                #ep_id, # TODO: We do not need ep id here
                                container_uuid=container_uuid,
                                description="New sum function defined without")

for fx_id in id_list:

    for i in range(1,10):
        task_id = fxc.run({'event'}, endpoint_id=fx_id, function_id=func_id)

    while True:

        result = fxc.get_batch_result([task_id])
        print(result)

        for tid in result:
            if result[tid]['status'] == 'failed':
                exc = result[tid]['exception']
                exc.reraise()
        time.sleep(2)
