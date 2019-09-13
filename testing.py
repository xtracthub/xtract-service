

# import json
#
# with open('/Users/tylerskluzacek/Desktop/result.json', 'r') as f:
#
#     crawl_data = json.load(f)
#
#
# i = 0
# for key in crawl_data:
#     if '.tif' in key:
#         print(key)
#         i += 1
# print(i)

#################
from funcx.sdk.client import FuncXClient
#
# # TODO: Make a class that represents the directory.
fxc = FuncXClient()
#
import pickle

print("opening file")
# with open('save_file.pkl', 'rb') as f:
#     uuid_list = pickle.load(f)

# success_count = 0
# for id in uuid_list:
#     # print(uuid_list[id])
#     res = fxc.get_task_status(id)
#     print(res)

    # if res['status'] == "SUCCEEDED":
    #     print(id)
    #     success_count += 1
    #     print(success_count)

# Last count 558
# print(success_count)

##################


task_id = "81951d94-22f9-4d2a-a4c9-257186faa48c"
print(fxc.get_task_status(task_id))
