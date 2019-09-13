
import pickle

from funcx.sdk.client import FuncXClient
#
# # TODO: Make a class that represents the directory.
fxc = FuncXClient()

# with open('csv_files.pkl', 'rb') as f:
#     uuid_list = pickle.load(f)
#
# # active_count = 0
# total_size = 0
# finished_count = 0
# for id in uuid_list:
#     res = fxc.get_task_status(id)
#     print(res)
#
#     try:
#         if 'extract time' in res['details']['result']:
#                 extract_time = res['details']['result']['extract time']
#
#                 total_size += extract_time
#                 finished_count += 1
#
#     except:
#         print("OOPS")
#
# print(total_size)
#
#
# print(total_size / finished_count)

##################################################

with open('images_2595.pkl', 'rb') as f:
    uuid_list = pickle.load(f)

# active_count = 0
total_size = 0
finished_count = 0
for batch_id in uuid_list:
    res = fxc.get_task_status(batch_id)

    try:
        for item in res['details']['result']['metadata']:
            # print(item['extract time'])
            extract_time = item['extract time']
            total_size += extract_time
            finished_count += 1
            print(finished_count)
    except:
        pass

# Actual number: HIGHER.
print(total_size)
print(finished_count)

print(total_size / finished_count)


# 2580
# 50.6655912399292
# 2580
# 0.01963782606198806