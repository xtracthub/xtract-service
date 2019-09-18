

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


import json
import time

t0 = time.time()
with open('/Users/tylerskluzacek/Desktop/relax.json', 'r') as f:
    data = json.load(f)

field_dict = {'keys': []}
for key in data:
    field_dict['keys'].append(key)

t1 = time.time()

field_dict['extract_time'] = t1-t0

print(field_dict)



