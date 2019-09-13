

import json

with open('/Users/tylerskluzacek/Desktop/result.json', 'r') as f:

    crawl_data = json.load(f)


counter = 0
size_counter = 0
for fpath in crawl_data:

    if '.tif' in fpath or '.tiff' in fpath or '.png' in fpath:
        counter += 1
        size_counter += crawl_data[fpath]['physical']['size']
        # print(crawl_data[fpath])

print(counter)
print(size_counter)



