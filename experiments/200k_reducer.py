
import json

with open('/Users/tylerskluzacek/Desktop/tyler_everything.json', 'r') as f:
    data_ls = json.load(f)


new_files = []

seed = 0
counter = 0
for family in data_ls:
    if counter > 30000:
        break
    seed += 1

    # Add some level of randomization
    if seed % 10 != 0:
        continue

    new_files.append(family)
    counter += 1

with open('tyler_30k.json', 'w') as g:
    json.dump(new_files, g)
