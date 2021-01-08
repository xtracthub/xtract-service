
import json

with open('tyler_200k.json', 'r') as f:
    data_ls = json.load(f)


new_files = []

counter = 0
for file in data_ls:
    if counter > 20000:
        break
    new_files.append(file)
    counter += 1

with open('tyler_20k.json', 'w') as g:
    json.dump(new_files, g)