
import csv

hdf_count = 0
crawl_info = "/Users/tylerskluzacek/Desktop/xpcs_crawl_info.csv"
with open(crawl_info, 'r') as f:
    csv_reader = csv.reader(f)

    for line in csv_reader:
        filename = line[0]

        if not filename.endswith('.hdf'):
            continue
        hdf_count += 1
print(f"HDF count: {hdf_count}")
