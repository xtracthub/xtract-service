
import os
import time
import sys

def get_data_dir_size():
    total_size = 0

    from os.path import join, getsize
    for root, dirs, files in os.walk('/project2/chard/skluzacek/data_to_process'):
        #print root, "consumes",
        total_size += sum(getsize(join(root, name)) for name in files)
        total_size += sum(getsize(join(root, name)) for name in dirs)
        #print "bytes in", len(files), "non-directory files"
        if 'CVS' in dirs:
            dirs.remove('CVS')  # don't visit CVS directories

    return total_size


if __name__ == "__main__": 
    print(get_data_dir_size())
