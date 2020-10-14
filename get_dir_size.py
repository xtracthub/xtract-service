
import os

total_size = 0 

from os.path import join, getsize
for root, dirs, files in os.walk('/project2/chard/skluzacek/data_to_process'):
    #print root, "consumes",
    total_size += sum(getsize(join(root, name)) for name in files)
    #print "bytes in", len(files), "non-directory files"
    if 'CVS' in dirs:
        dirs.remove('CVS')  # don't visit CVS directories

print(total_size)
