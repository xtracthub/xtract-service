# ****** sys path add *******
import sys
import json
sys.path.append('/Users/joaovictor/xtract-service/')

import os
from extractors.xtract_hdf import HDFExtractor
from xtract_sdk.packagers.family import Family
from xtract_sdk.packagers.family_batch import FamilyBatch
from extractors.utils.base_extractor import base_extractor

# ****** JOAO LOCAL TESTS ******
ep_name = "test_hdf_ep"
xtract_dir = "/Users/joaovictor/.xtract"
# Note: this is the following to my local version of the git repo 'xtract-hdf'
sys_path_add = "/Users/joaovictor/xtract-hdf"
module_path = "xtract_hdf_main"  # The file containing 'execute_extractor'
metadata_write_path = "/Users/joaovictor/Desktop/test_metadata"

# HERE WE PACK LOCAL FAMILIES INTO SAME STRUCTURES AS USED BY XTRACT.
test_fam_1 = Family()
test_fam_2 = Family()

base_path = "/Users/joaovictor/xtract-hdf/"
test_fam_1.add_group(files=[{'path': os.path.join(base_path, 'local_test.hdf5'), 'metadata': dict()}], parser=None)
test_fam_1.download_type = "LOCAL"
print(f"[DEBUG] JSON form of our family object: {test_fam_1.to_dict()}")

fam_batch = FamilyBatch()
fam_batch.add_family(test_fam_1)

# # ****** TEST AND RUN THE EXTRACTOR ON YOUR FAMILYBATCH() *******
extractor = HDFExtractor()
event = extractor.create_event(family_batch=fam_batch,
                               ep_name=ep_name,
                               xtract_dir=xtract_dir,
                               module_path=module_path,
                               sys_path_add=sys_path_add,
                               metadata_write_path=metadata_write_path)

metadata = base_extractor(event)
print('Statistics:' + str(metadata))