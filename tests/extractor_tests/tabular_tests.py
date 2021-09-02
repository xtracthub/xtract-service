
import os

from extractors.xtract_tabular import TabularExtractor
from xtract_sdk.packagers.family import Family
from xtract_sdk.packagers.family_batch import FamilyBatch
from extractors.utils.base_extractor import base_extractor

# TYLER LOCAL TESTS
ep_name = "test_tabular_ep"
xtract_dir = "/Users/tylerskluzacek/.xtract"
# Note: this is the following to my local version of the git repo 'xtract-tabular'
sys_path_add = "/Users/tylerskluzacek/xtract-sdk/tests/xtract-tabular"
module_path = "xtract_tabular_main"  # The file containing 'execute_extractor'
recursion_depth = 5000
metadata_write_path = "/Users/tylerskluzacek/Desktop/test_metadata"

# HERE WE PACK LOCAL FAMILIES INTO SAME STRUCTURES AS USED BY XTRACT.
test_fam_1 = Family()
test_fam_2 = Family()

base_path = "/Users/tylerskluzacek/xtract-sdk/tests/xtract-tabular/tests/test_files"
test_fam_1.add_group(files=[{'path': os.path.join(base_path, 'comma_delim'), 'metadata': dict()}], parser=None)
test_fam_1.download_type = "LOCAL"
print(f"[DEBUG] JSON form of our family object: {test_fam_1.to_dict()}")

fam_batch = FamilyBatch()
fam_batch.add_family(test_fam_1)

# ****** JOAO LOCAL TESTS ******
# TODO: Do packing similar to mine above.

# ****** TEST AND RUN THE EXTRACTOR ON YOUR FAMILYBATCH() *******
extractor = TabularExtractor()
event = extractor.create_event(family_batch=fam_batch,
                               ep_name=ep_name,
                               xtract_dir=xtract_dir,
                               module_path=module_path,
                               sys_path_add=sys_path_add,
                               metadata_write_path=metadata_write_path)

metadata = base_extractor(event)
print(metadata)
