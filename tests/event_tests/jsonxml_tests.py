# ****** sys path add *******
import os
import sys
import pathlib
sys.path.append(str(pathlib.Path(os.getcwd()).parent.parent))

import os
from extractors.xtract_jsonxml import JsonXMLExtractor
from xtract_sdk.packagers.family import Family
from xtract_sdk.packagers.family_batch import FamilyBatch
from extractors.utils.base_extractor import base_extractor

ep_name = "test_jsonxml_ep"
xtract_dir = "/Users/joaovictor/.xtract"
sys_path_add = "/Users/joaovictor/xtract/xtract-jsonxml"
module_path = "xtract_jsonxml_main"
metadata_write_path = "/Users/joaovictor/Desktop/test_metadata"

# HERE WE PACK LOCAL FAMILIES INTO SAME STRUCTURES AS USED BY XTRACT.
test_fam_1 = Family()
test_fam_2 = Family()

base_path = "/Users/joaovictor/xtract/xtract-jsonxml/test_files"
test_fam_1.add_group(files=[{'path': os.path.join(base_path, 'RY9405.xml'), 'metadata': dict()}], parser=None)
test_fam_1.download_type = "LOCAL"
# print(f"[DEBUG] JSON form of our family object: {test_fam_1.to_dict()}")

fam_batch = FamilyBatch()
fam_batch.add_family(test_fam_1)

# # ****** TEST AND RUN THE EXTRACTOR ON YOUR FAMILYBATCH() *******
extractor = JsonXMLExtractor()
event = extractor.create_event(family_batch=fam_batch,
                               ep_name=ep_name,
                               xtract_dir=xtract_dir,
                               module_path=module_path,
                               sys_path_add=sys_path_add,
                               metadata_write_path=metadata_write_path)

metadata = base_extractor(event)