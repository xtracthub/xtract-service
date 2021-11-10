# ****** sys path add *******
import os
import sys
import pathlib
sys.path.append(str(pathlib.Path(os.getcwd()).parent.parent))

from extractors.xtract_imagesort import ImagesExtractor
from xtract_sdk.packagers.family import Family
from xtract_sdk.packagers.family_batch import FamilyBatch
from extractors.utils.base_extractor import base_extractor

# ****** JOAO LOCAL TESTS ******
ep_name = "test_images_ep"
xtract_dir = "/Users/joaovictor/.xtract"
# Note: this is the following to my local version of the git repo 'xtract-images'
sys_path_add = "/Users/joaovictor/xtract/xtract-images"
module_path = "xtract_images_main"  # The file containing 'execute_extractor'
recursion_depth = 5000
metadata_write_path = "/Users/joaovictor/Desktop/test_metadata"

# HERE WE PACK LOCAL FAMILIES INTO SAME STRUCTURES AS USED BY XTRACT.
test_fam_1 = Family()
test_fam_2 = Family()
base_path = "/Users/joaovictor/xtract/xtract-images/training_data/"
test_fam_1.download_type = "LOCAL"
test_fam_1.add_group(files=[{'path': os.path.join(base_path, 'graphics/social.png'), 'metadata': dict()}], parser=None)
test_fam_1.add_group(files=[{'path': os.path.join(base_path, 'maps/Fig1.jpg.png'), 'metadata': dict()}], parser=None)
test_fam_1.add_group(files=[{'path': os.path.join(base_path, 'maps/311.jpg'), 'metadata': dict()}], parser=None)

print(test_fam_1)

print(f"[DEBUG] JSON form of our family object: {test_fam_1.to_dict()}")

fam_batch = FamilyBatch()
fam_batch.add_family(test_fam_1)

# ****** TEST AND RUN THE EXTRACTOR ON YOUR FAMILYBATCH() *******
extractor = ImagesExtractor()
print(dir(extractor))

event = extractor.create_event(family_batch=fam_batch,
                               ep_name=ep_name,
                               xtract_dir=xtract_dir,
                               module_path=module_path,
                               sys_path_add=sys_path_add,
                               metadata_write_path=metadata_write_path)

metadata = base_extractor(event)
print('Statistics: ' + str(metadata))