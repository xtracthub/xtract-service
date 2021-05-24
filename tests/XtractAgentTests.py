
import os

from xtract_sdk.xtract import XtractAgent
from xtract_sdk.packagers.family import Family
from xtract_sdk.packagers.family_batch import FamilyBatch

xag = XtractAgent(ep_name='tyler_test', xtract_dir='/Users/tylerskluzacek/.xtract')

fam_to_process = Family(download_type='LOCAL', base_url="")
base_path = '/Users/tylerskluzacek/data_folder/413cafa0-9b43-4ffb-9c54-4834dd265a46'
fam_to_process.add_group(files=[{'path': os.path.join(base_path, 'INCAR'), 'metadata': {}},
                                {'path': os.path.join(base_path, 'OUTCAR'), 'metadata': {}},
                                {'path': os.path.join(base_path, 'POSCAR'), 'metadata': {}}],
                         parser='dft')
fam_to_process = fam_to_process.to_dict()

xag.load_family(fam_to_process)
xag.fetch_all_files()

for item in xag.ready_families:
    print(item)
