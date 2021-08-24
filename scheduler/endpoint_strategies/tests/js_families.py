import os

base_path = '/home/tskluzac/test_files'

hdf_path = os.path.join(base_path, 'xtract-hdf')

hdf_1_path = os.path.join(hdf_path, 'dataset_comp_image_spectra.h5')
sample_hdf_1 = {'family_id': 'sample_hdf_1',
                'files': [hdf_1_path],
                'groups':
                    {'group_id': 1, 'parser': 'implicit', 'files': [{'path': hdf_1_path}]}
                }