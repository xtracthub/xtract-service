from extractors.extractor import Extractor
from extractors.utils.base_event import create_event


class HDFExtractor(Extractor):

    def __init__(self):
        super().__init__(extr_id=None,
                         func_id="",  # TODO: Fill in once func is registered
                         extr_name="xtract-hdf",
                         store_type="ecr",
                         store_url="") # TODO: Fill in once container is pushed)

    def create_event(self,
                     family_batch,
                     ep_name,
                     xtract_dir,
                     sys_path_add,
                     module_path,
                     metadata_write_path):

        event = create_event(family_batch=family_batch,
                             ep_name=ep_name,
                             xtract_dir=xtract_dir,
                             sys_path_add=sys_path_add,
                             module_path=module_path,
                             metadata_write_path=metadata_write_path,
                             writer='json')

        return event
        # super().set_extr_func(hdf_extract)


# def hdf_extract(event):
#     """Extract metadata from hdf data.

#     Parameters
#     ----------
#     event : dict
#         A dict describing the data and credentials to act on

#     Returns
#     -------
#     dict : The resulting metadata and timers
#     """
#     import sys
#     import time
#     import os

#     t0 = time.time()

#     sys.path.insert(1, '/')
#     import xtract_hdf_main

#     new_mdata = None

#     # TODO: add check for file existence.
#     # TODO: batching (via Xtract_sdk)
#     family = event
#     h5_path = event['files'][0]  # These are all of size 1.

#     new_mdata = xtract_hdf_main.extract_hdf_main(h5_path)
#     return new_mdata
