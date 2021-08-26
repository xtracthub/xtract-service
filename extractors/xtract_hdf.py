from extractors.extractor import Extractor


class HDFExtractor(Extractor):

    def __init__(self):
        super().__init__(extr_id=None,
                         func_id="",  # TODO: Fill in once func is registered
                         extr_name="xtract-hdf",
                         store_type="ecr",
                         store_url="") # TODO: Fill in once container is pushed)
        super().set_extr_func(hdf_extract)


def hdf_extract(event):
    """Extract metadata from hdf data.

    Parameters
    ----------
    event : dict
        A dict describing the data and credentials to act on

    Returns
    -------
    dict : The resulting metadata and timers
    """
    import sys
    import time
    import os

    t0 = time.time()

    sys.path.insert(1, '/')
    import xtract_hdf_main

    new_mdata = None

    # TODO: add check for file existence.
    # TODO: batching (via Xtract_sdk)
    family = event
    h5_path = event['files'][0]  # These are all of size 1.

    new_mdata = xtract_hdf_main.extract_hdf_main(h5_path)
    return new_mdata
