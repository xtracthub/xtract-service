from extractors.extractor import Extractor


class JsonXMLExtractor(Extractor):

    def __init__(self):
        super().__init__(extr_id=None,
                         func_id="",  # TODO: Fill in once func is registered
                         extr_name="xtract-jsonxml",
                         store_type="ecr",
                         store_url="039706667969.dkr.ecr.us-east-1.amazonaws.com/xtract-jsonxml:lateset")
        super().set_extr_func(jsonxml_extract)


def jsonxml_extract(event):
    """Extract metadata from json/xml data.

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

    from xtract_sdk.downloaders.google_drive import GoogleDriveDownloader

    t0 = time.time()

    sys.path.insert(1, '/')
    import xtract_jsonxml_main

    new_mdata = None

    creds = event["creds"]
    family_batch = event["family_batch"]

    # This should be either 'local' or 'remote'.
    extract_mode = event["extract_mode"]

    downloader = GoogleDriveDownloader(auth_creds=creds)
    assert extract_mode in ["remote", "local"], "Invalid extraction mode"

    if extract_mode == "remote":
        ta = time.time()
        downloader.batch_fetch(family_batch=family_batch)
        tb = time.time()

        file_paths = downloader.success_files
    elif extract_mode == "local":
        ta = tb = 0  # Set both times to be zero so that transfer time is zero.
        file_paths = []

    if len(file_paths) == 0 and file_paths == "remote":
        return {'family_batch': family_batch, 'error': True, 'tot_time': time.time() - t0,
                'err_msg': "unable to download files"}

    for family in family_batch.families:
        img_path = family.files[0]['path']
        # return img_path

        new_mdata = xtract_jsonxml_main.extract_json_metadata(img_path)
        family.metadata = new_mdata

    if extract_mode == "remote":
        [os.remove(file_path) for file_path in downloader.success_files]

    t1 = time.time()
    # Return batch
    return {'family_batch': family_batch, 'tot_time': t1 - t0, 'trans_time': tb - ta}

