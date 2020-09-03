
from extractors.extractor import Extractor


class TabularExtractor(Extractor):

    def __init__(self):

        super().__init__(extr_id=None,
                         func_id="TODO",
                         extr_name="xtract-todo",
                         store_type="ecr",
                         store_url="039706667969.dkr.ecr.us-east-1.amazonaws.com/xtract-decompress-init:latest")
        super().set_extr_func(decompress_extract)


def decompress_extract(event):

    import sys
    import time

    from xtract_sdk.downloaders.globus_https import GlobusHttpsDownloader
    from xtract_sdk.downloaders.google_drive import GoogleDriveDownloader

    t0 = time.time()

    sys.path.insert(1, '/')
    # from exceptions import RemoteExceptionWrapper, HttpsDownloadTimeout, ExtractorError, PetrelRetrievalError

    new_mdata = None

    family_batch = event["family_batch"]

    downloader = GlobusHttpsDownloader()

    ta = time.time()
    # return family_batch.file_ls
    downloader.batch_fetch(family_batch=family_batch)
    tb = time.time()

    file_paths = downloader.success_files
    # return file_paths

    if len(file_paths) == 0:
        return {'family_batch': family_batch, 'error': True, 'tot_time': time.time()-t0,
                'err_msg': "unable to download files"}

    for family in family_batch.families:
        img_path = family.files[0]['path']
        # return img_path
        new_mdata = xtract_tabular_main.extract_columnar_metadata(img_path)
        family.metadata = new_mdata

    t1 = time.time()
    # Return batch
    return {'family_batch': family_batch, 'tot_time': t1-t0, 'trans_time': tb-ta}


