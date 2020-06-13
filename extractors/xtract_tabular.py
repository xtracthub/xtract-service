
from extractors.extractor import Extractor


class TabularExtractor(Extractor):

    def __init__(self):

        super().__init__(extr_id=None,
                         func_id="7ce87fc6-d4bb-4cd5-be01-4ac1ddbd5ba8",
                         extr_name="xtract-tabular",
                         store_type="ecr",
                         store_url="039706667969.dkr.ecr.us-east-1.amazonaws.com/xtract-tabular:latest")
        super().set_extr_func(tabular_extract)


def tabular_extract(event):

    import sys
    import time

    from xtract_sdk.downloaders.globus_https import GlobusHttpsDownloader
    from xtract_sdk.downloaders.google_drive import GoogleDriveDownloader

    t0 = time.time()

    sys.path.insert(1, '/')
    import xtract_tabular_main
    # from exceptions import RemoteExceptionWrapper, HttpsDownloadTimeout, ExtractorError, PetrelRetrievalError

    new_mdata = None

    creds = event["gdrive"]
    file_id = event["file_id"]
    is_gdoc = event["is_gdoc"]
    mimeType = "text/csv"

    ta = time.time()
    downloader = GoogleDriveDownloader(auth_creds=creds)
    if is_gdoc:
        downloader.fetch(fid=file_id, download_type="export", mimeType=mimeType)
    else:
        downloader.fetch(fid=file_id, download_type="media")
    tb = time.time()

    file_paths = downloader.success_files

    for path in file_paths:
        try:
            new_mdata = xtract_tabular_main.extract_columnar_metadata(path)
        except Exception as e:
            return e

    t1 = time.time()
    return {'metadata': new_mdata, 'tot_time': t1 - t0, 'trans_time': tb - ta}
