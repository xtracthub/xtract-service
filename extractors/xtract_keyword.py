
from extractors.extractor import Extractor


class KeywordExtractor(Extractor):

    def __init__(self):

        super().__init__(extr_id=None,
                         func_id="d5a09aee-927f-4d6e-9ee4-5ad461d9148b",
                         extr_name="xtract-keyword",
                         store_type="ecr",
                         store_url="039706667969.dkr.ecr.us-east-1.amazonaws.com/xtract-keyword:latest")
        super().set_extr_func(keyword_extract)

# def execute_extractor():
# event is a 'context' for an extractor to run on

def keyword_extract(event):

    import sys
    import time

    from xtract_sdk.downloaders import GoogleDriveDownloader

    t0 = time.time()

    sys.path.insert(1, '/')
    import xtract_keyword_main
    # from exceptions import RemoteExceptionWrapper, HttpsDownloadTimeout, ExtractorError, PetrelRetrievalError

    creds = event["creds"]
    family_batch = event["family_batch"]

    downloader = GoogleDriveDownloader(auth_creds=creds)

    ta = time.time()
    try:
        downloader.batch_fetch(family_batch=family_batch)
    except Exception as e:
        return {'exception': e, 'files': family_batch.file_ls}
    tb = time.time()

    file_paths = downloader.success_files

    if len(file_paths) == 0:
        return {'family_batch': family_batch, 'error': True, 'tot_time': time.time()-t0,
                'err_msg': "unable to download files"}

    for family in family_batch.families:
        keyword_mimetype = family.files[0]['mimeType']
        is_pdf = True if 'pdf' in keyword_mimetype.lower() else False
        file_path = family.files[0]['path']

        new_mdata = xtract_keyword_main.extract_keyword(file_path, pdf=is_pdf)

        # TODO: move this sort of thing
        # Here we remove the empty metadata.
        if type(new_mdata) is dict():
            if len(new_mdata['keywords']) == 0:
                new_mdata = {}
        if new_mdata is None:
            new_mdata = {}

        family.metadata = new_mdata

    # TODO: obliterate downloaded files here.

    t1 = time.time()
    return {'family_batch': family_batch, 'tot_time': t1-t0, 'trans_time': tb-ta}
