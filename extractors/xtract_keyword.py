
from extractors.extractor import Extractor


class KeywordExtractor(Extractor):

    def __init__(self):

        super().__init__(extr_id=None,
                         func_id="48777905-453f-41c1-91ca-b563b9b5ded3",
                         extr_name="xtract-keyword",
                         store_type="ecr",
                         store_url="039706667969.dkr.ecr.us-east-1.amazonaws.com/xtract-keyword:latest")
        super().set_extr_func(keyword_extract)


def keyword_extract(event):

    import sys
    import time

    from xtract_sdk.downloaders.globus_https import GlobusHttpsDownloader
    from xtract_sdk.downloaders.google_drive import GoogleDriveDownloader


    t0 = time.time()

    sys.path.insert(1, '/')

    import xtract_keyword_main
    # except Exception as e:
    #     return e
    # print("BUT NO HERE?")


    new_mdata = None

    creds = event["gdrive"]
    file_id = event["file_id"]
    is_gdoc = event["is_gdoc"]
    extension = event["extension"]

    if extension.lower() == "pdf":
        mimeType = "application/pdf"
    else:
        mimeType = "text/plain"

    ta = time.time()
    try:
        downloader = GoogleDriveDownloader(auth_creds=creds)
        if is_gdoc:
            downloader.fetch(fid=file_id, download_type="export", mimeType=mimeType)
        else:
            downloader.fetch(fid=file_id, download_type="media")

    except Exception as e:
        return e
    tb = time.time()

    for filepath in downloader.success_files:
        try:
            new_mdata = xtract_keyword_main.extract_keyword(filepath, pdf=True if extension.lower() == 'pdf' else False)
        except Exception as e:
            return e

    t1 = time.time()
    return {'metadata': new_mdata, 'tot_time': t1-t0, 'trans_time': tb-ta}
