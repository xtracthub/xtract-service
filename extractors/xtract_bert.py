
from funcx.sdk.client import FuncXClient

fxc = FuncXClient()

location = '039706667969.dkr.ecr.us-east-1.amazonaws.com/xtract-bert'
description = 'Xtract text types container using pretrained BERT NER model'
container_type = 'docker'
name = "xtract/bert"
container_uuid = fxc.register_container(name, location, description, container_type)


def bert_extract(event):

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
