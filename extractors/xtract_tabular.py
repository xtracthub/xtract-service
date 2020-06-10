
from extractors.extractor import Extractor

tab_extr = Extractor(extr_id=None,
                     func_id=None,
                     extr_name="xtract-tabular",
                     store_type="ecr",
                     store_url="039706667969.dkr.ecr.us-east-1.amazonaws.com/xtract-tabular:latest")


def tabular_extract(event):
    import io
    import os
    import sys
    import time
    import pickle
    import shutil
    import requests
    import tempfile
    import threading



    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload

    t0 = time.time()
    # from shutil import copyfile

    try:
        sys.path.insert(1, '/')
        import xtract_tabular_main
        # from exceptions import RemoteExceptionWrapper, HttpsDownloadTimeout, ExtractorError, PetrelRetrievalError

    except Exception as e:
        return e

    # A list of file paths
    all_families = event['inputs']
    dir_name = tempfile.mkdtemp()
    os.chdir(dir_name)

    def generate_drive_connection(creds):
        service = build('drive', 'v3', credentials=creds)
        return service

    d_type = "GDRIVE"

    new_mdata = None
    if d_type is "GDRIVE":

        try:

            creds = event["gdrive"]
            file_id = event["file_id"]
            is_gdoc = event["is_gdoc"]
            mimeType = "text/csv"
        # TODO: These DO NOT need to c
        except Exception as e:
            return e
        try:
            service = generate_drive_connection(creds)
            ta = time.time()

            try:
                if is_gdoc:
                    request = service.files().export(fileId=file_id, mimeType=mimeType)
                else:
                    request = service.files().get_media(fileId=file_id)
            except Exception as e:
                return f"[Xtract] Was unable to launch Google service request: {e}"

            fh = io.FileIO(file_id, 'wb')
            downloader = MediaIoBaseDownload(fh, request)
        except Exception as e:
            return f"[Xtract] Unable to perform MediaBaseDownload: {e}"

        try:
            done = False
            while done is False:
                status, done = downloader.next_chunk()
                print("Download %d%%." % int(status.progress() * 100))
        except Exception as e:
            return e

        tb = time.time()

        try:
            new_mdata = xtract_tabular_main.extract_columnar_metadata(file_id)
        except Exception as e:
            return e

    t1 = time.time()
    return {'metadata': new_mdata, 'tot_time': t1-t0, 'trans_time': tb-ta}


tab_extr.set_extr_func(tabular_extract)

print(tab_extr.extr_func("potato"))


