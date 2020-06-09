

from funcx.sdk.client import FuncXClient

fxc = FuncXClient()

location = '039706667969.dkr.ecr.us-east-1.amazonaws.com/xtract-tabular'
description = 'Xtract image types container'
container_type = 'docker'
name = "xtract/tabular"
container_uuid = fxc.register_container(name, location, description, container_type)


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
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request

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

        # return "here???"

        try:
            new_mdata = xtract_tabular_main.extract_columnar_metadata(file_id)
        except Exception as e:
            return e

    t1 = time.time()
    return {'metadata': new_mdata, 'tot_time': t1-t0, 'trans_time': tb-ta}

