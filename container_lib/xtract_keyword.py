
from funcx.sdk.client import FuncXClient

fxc = FuncXClient()

location = '039706667969.dkr.ecr.us-east-1.amazonaws.com/xtract-keyword'
description = 'Xtract keyword types container'
container_type = 'docker'
name = "xtract/keyword"
container_uuid = fxc.register_container(name, location, description, container_type)


def keyword_extract(event):
    import io
    import os
    import sys
    import time
    import pickle
    import shutil
    import requests
    import tempfile
    import threading
    from urllib.error import HTTPError



    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request

    # return "imported everything!"

    t0 = time.time()

    try:
        sys.path.insert(1, '/')
        import xtract_keyword_main
        # from exceptions import RemoteExceptionWrapper, HttpsDownloadTimeout, ExtractorError, PetrelRetrievalError

    except Exception as e:
        return e

    try:
        # A list of file paths
        # all_families = event['inputs']
        dir_name = tempfile.mkdtemp()
        os.chdir(dir_name)
    except Exception as e:
        return e

    # TODO: Start here!

    def generate_drive_connection(creds):
        service = build('drive', 'v3', credentials=creds)
        return service

    d_type = "GDRIVE"

    new_mdata = None
    if d_type is "GDRIVE":
        try:
            creds = event["gdrive"]
        except Exception as e:
            return e

        try:
            file_id = event["file_id"]
            extension = event["extension"]

            if extension.lower() == "pdf":
                mimeType = "application/pdf"
            else:
                mimeType = "text/plain"
        except Exception as e:
            return f"HOW THE FUCK DID THIS FAIL?: {e}"


        # return mimeType
        try:
            service = generate_drive_connection(creds)
            ta = time.time()

            try:
                request = service.files().get_media(fileId=file_id)

                fh = io.FileIO(file_id, 'wb')
                downloader = MediaIoBaseDownload(fh, request)

                done = False
                while done is False:
                    status, done = downloader.next_chunk()
                    print("Download %d%%." % int(status.progress() * 100))
            except Exception as e:
                #return "ARE WE IN EXCEPTION BLOCK???"
                if e.__class__.__name__ == "HttpError":
                    try:
                        request = service.files().export(fileId=file_id, mimeType=mimeType)
                        fh = io.FileIO(file_id, 'wb')
                        downloader = MediaIoBaseDownload(fh, request)

                        done = False
                        while done is False:
                            # return downloader.next_chunk()
                            status, done = downloader.next_chunk()

                            print("Download %d%%." % int(status.progress() * 100))
                    except Exception as e:
                        return e
        except Exception as e:
            return e

        tb = time.time()

        # return "DO WE FAIL BEFORE HERE?"
        try:
            with open(file_id, 'rb') as f:
                i = 0
                for line in f:
                    if i ==2:
                        return line
                    i += 1

        except Exception as e:
            return e

        try:
            new_mdata = xtract_keyword_main.extract_keyword(file_id, pdf=True if extension.lower() == 'pdf' else False)
        except Exception as e:
            return e

    t1 = time.time()
    return {'metadata': new_mdata, 'tot_time': t1-t0, 'trans_time': tb-ta}
