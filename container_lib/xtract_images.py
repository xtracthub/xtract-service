
from funcx.sdk.client import FuncXClient

fxc = FuncXClient()

location = '039706667969.dkr.ecr.us-east-1.amazonaws.com/xtract-images'
description = 'Xtract image types container'
container_type = 'docker'
name = "xtract/images"
container_uuid = fxc.register_container(name, location, description, container_type)


def images_extract(event):

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
    from shutil import copyfile

    try:
        sys.path.insert(1, '/app')
        import xtract_images_main
        # from exceptions import RemoteExceptionWrapper, HttpsDownloadTimeout, ExtractorError, PetrelRetrievalError

    except Exception as e:
        return e

    # A list of file paths
    dir_name = tempfile.mkdtemp()
    os.chdir(dir_name)

    copyfile('/app/pca_model.sav', f'{dir_name}/pca_model.sav')
    copyfile('/app/clf_model.sav', f'{dir_name}/clf_model.sav')

    def generate_drive_connection(creds):
        service = build('drive', 'v3', credentials=creds)
        return service

    d_type = "GDRIVE"

    new_mdata = None
    if d_type is "GDRIVE":
        # return "Just about to the pickle. "
        try:
            creds = event["gdrive"]
            file_id = event["file_id"]
            #return type(creds)
        except Exception as e:
            return f"Error A: {e}"


        # TODO: These DO NOT need to cascade...
        try:
            service = generate_drive_connection(creds)
        except Exception as e:
            return e
        ta = time.time()
        try:
            request = service.files().get_media(fileId=file_id)
        except Exception as e:
            return e

        try:
            fh = io.FileIO(file_id, 'wb')
            downloader = MediaIoBaseDownload(fh, request)
        except Exception as e:
            return e

        try:
            done = False
            while done is False:
                status, done = downloader.next_chunk()
                print("Download %d%%." % int(status.progress() * 100))
        except Exception as e:
            return e

        tb = time.time()
        new_mdata = xtract_images_main.extract_image('predict', file_id)

    t1 = time.time()
    return {'image': new_mdata, 'tot_time': t1-t0, 'trans_time': tb-ta}
