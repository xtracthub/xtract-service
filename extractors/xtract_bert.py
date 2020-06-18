
from funcx.sdk.client import FuncXClient

fxc = FuncXClient()

location = '039706667969.dkr.ecr.us-east-1.amazonaws.com/xtract-bert'
description = 'Xtract text types container using pretrained BERT NER model'
container_type = 'docker'
name = "xtract/bert"
container_uuid = fxc.register_container(name, location, description, container_type)


def bert_extract(event):
    import io
    import os
    import sys
    import time
    import pickle
    import tempfile

    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request

    t0 = time.time()

    try:
        sys.path.insert(1, '/')
        from xtract_bert_main import load_model, extract_text_metadata

        t_model_load_start = time.time()
        # TODO: multiproc (load both at once in background while downloading files).
        bert_model = load_model('bert')
        w2v_model = load_model('w2v')

        t_model_load_end = time.time()

        model_loading_time = t_model_load_end - t_model_load_start

    except Exception as e:
        return e

    # A list of file paths
    all_families = event['inputs']
    dir_name = tempfile.mkdtemp()
    os.chdir(dir_name)

    def generate_drive_connection(creds):
        service = build('drive', 'v3', credentials=creds)
        return service

    all_results = []
    for family in all_families:
        # TODO: BREAK INTO DOWNLOAD PHASE, THEN PROCESS PHASE.

        d_type = "GDRIVE"

        new_mdata = None
        if d_type is "GDRIVE":

            creds = pickle.loads(event["gdrive_pkl"])[0]
            file_id = event["file_id"]
            is_gdoc = event["is_gdoc"]
            mimeType = "text/csv"
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
                bert_mdata = extract_text_metadata(file_id, 'bert', bert_model)
                w2v_mdata = extract_text_metadata(file_id, 'w2v', w2v_model)
                all_results.append({file_id: {'bert': bert_mdata, 'w2v': w2v_mdata}})
            except Exception as e:
                return e

        t1 = time.time()
        return {'metadata': all_results, 'tot_time': t1-t0, 'trans_time': tb-ta}
