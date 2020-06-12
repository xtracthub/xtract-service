
from extractors.extractor import Extractor


class ImageExtractor(Extractor):

    def __init__(self):

        super().__init__(extr_id=None,
                         func_id="???",
                         extr_name="xtract-image",
                         store_type="ecr",
                         store_url="039706667969.dkr.ecr.us-east-1.amazonaws.com/xtract-image:latest")
        super().set_extr_func(images_extract)


def images_extract(event):

    import os
    import sys
    import time

    from shutil import copyfile

    from xtract_sdk.downloaders.globus_https import GlobusHttpsDownloader
    from xtract_sdk.downloaders.google_drive import GoogleDriveDownloader

    t0 = time.time()

    sys.path.insert(1, '/app')
    import xtract_images_main

    cur_ls = os.listdir('.')
    if 'pca_model.sav' not in cur_ls or 'clf_model.sav' not in cur_ls:
        # TODO: Make these lines unnecessary.
        copyfile('/app/pca_model.sav', f'pca_model.sav')
        copyfile('/app/clf_model.sav', f'clf_model.sav')

    creds = event["gdrive"]
    file_id = event["file_id"]

    ta = time.time()
    downloader = GoogleDriveDownloader(auth_creds=creds)
    downloader.fetch(fid=file_id, download_type="media")
    tb = time.time()

    file_paths = downloader.success_files

    new_mdata = None
    for path in file_paths:
        new_mdata = xtract_images_main.extract_image('predict', path)

    t1 = time.time()
    return {'image': new_mdata, 'tot_time': t1-t0, 'trans_time': tb-ta}
