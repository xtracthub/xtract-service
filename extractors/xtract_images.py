
from extractors.extractor import Extractor


class ImageExtractor(Extractor):

    def __init__(self):

        super().__init__(extr_id=None,
                         func_id="2c697742-c8e1-446a-9c1e-a0338d018687",
                         extr_name="xtract-image",
                         store_type="ecr",
                         store_url="039706667969.dkr.ecr.us-east-1.amazonaws.com/xtract-image:latest")
        super().set_extr_func(images_extract)


def images_extract(event):

    import os
    import sys
    import time

    from shutil import copyfile

    from xtract_sdk.downloaders.google_drive import GoogleDriveDownloader

    t0 = time.time()

    sys.path.insert(1, '/app')
    import xtract_images_main

    cur_ls = os.listdir('.')
    if 'pca_model.sav' not in cur_ls or 'clf_model.sav' not in cur_ls:
        # TODO: Make these lines unnecessary.
        copyfile('/app/pca_model.sav', f'pca_model.sav')
        copyfile('/app/clf_model.sav', f'clf_model.sav')

    family_batch = event["family_batch"]
    creds = event["creds"]

    downloader = GoogleDriveDownloader(auth_creds=creds)

    # TODO: Put time info into the downloader/extractor objects.
    ta = time.time()
    # TODO needs to implement as batch.
    downloader.batch_fetch(family_batch=family_batch)
    tb = time.time()

    file_paths = downloader.success_files
    if len(file_paths) == 0:
        return {'family_batch': family_batch, 'error': True, 'tot_time': time.time()-t0,
                'err_msg': "unable to download files"}

    for family in family_batch.families:

        img_path = family.files[0]['path']
        new_mdata = xtract_images_main.extract_image('predict', img_path)
        family.metadata = new_mdata

    t1 = time.time()
    # Return batch
    return {'family_batch': family_batch, 'tot_time': t1-t0, 'trans_time': tb-ta}
