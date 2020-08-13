
from extractors.extractor import Extractor


class ImageExtractor(Extractor):

    def __init__(self):

        super().__init__(extr_id=None,
                         func_id="aceb61b1-ba41-4557-b53b-1d4fd331c1d1",
                         extr_name="xtract-image",
                         store_type="ecr",
                         store_url="039706667969.dkr.ecr.us-east-1.amazonaws.com/xtract-image:latest")
        super().set_extr_func(images_extract)


def images_extract(event):

    # return "hey, bud"

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


    # return "we coo? "

    family_batch = event["family_batch"]
    creds = event["creds"]

    downloader = GoogleDriveDownloader(auth_creds=creds)

    # TODO: Put time info into the downloader/extractor objects.
    ta = time.time()
    # TODO needs to implement as batch.
    # return family_batch.file_ls
    try:
        downloader.batch_fetch(family_batch=family_batch)
    except Exception as e:
        return e
    # return "well we're here..."
    tb = time.time()

    file_paths = downloader.success_files
    if len(file_paths) == 0:
        return {'family_batch': family_batch, 'error': True, 'tot_time': time.time()-t0,
                'err_msg': "unable to download files"}

    # return file_paths
    for family in family_batch.families:

        img_path = family.files[0]['path']
        new_mdata = xtract_images_main.extract_image('predict', img_path)
        family.metadata = new_mdata

    t1 = time.time()

    [os.remove(file_path) for file_path in downloader.success_files]

    # Return batch
    return {'family_batch': family_batch, 'tot_time': t1-t0, 'trans_time': tb-ta}
