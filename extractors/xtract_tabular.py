from extractors.extractor import Extractor


class TabularExtractor(Extractor):

    def __init__(self):

        super().__init__(extr_id=None,
                         func_id="aaa9bf93-c13b-4553-b16e-e2a67d0c23f5",
                         extr_name="xtract-tabular",
                         store_type="ecr",
                         store_url="039706667969.dkr.ecr.us-east-1.amazonaws.com/xtract-tabular:latest")
        super().set_extr_func(tabular_extract)




def tabular_extract(event):
    """Extract metadata from tabular data.
    
    Parameters
    ----------
    event : dict
        A dict describing the data and credentials to act on

    Returns
    -------
    dict : The resulting metadata and timers
    """
    import sys
    import time
    import shutil

    from xtract_sdk.downloaders.google_drive import GoogleDriveDownloader

    t0 = time.time()

    sys.path.insert(1, '/')
    import xtract_tabular_main
    # from exceptions import RemoteExceptionWrapper, HttpsDownloadTimeout, ExtractorError, PetrelRetrievalError

    def min_hash(fpath):
        """
        Extracts MinHash digest of a file's bytes

        fpath (str): path to file to extract MinHash of
        """

        from datasketch import MinHash

        NUM_PERMS = 128
        CHUNK_SZ = 64

        mh = MinHash(num_perm=NUM_PERMS)

        with open(fpath, 'rb') as of:
            print("File is open")
            count = 0
            by = of.read(CHUNK_SZ)
            while by != b"":
                by = of.read(CHUNK_SZ)
                count += 1
                mh.update(by)

        return mh

    new_mdata = None

    creds = event["creds"]
    family_batch = event["family_batch"]

    downloader = GoogleDriveDownloader(auth_creds=creds)

    ta = time.time()
    downloader.batch_fetch(family_batch=family_batch)
    tb = time.time()

    file_paths = downloader.success_files

    if len(file_paths) == 0:
        return {'family_batch': family_batch, 'error': True, 'tot_time': time.time()-t0,
                'err_msg': "unable to download files"}

    for family in family_batch.families:
        img_path = family.files[0]['path']
        # return img_path

        new_mdata = xtract_tabular_main.extract_columnar_metadata(img_path)
        new_mdata["min_hash"] = min_hash(img_path)
        family.metadata = new_mdata

    # shutil.rmtree(file_paths)  # TODO: Bring back proper way of doing this.

    t1 = time.time()
    # Return batch
    return {'family_batch': family_batch, 'tot_time': t1-t0, 'trans_time': tb-ta}
