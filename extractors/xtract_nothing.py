
from extractors.extractor import Extractor


class NothingExtractor(Extractor):

    def __init__(self):

        super().__init__(extr_id=None,
                         func_id="ae23f090-5a3e-4ef1-9285-31c751abd1a9",
                         extr_name="xtract-matio",
                         store_type="ecr",
                         store_url="039706667969.dkr.ecr.us-east-1.amazonaws.com/xtract-matio:latest")
        super().set_extr_func(nothing_extract)


def nothing_extract(event):

    """
    Function
    :param event (dict) -- contains auth header and list of HTTP links to extractable files:
    :return metadata (dict) -- metadata as gotten from the materials_io library:
    """

    import time

    import os
    import sys

    from xtract_sdk.packagers.family import Family
    from xtract_sdk.packagers.family_batch import FamilyBatch

    t0 = time.time()

    sys.path.insert(1, '/')

    # A list of file paths
    all_families = event['family_batch']

    if type(all_families) == dict:
        family_batch = FamilyBatch()
        for family in all_families["families"]:
            fam = Family()
            fam.from_dict(family)
            family_batch.add_family(fam)
        all_families = family_batch

    for family in all_families.families:
        family_id = family.family_id
        fam_files = family.files
        headers = family.headers

        for file_obj in fam_files:

            # new_path = os.path.join(family_id, local_filename)

            for i in range(10):
                with open(file_obj['path'], 'r') as f:
                    f.close()

    t1 = time.time()

    return {"family_batch": all_families,
            "container_version": os.environ["container_version"],
            "transfer_time": 0,
            "import_time": 0,
            "family_fetch_time": 0,
            "file_unpack_time": 0,
            "full_extract_loop_time": 0,
            "total_time": t1 - t0
            }
