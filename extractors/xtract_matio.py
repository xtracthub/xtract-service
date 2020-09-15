
from extractors.extractor import Extractor


class MatioExtractor(Extractor):

    def __init__(self):

        super().__init__(extr_id=None,
                         func_id="9944ab30-9ae1-414c-9a3f-0a30f83cb99a",
                         extr_name="xtract-matio",
                         store_type="ecr",
                         store_url="039706667969.dkr.ecr.us-east-1.amazonaws.com/xtract-matio:latest")
        super().set_extr_func(matio_extract)


def matio_extract(event):

    """
    Function
    :param event (dict) -- contains auth header and list of HTTP links to extractable files:
    :return metadata (dict) -- metadata as gotten from the materials_io library:
    """

    import os
    import sys
    import time
    import shutil

    from xtract_sdk.downloaders.globus_https import GlobusHttpsDownloader
    from xtract_sdk.packagers.family import Family
    from xtract_sdk.packagers.family_batch import FamilyBatch

    t0 = time.time()

    sys.path.insert(1, '/')

    from xtract_matio_main import extract_matio
    # from exceptions import RemoteExceptionWrapper, HttpsDownloadTimeout, ExtractorError, PetrelRetrievalError

    # A list of file paths
    all_families = event['family_batch']

    # return all_families

    # TODO: This is ugly. Clean up!
    # return "hi"
    family_batch = FamilyBatch()
    for family in all_families["families"]:
        # return family
        fam = Family()
        fam.from_dict(family)
        family_batch.add_family(fam)
    all_families = family_batch

    # return {'family_batch': family_batch}

    # This collects all of the files for all of the families.
    file_counter = 0
    filename_to_path_map = dict()
    batch_thruple_ls = []

    base_url = None  # TODO: add function to apply base_url
    for family in all_families.families:
        family_id = family.family_id
        fam_files = family.files
        headers = family.headers

        for file_obj in fam_files:
            base_url = file_obj["base_url"]
            filename = base_url + file_obj["path"]
            local_filename = filename.split('/')[-1]

            new_path = os.path.join(family_id, local_filename)
            filename_to_path_map[filename] = new_path
            batch_thruple = (filename, new_path, headers)

            batch_thruple_ls.append(batch_thruple)
            file_counter += 1

    # return batch_thruple_ls
    down_start_t = time.time()
    downloader = GlobusHttpsDownloader()
    downloader.batch_fetch(batch_thruple_ls)
    down_end_t = time.time()

    if len(downloader.fail_files) > 0:
        raise ValueError("TODO BETTER ERROR HANDLING -- was unable to fetch files from Globus")

    # return {'success': downloader.success_files, 'failure': downloader.fail_files}

    # This extracts the metadata for each group in each family.
    for family in all_families.families:
        for gid in family.groups:
            parser = family.groups[gid].parser
            actual_group_paths = []
            for file_obj in family.groups[gid].files:
                filename = file_obj["base_url"] + file_obj["path"]
                actual_group_paths.append(filename_to_path_map[filename])

        # for path in actual_group_paths:
        #     if "INCAR" in path:
        #         lines = ""
        #         with open(path, 'r') as f:
        #             for line in f:
        #                 lines += line
        #         return lines

            extraction_start_t = time.time()

            new_mdata = extract_matio(paths=actual_group_paths, parser=parser)
            extraction_end_t = time.time()

            new_mdata["extraction_time"] = extraction_end_t - extraction_start_t
            family.groups[gid].metadata = new_mdata

        # TODO: Gotta delete data.
        # shutil.rmtree(family_id)  # Cleanup the clutter -- will not need file again since family includes all groups
    t1 = time.time()

    return {"family_batch": all_families,
            "container_version": os.environ["container_version"],
            "transfer_time": down_end_t - down_start_t,
            "total_time": t1 - t0
            }


