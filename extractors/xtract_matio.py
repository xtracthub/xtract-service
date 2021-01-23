
from extractors.extractor import Extractor


class MatioExtractor(Extractor):

    def __init__(self):

        super().__init__(extr_id=None,
                         func_id="71639b5b-ef94-41bd-87b9-18b9f7b2fb72",
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

    import time
    import json

    import pickle as pkl

    import_start = time.time()
    import os
    import sys
    import shutil

    from xtract_sdk.downloaders.globus_https import GlobusHttpsDownloader
    from xtract_sdk.packagers.family import Family
    from xtract_sdk.packagers.family_batch import FamilyBatch

    t0 = time.time()

    sys.path.insert(1, '/')

    # return "Hello? "


    from xtract_matio_main import extract_matio

    import_end = time.time()

    # A list of file paths
    all_families = event['family_batch']

    is_local = True
    should_delete = False

    load_family_start_t = time.time()
    if type(all_families) == dict:
        family_batch = FamilyBatch()
        for family in all_families["families"]:
            fam = Family()
            fam.from_dict(family)
            family_batch.add_family(fam)
        all_families = family_batch

    load_family_end_t = time.time()


    get_files_start_t = time.time()
    # This collects all of the files for all of the families.
    file_counter = 0
    filename_to_path_map = dict()
    batch_thruple_ls = []

    total_families = 0

    base_url = None
    for family in all_families.families:
        family_id = family.family_id
        fam_files = family.files
        headers = family.headers

        for file_obj in fam_files:
            base_url = file_obj["base_url"]
            filename = base_url + file_obj["path"]

            # if not is_local:
            local_filename = filename.split('/')[-1]
            # else:
            #     local_filename = filename

            new_path = os.path.join(family_id, local_filename)
            filename_to_path_map[filename] = new_path
            batch_thruple = (filename, new_path, headers)

            batch_thruple_ls.append(batch_thruple)
            file_counter += 1

    get_files_end_t = time.time()

    if not is_local:
        down_start_t = time.time()
        downloader = GlobusHttpsDownloader()
        downloader.batch_fetch(batch_thruple_ls)
        down_end_t = time.time()

        if len(downloader.fail_files) > 0:
            raise ValueError("TODO BETTER ERROR HANDLING -- was unable to fetch files from Globus")
    else:
        down_start_t = down_end_t = 0

    extract_iter_start_t = time.time()
    # This extracts the metadata for each group in each family.ZZ
    for family in all_families.families:

        for gid in family.groups:
            parser = family.groups[gid].parser
            actual_group_paths = []

            # return family.groups[gid].files
            for file_obj in family.groups[gid].files:

                filename = file_obj["base_url"] + file_obj["path"]

                if is_local:
                    actual_group_paths.append(filename)
                else:
                    actual_group_paths.append(filename_to_path_map[filename])

            extraction_start_t = time.time()
            new_mdata = extract_matio(paths=actual_group_paths, parser=parser)
            extraction_end_t = time.time()

            new_mdata["extraction_time"] = extraction_end_t - extraction_start_t
            family.groups[gid].metadata = new_mdata

        with open(f"/projects/CSC249ADCD01/skluzacek/metadata/{family.family_id}", 'wb') as f:
            #with open(f"/home/tskluzac/metadata/{family.family_id}", 'wb') as f:
            # with open(f"/project2/chard/skluzacek/metadata/{family.family_id}") as f:
            pkl.dump(family, f)


        # if should_delete:
        #     # Cleanup the clutter -- will not need file again since family includes all groups
        #     shutil.rmtree(os.path.dirname(all_families.file_ls[0]['path']))

    extract_iter_end_time = time.time()

    t1 = time.time()

    return {'finished': len(all_families.families)}

    # return {"family_batch": all_families,
    #         "container_version": os.environ["container_version"],
    #         "transfer_time": down_end_t - down_start_t,
    #         "import_time": import_end - import_start,
    #         "family_fetch_time": load_family_end_t - load_family_start_t,
    #         "file_unpack_time": get_files_end_t - get_files_start_t,
    #         "full_extract_loop_time": extract_iter_end_time - extract_iter_start_t,
    #         "total_time": t1 - t0
    #         }


