
from extractors.extractor import Extractor

import requests
import time


class MatioExtractor(Extractor):

    def __init__(self):

        super().__init__(extr_id=None,
                         func_id="dcffea71-cf3d-4c01-9c42-06c04576fea5",
                         extr_name="xtract-matio",
                         store_type="ecr",
                         store_url="039706667969.dkr.ecr.us-east-1.amazonaws.com/xtract-matio:latest")
        super().set_extr_func(matio_extract)

    def test_matio(self):
        test_base_url = "https://data.materialsdatafacility.org"
        file_ls = [test_base_url + "/thurston_selfassembled_peptide_spectra_v1.1/DFT/MoleculeConfigs/di_30_-50.xyz/INCAR",
                  test_base_url + "/thurston_selfassembled_peptide_spectra_v1.1/DFT/MoleculeConfigs/di_30_-50.xyz/OUTCAR",
                  test_base_url + "/thurston_selfassembled_peptide_spectra_v1.1/DFT/MoleculeConfigs/di_30_-50.xyz/POSCAR"]

        from fair_research_login import NativeClient
        client = NativeClient(client_id='7414f0b4-7d05-4bb6-bb00-076fa3f17cf5')
        tokens = client.login(
            requested_scopes=['https://auth.globus.org/scopes/56ceac29-e98a-440a-a594-b41e7a084b62/all',
                              'urn:globus:auth:scope:transfer.api.globus.org:all',
                              "https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all",
                              "urn:globus:auth:scope:data.materialsdatafacility.org:all",
                              'email', 'openid'],
            no_local_server=True,
            no_browser=True)

        transfer_token = tokens['transfer.api.globus.org']['access_token']
        mdf_token = tokens["data.materialsdatafacility.org"]['access_token']
        funcx_token = tokens['funcx_service']['access_token']

        headers = {'Authorization': f"Bearer {funcx_token}", 'Transfer': transfer_token, 'FuncX': funcx_token,
                   'Petrel': mdf_token}

        #family = {"family_id": "fam_id_0", "files": file_ls, "headers": headers, "groups": [{"group_id": "gid_0", "parser": "dft", "files": file_ls}]}
        family = {'family_id': '3450d3f3-0fe2-4d3c-bc66-e3b59a45a27d', 'files': {
            '/mdf_open/tsopanidis_wha_amaps_v1.1/Activation Maps/Activation Maps for the WHA Dataset/A_102_28.png': {
                'physical': {'size': 422059, 'extension': 'png', 'path_type': 'globus'}}}, 'groups': [
            {'group_id': '3d62d5e7-bd08-4302-a538-08d6a46ac265', 'parser': 'image', 'files': [
                '/mdf_open/tsopanidis_wha_amaps_v1.1/Activation Maps/Activation Maps for the WHA Dataset/A_102_28.png']}],
         'extractor': 'matio', 'headers': headers, 'base_url': test_base_url}

        families = {"families": [family]}

        task_id = self.remote_extract_solo(event=families, fx_eid="82ceed9f-dce1-4dd1-9c45-6768cf202be8", headers=headers)
        from funcx.serialize import FuncXSerializer
        fx_ser = FuncXSerializer()
        get_url = 'https://dev.funcx.org/api/v1/{}/status'

        while True:
            status_thing = requests.get(get_url.format(task_id), headers=headers).json()
            print(status_thing)
            if 'result' in status_thing:
                result = fx_ser.deserialize(status_thing['result'])
                print(f"Result: {result}")
                break
            elif 'exception' in status_thing:
                print(f"Exception: {fx_ser.deserialize(status_thing['exception'])}")
                break
            time.sleep(1)


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


    t0 = time.time()

    sys.path.insert(1, '/')
    from xtract_matio_main import extract_matio
    # from exceptions import RemoteExceptionWrapper, HttpsDownloadTimeout, ExtractorError, PetrelRetrievalError

    # A list of file paths
    all_families = event['families']

    # This collects all of the files for all of the families.
    file_counter = 0
    filename_to_path_map = dict()
    batch_thruple_ls = []
    # TODO: Move a lot of this family-packing into the SDK.
    for family in all_families:
        family_id = family["family_id"]
        fam_files = family["files"]
        headers = family["headers"]
        base_url = family["base_url"]

        # return fam_files
        for filename in fam_files:

            filename = base_url + filename
            local_filename = filename.split('/')[-1]

            new_path = os.path.join(family_id, local_filename)
            filename_to_path_map[filename] = new_path
            batch_thruple = (filename, new_path, headers)
            # return batch_thruple
            batch_thruple_ls.append(batch_thruple)
            file_counter += 1

    down_start_t = time.time()
    downloader = GlobusHttpsDownloader()
    downloader.batch_fetch(batch_thruple_ls)
    down_end_t = time.time()

    # return downloader.success_files

    # This extracts the metadata for each group in each family.
    all_family_mdata = []
    for family in all_families:
        family_mdata = []
        family_id = family["family_id"]
        base_url = family["base_url"]
        for group in family["groups"]:
            gid = group["group_id"]
            parser = group["parser"]
            actual_group_paths = []
            for filename in group["files"]:
                filename = base_url + filename
                actual_group_paths.append(filename_to_path_map[filename])
            extraction_start_t = time.time()
            new_mdata = extract_matio(paths=actual_group_paths, parser=parser)
            extraction_end_t = time.time()
            new_mdata["group_id"] = gid
            new_mdata["extraction_time"] = extraction_end_t - extraction_start_t
            family_mdata.append(new_mdata)

        family_entry = {"family_id": family_id, "mdata": family_mdata}
        all_family_mdata.append(family_entry)

        # return downloader.success_files
        # TODO: throw earlier if download failed.
        # TODO 2: Xtract-sdk needs to throw error on failed download.
        shutil.rmtree(family_id)  # Cleanup the clutter -- will not need file again since family includes all groups
    t1 = time.time()

    return {"mdata": all_family_mdata,
            "container_version": os.environ["container_version"],
            "transfer_time": down_end_t - down_start_t,
            "total_time": t1 - t0
            }

# #
# miox = MatioExtractor()
# func_id = miox.register_function()
# miox.test_matio()

