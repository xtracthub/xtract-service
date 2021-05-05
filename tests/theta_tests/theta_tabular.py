
from funcx import FuncXClient

from xtract_sdk.packagers.family import Family
from xtract_sdk.packagers.family_batch import FamilyBatch

# THE ACTUAL FUNCTION
from extractors.xtract_tabular import tabular_extract

import time

fam_1 = Family()
fam_1.add_group(files=[{'path': '/projects/CSC249ADCD01/skluzacek/containers/comma_delim'}], parser=None)
fam_1.download_type = "LOCAL"
fam_1.base_url = ""

fam_batch = FamilyBatch()
fam_batch.add_family(fam_1)


def test(event):
    import os
    return os.environ['container_version']


def main(fxc, ep_id):
    container_uuid = fxc.register_container('/projects/CSC249ADCD01/skluzacek/containers/xtract-tabular.img', 'singularity')
    print("Container UUID: {}".format(container_uuid))
    fn_uuid = fxc.register_function(tabular_extract,
                                    #ep_id, # TODO: We do not need ep id here
                                    container_uuid=container_uuid,
                                    description="Tabular test function.")
    print("FN_UUID : ", fn_uuid)
    res = fxc.run({'family_batch': fam_batch,
                   'creds': None,
                   'download_file': False},
                  endpoint_id=ep_id, function_id=fn_uuid)
    print(res)
    for i in range(100):
        try:
            x = fxc.get_result(res)
            print(x)
            # print(x['family_batch'].families[0].metadata)
            break
        except Exception as e:
            print("Exception: {}".format(e))
            time.sleep(2)


if __name__=="__main__":
    fxc = FuncXClient()
    main(fxc, "adf8837a-8944-49fa-a877-7755915c61d6")