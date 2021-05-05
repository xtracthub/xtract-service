from funcx import FuncXClient
from extractors.xtract_tabular import tabular_extract
import time

from xtract_sdk.packagers.family import Family
from xtract_sdk.packagers.family_batch import FamilyBatch

file_id = "1XCS2Xqu35TiQgCpI8J8uu4Mss9FNnp1-AuHo-pMujb4"
file_id2 = "0B5nDSpS9a_3kUFdiTXRFdS12QUk"

family_1 = Family()
family_2 = Family()

family_1.add_group(files=[
    {'path': file_id, 'is_gdoc': True, 'mimeType': "text/csv"}],
    parser='xtract-tabular')
family_1.base_url = ""

family_2.add_group(files=[
    {'path': file_id2, 'is_gdoc': False}],
    parser='xtract-tabular')
family_2.download_type = "GDRIVE"

fam_batch = FamilyBatch()
fam_batch.add_family(family_1)
fam_batch.add_family(family_2)

def test(event):
    import os
    return os.environ['container_version']


def main(fxc, ep_id):
    container_uuid = fxc.register_container('xtract-tabular.img', 'singularity')
    print("Container UUID: {}".format(container_uuid))
    fn_uuid = fxc.register_function(test,
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
            print(x['family_batch'].families[0].metadata)
            break
        except Exception as e:
            print("Exception: {}".format(e))
            time.sleep(2)


if __name__=="__main__":
    fxc = FuncXClient()
    main(fxc, "7faf21be-4667-447c-a96d-4dbe14875cf1")