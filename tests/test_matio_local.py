
from funcx import FuncXClient

from extractors.xtract_matio import matio_extract
from extractors.xtract_matio import MatioExtractor

from extractors.utils.mappings import mapping

import time


from xtract_sdk.packagers.family import Family
from xtract_sdk.packagers.family_batch import FamilyBatch


map = mapping['xtract-matio::midway2']
base_url = ""

base_path = map['data_path']
container_type = map['container_type']
location = map['location']
ep_id = map['ep_id']

fam_1 = Family()
fam_1.add_group(files=[{"path": f"{base_path}INCAR", "metadata": {}, "base_url": base_url}, {"path": f"{base_path}OUTCAR", "metadata": {}, "base_url": base_url}, {"path": f"{base_path}POSCAR", "metadata": {}, "base_url": base_url}], parser='dft')
fam_1.family_id = "data_toprocess"
# fam_1.headers = file_headers

fam_batch = FamilyBatch()
fam_batch.add_family(fam_1)


img_extractor = MatioExtractor()

fn_uuid = img_extractor.register_function(container_type=container_type, location=location,
                                          ep_id=ep_id, group="a31d8dce-5d0a-11ea-afea-0a53601d30b5")


def test(event):
    return sum(event)


def main(fxc, ep_id):

    # while True:
    # container1_uuid = fxc.register_container('xtract-matio.img', 'singularity')
    # container2_uuid = fxc.register_container('/projects/CSC249ADCD01/skluzacek/xtract-images.img', 'singularity')
    # print("Container UUID: {}".format(container1_uuid))
    # fn_uuid = fxc.register_function(matio_extract,
    #                                 ep_id, # TODO: We do not need ep id here
    #                                 container_uuid=container1_uuid,
    #                                 description="New sum function defined without string spec")
    # fn_uuid = fxc.register_function(test, ep_id, description = "HA")
    # print("FN_UUID : ", fn_uuid)
    while True:
        # res = fxc.run([1,2,3,99], endpoint_id=ep_id, function_id=fn_uuid)
        res = fxc.run({'family_batch': fam_batch}, endpoint_id=ep_id, function_id=fn_uuid)
        print(res)
        for i in range(100):
            try:
                x = fxc.get_result(res)
                print(x)
                time.sleep(3)
                break
            except Exception as e:
                print("Exception: {}".format(e))
                time.sleep(2)


if __name__=="__main__":
    fxc = FuncXClient()
    main(fxc, '71509922-996f-4559-b488-4588f06f0925')