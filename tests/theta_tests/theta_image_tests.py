from funcx import FuncXClient
from extractors.xtract_images import images_extract
import time

from xtract_sdk.packagers.family import Family
from xtract_sdk.packagers.family_batch import FamilyBatch

fam_1 = Family()
fam_batch = FamilyBatch()
fam_batch.add_family(fam_1)

fam_1.add_group(files=[{"path": '/projects/CSC249ADCD01/skluzacek/train2014/COCO_train2014_000000291550.jpg', "is_gdoc": False, "mimeType": "image/jpg", "metadata": {}}], parser='image')


def test(event):
    import os
    return os.environ['container_version']


def main(fxc, ep_id):
    container_uuid = fxc.register_container('/projects/CSC249ADCD01/skluzacek/containers/xtract-images.img', 'singularity')
    print("Container UUID: {}".format(container_uuid))
    fn_uuid = fxc.register_function(images_extract,
                                    #ep_id, # TODO: We do not need ep id here
                                    container_uuid=container_uuid,
                                    description="New sum function defined without string spec")
    print("FN_UUID : ", fn_uuid)
    res = fxc.run({'family_batch': fam_batch,
                   'creds': None,
                   'download_file': False},
                  endpoint_id=ep_id, function_id=fn_uuid)
    print(res)
    for i in range(200):
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
    # main(fxc, "7faf21be-4667-447c-a96d-4dbe14875cf1")
    main(fxc, "020c40c1-5a5d-46b3-8e27-a7dc0073a8ff")