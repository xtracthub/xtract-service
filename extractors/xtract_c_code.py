from extractors.extractor import Extractor
from extractors.utils.base_event import create_event


class CCodeExtractor(Extractor):

    def __init__(self):
        super().__init__(extr_id=None,
                         func_id="", # TODO: fill in once func is registered
                         extr_name="xtract-c-code",
                         store_type="", # TODO: fill in once func is registered
                         store_url="") # TODO: fill in once func is registered

    def create_event(self,
                     family_batch,
                     ep_name,
                     xtract_dir,
                     sys_path_add,
                     module_path,
                     metadata_write_path,
                     recursion_depth=None):

        event = create_event(family_batch=family_batch,
                             ep_name=ep_name,
                             xtract_dir=xtract_dir,
                             sys_path_add=sys_path_add,
                             module_path=module_path,
                             metadata_write_path=metadata_write_path)

        return event
