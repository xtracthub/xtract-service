from extractors.extractor import Extractor
from extractors.utils.base_event import create_event


class KeywordExtractor(Extractor):

    def __init__(self):
        super().__init__(extr_id=None,
                         func_id="833f6271-e03c-4ac5-bc32-64eba7f13460",
                         extr_name="xtract-tabular", #TODO: change to xtract-keyword
                         store_type="ecr",
                         store_url="039706667969.dkr.ecr.us-east-1.amazonaws.com/xtract-tabular:latest") #TODO: change to xtract-keyword

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
