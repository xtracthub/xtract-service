
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
    import sys



    # Enable using files stored at '/' in the container.
    sys.path.insert(1, '/')


    from xtract_matio_main import extract_matio





    return {'finished': len(all_families.families), 'extract_time': t1-import_end}
