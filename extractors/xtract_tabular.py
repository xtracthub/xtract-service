from extractors.extractor import Extractor


class TabularExtractor(Extractor):

    def __init__(self):

        super().__init__(extr_id=None,
                         func_id="833f6271-e03c-4ac5-bc32-64eba7f13460",
                         extr_name="xtract-tabular",
                         store_type="ecr",
                         store_url="039706667969.dkr.ecr.us-east-1.amazonaws.com/xtract-tabular:latest")
        super().set_extr_func(tabular_extract)


def tabular_extract(event):
    """
    EXTRACTOR CODE.

    The tabular extractor input

    Group-inputs: single file.
    Group-outputs: dictionary of tabular metadata, mainly centered around the general structure and the column values.
    
    Parameters
    ----------
    event : dict -- contains credential and FamilyBatch objects.

    Returns
    -------
    dict : contains macro information, including family_ids, local path(s) where the metadata are stored.
    """

    import sys
    import time

    t0 = time.time()

    from xtract_sdk.xtract import XtractAgent

    sys.path.insert(1, '/')
    sys.setrecursionlimit(5000)  # I'm a bad person.
    import xtract_tabular_main

    family_batch = event["family_batch"]
    ep_name = event["ep_name"]
    dot_xtract_path = event[".xtract_path"]

    # Create XtractAgent() and load with families.
    xtra = XtractAgent(ep_name=ep_name, xtract_dir=dot_xtract_path)

    for family in family_batch.families:
        xtra.load_family(family.to_dict())

    # Fetch all of the files via the Xtract agent.
    try:
        xtra.fetch_all_files()
    except Exception as e:
        return f"Caught: {e}"

    for family in xtra.ready_families:
        tab_file = family['files'][0]['path']
        new_mdata = xtract_tabular_main.extract_columnar_metadata(tab_file)

        # TODO: Write preamble out to file.
        # TODO: write this out to file.

        return new_mdata

    # # TODO: Delete if we need to do that.
    # t1 = time.time()
    # # Return batch
    t1 = time.time()
    return {'family_batch': family_batch, 'tot_time': t1-t0}
