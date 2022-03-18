
import os

from xtract_sdk.packagers.family import Family
from xtract_sdk.packagers.family_batch import FamilyBatch


def create_mock_event(files, parser=None):
    mock_event = dict()

    fam_batch = FamilyBatch()

    test_fam_1 = Family()
    group_file_objs = []

    for file in files:

        base_path = file
        group_file_objs.append({'path': base_path, 'metadata': dict()})
        test_fam_1.download_type = "LOCAL"

    test_fam_1.add_group(files=group_file_objs, parser=parser)
    fam_batch.add_family(test_fam_1)

    mock_event['family_batch'] = fam_batch
    return mock_event


def create_many_family_mock_event(files, parser=None):
    # TODO: this will break for matio
    mock_event = dict()

    fam_batch = FamilyBatch()
    family_id = None

    for file in files:

        if type(file) is dict:
            family_id = str(file['family_id'])
            file = file['filename']

        test_fam_1 = Family()
        group_file_objs = []

        base_path = file
        group_file_objs.append({'path': base_path, 'metadata': dict()})
        test_fam_1.download_type = "LOCAL"

        test_fam_1.add_group(files=group_file_objs, parser=parser)
        if family_id is not None:
            test_fam_1.family_id = family_id
        fam_batch.add_family(test_fam_1)

    mock_event['family_batch'] = fam_batch
    return mock_event