
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


def create_many_family_mock_event(families, parser=None):
    mock_event = dict()

    fam_batch = FamilyBatch()
    family_id = None

    for family_obj in families:

        test_fam_1 = Family()
        group_file_objs = []

        # If we only have 1 file in the family group.
        # print(family_id)
        family_id = str(family_obj['family_id'])
        file_names = family_obj['filenames']

        # Otherwise, if we have multiple files to pack ingo group.
        # elif len(file_names) > 1:
        for fobj in file_names:
            base_path = fobj
            group_file_objs.append({'path': base_path, 'metadata': dict()})

        # Hard-code set to LOCAL.  # TODO: Remove if we decide not to support HTTPS/GDrive download...
        test_fam_1.download_type = "LOCAL"

        test_fam_1.add_group(files=group_file_objs, parser=parser)
        if family_id is not None:
            test_fam_1.family_id = family_id
        fam_batch.add_family(test_fam_1)

    mock_event['family_batch'] = fam_batch
    return mock_event