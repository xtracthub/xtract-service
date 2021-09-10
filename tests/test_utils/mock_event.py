
import os

from xtract_sdk.packagers.family import Family
from xtract_sdk.packagers.family_batch import FamilyBatch


def create_mock_event(files):
    mock_event = dict()

    fam_batch = FamilyBatch()

    for file in files:
        test_fam_1 = Family()
        base_path = file
        test_fam_1.add_group(files=[{'path': base_path, 'metadata': dict()}], parser=None)
        test_fam_1.download_type = "LOCAL"
        fam_batch.add_family(test_fam_1)

    mock_event['family_batch'] = fam_batch
    return mock_event
