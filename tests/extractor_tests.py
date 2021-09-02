
import os
from xtract_sdk.xtract import XtractAgent
from xtract_sdk.packagers.family import Family
from xtract_sdk.packagers.family_batch import FamilyBatch


def extract_tabular(event):
    from xtract_sdk.agent.xtract import XtractAgent

    # Load endpoint configuration. Init the XtractAgent.
    # TODO: lots of this should go into event.
    xtra = XtractAgent(ep_name="test_tabular_ep",
                       xtract_dir="/Users/tylerskluzacek/.xtract",
                       sys_path_add="/Users/tylerskluzacek/xtract-sdk/tests/xtract-tabular",
                       module_path="xtract_tabular_main",
                       recursion_depth=5000,
                       metadata_write_path="/Users/tylerskluzacek/Desktop/test_metadata")

    # Execute the extractor on our family_batch.
    xtra.execute_extractions(family_batch=event['family_batch'], input_type=str)

    # All metadata are held in XtractAgent's memory. Flush to disk!
    xtra.flush_metadata_to_files(writer='json')

    return xtra.get_completion_stats()


mock_event = dict()

test_fam_1 = Family()
test_fam_2 = Family()

base_path = "/Users/tylerskluzacek/xtract-sdk/tests/xtract-tabular/tests/test_files"
test_fam_1.add_group(files=[{'path': os.path.join(base_path, 'comma_delim'), 'metadata': dict()}], parser=None)
test_fam_1.download_type = "LOCAL"
print(test_fam_1.to_dict())

fam_batch = FamilyBatch()
fam_batch.add_family(test_fam_1)
mock_event['family_batch'] = fam_batch

data = extract_tabular(mock_event)
print(data)
