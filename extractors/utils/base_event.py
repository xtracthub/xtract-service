
import copy


def create_event(family_batch, ep_name, xtract_dir, sys_path_add, module_path, metadata_write_path,
                 recursion_limit=1000, input_type=str, writer='json', parser=None):

    # TODO: should probably have an event class.
    # This enables us to not have mutable 'pass by reference' copies
    event = copy.deepcopy(default_event)

    event['family_batch'] = family_batch
    event['ep_name'] = ep_name
    event['xtract_dir'] = xtract_dir
    event['sys_path_add'] = sys_path_add
    event['module_path'] = module_path
    event['recursion_limit'] = recursion_limit
    event['type'] = input_type
    event['writer'] = writer
    event['metadata_write_path'] = metadata_write_path

    return event


default_event = {
    "family_batch": None,
    "ep_name": None,
    "xtract_dir": None,
    "sys_path_add": None,
    "module_path": None,
    "metadata_write_path": None,
    # The following are system defaults.
    "recursion_limit": 1000,
    "type": str,
    "writer": "json"
}
