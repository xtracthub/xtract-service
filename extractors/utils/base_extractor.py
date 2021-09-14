

def base_extractor(event):
    import json
    from xtract_sdk.agent.xtract import XtractAgent

    # Load endpoint configuration. Init the XtractAgent.
    xtra = XtractAgent(ep_name=event['ep_name'],
                       xtract_dir=event['xtract_dir'],
                       sys_path_add=event['sys_path_add'],
                       module_path=event['module_path'],
                       recursion_depth=event['recursion_limit'],
                       metadata_write_path=event['metadata_write_path'])

    import importlib
    family_batch = event['family_batch']
    # TODO: HERE -- name 'event' is not defined
    families = family_batch.families

    my_module = importlib.import_module(xtra.module_path)

    for family in families:
        for gid in family.groups:
            group = family.groups[gid]
            # This means that family contains one file, and extractor inputs one file.
            if event['type'] is str:
                # return "RIGHT HERE"
                # Automatically try to hit the 'execute_extractor' function
                # TODO: this next line creating the error!
                mdata = my_module.execute_extractor(group.files[0]['path'])
                # return mdata
    # return event
    # Execute the extractor on our family_batch.
    xtra.execute_extractions(family_batch=event['family_batch'], input_type=event['type'])

    # return "helo"
    # return event
    # All metadata are held in XtractAgent's memory. Flush to disk!
    paths = xtra.flush_metadata_to_files(writer=event['writer'])
    stats = xtra.get_completion_stats()
    stats['mdata_paths'] = paths

    return stats
