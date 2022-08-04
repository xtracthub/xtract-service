

def base_extractor(event):
    from xtract_sdk.agent.xtract import XtractAgent

    # Load endpoint configuration. Init the XtractAgent.
    xtra = XtractAgent(ep_name=event['ep_name'],
                       xtract_dir=event['xtract_dir'],
                       sys_path_add=event['sys_path_add'],
                       module_path=event['module_path'],
                       recursion_depth=event['recursion_limit'],
                       metadata_write_path=event['metadata_write_path'])

    # Execute the extractor on our family_batch.
    xtra.execute_extractions(family_batch=event['family_batch'], input_type=event['type'])

    # All metadata are held in XtractAgent's memory. Flush to disk!
    paths = xtra.flush_metadata_to_files(writer=event['writer'])
    stats = xtra.get_completion_stats()
    stats['mdata_paths'] = paths
    # stats['completed_timestamp'] =

    return stats
