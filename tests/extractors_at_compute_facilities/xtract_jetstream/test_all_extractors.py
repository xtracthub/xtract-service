
from tests.test_utils.native_app_login import globus_native_auth_login
# from tests.extractors_at_compute_facilities.facility_fx_eps import facility_fx_eps
from tests.extractors_at_compute_facilities import extractors_to_test
from extractors.extractor import base_extractor

import os
import time
import funcx
from queue import Queue
from tests.test_utils.mock_event import create_mock_event

from funcx import FuncXClient

# fxc = FuncXClient()


# def get_execution_information(env_name):
#
#     fx_eid = facility_fx_eps[env_name]['fx_ep']
#
#     extractors = extractors_to_test.all_extractors
#     extractor_home = facility_fx_eps[env_name]['container_path']
#
#     print(f"FuncX EP ID:\t{fx_eid}")
#     print(f"Extractor home:\t{extractor_home}")
#
#     print(f"Preparing to process {len(extractors)} at endpoint...")
#
#     return {'fx_eid': fx_eid,
#             # 'tokens': tokens,
#             'extractors': extractors,
#             'extractor_home': extractor_home}


def register_functions(fx_eid, extractor_home,  fxc=None):
    # Grab the names of all extractors.
    extractors = extractors_to_test.ALL_EXTRACTORS.keys()

    print(f"EXTRACTORS: {extractors}")
    print(f"EXTRACTOR HOME: {extractor_home}")

    funcx_uuids = dict()
    for item in extractors:
        print(f"DEBUG: registering {item}")
        extractor = f"{item}.img"
        dest_ext_path = os.path.join(extractor_home, extractor)
        container_uuid = fxc.register_container(location=dest_ext_path, container_type='singularity')
        fn_uuid = fxc.register_function(base_extractor,
                                        container_uuid=container_uuid,
                                        description="Tabular test function.")
        print(f"Registered container for extractor {item} to path {dest_ext_path}")

        funcx_uuids[item] = fn_uuid
        time.sleep(0.2)
    return funcx_uuids


# def run(environment, fxc):
#     execution_information = get_execution_information(environment)
#     func_uuids = register_functions(execution_information, fxc=fxc)
#
#     results_check_queue = Queue()
#     ext_to_tids_map = dict()
#     ext_to_pass_fail_counts_queue = dict()
#     tid_to_ext_inverse = dict()
#
#     all_extractors = extractors_to_test.all_extractors
#     for ext_type in all_extractors:
#         ext_to_tids_map[ext_type] = []
#         ext_to_pass_fail_counts_queue[ext_type] = {'pass': 0,
#                                                    'fail': 0}
#
#         for test_file in all_extractors[ext_type]['test_files']:
#
#             # If the MatIO case, let's just take all the files!
#             base_file_path = "/home/tskluzac/.xtract/.test_files"
#
#             if 'parser' in all_extractors[ext_type]:
#                 all_files = []
#                 for file in all_extractors[ext_type]['test_files']:
#                     all_files.append(os.path.join(base_file_path, file))
#                 mock_event = create_mock_event(all_files, parser=all_extractors[ext_type]['parser'])
#             else:
#                 mock_event = create_mock_event([os.path.join(base_file_path, test_file)])
#
#             event = all_extractors[ext_type]['extractor'].create_event(
#                 ep_name="foobar",
#                 family_batch=mock_event['family_batch'],
#                 xtract_dir="/home/tskluzac/.xtract",
#                 module_path=f"xtract_{ext_type.replace('xtract-', '')}_main",
#                 sys_path_add="/",
#                 metadata_write_path="/home/tskluzac/mdata"
#             )
#
#             print(f"Event: {event}")
#
#             tid = fxc.run(event,
#                           endpoint_id=execution_information['fx_eid'],
#                           function_id=func_uuids[ext_type])
#
#             ext_to_tids_map[ext_type].append(tid)
#             tid_to_ext_inverse[tid] = ext_type
#             results_check_queue.put(tid)
#
#             # TODO: BUG IN FUNCX -- THIS DOES NOT CONNECT IN CONTAINER (LOCALLY) AT < 0.5
#             time.sleep(1)
#
#             # If MatIO, just quitting after the first iteration! (since we lumped all the files into 1).
#             if 'parser' in all_extractors[ext_type]:
#                 break
#
#     while not results_check_queue.empty():
#         tid_to_check = results_check_queue.get()
#         ext_type = tid_to_ext_inverse[tid_to_check]
#
#         print(f"Checking task of extractor type: {ext_type}")
#
#         try:
#             res = fxc.get_result(tid_to_check)
#         except funcx.utils.errors.TaskPending as e:
#             results_check_queue.put(tid_to_check)
#             print(e)
#             time.sleep(2)
#             continue
#
#         print(res)
#         if res['n_groups_nonempty'] == 1:
#             ext_to_pass_fail_counts_queue[ext_type]['pass'] += 1
#         elif res['n_groups_empty'] > 0:
#             print("THIS ONE FAILED!")
#             ext_to_pass_fail_counts_queue[ext_type]['fail'] += 1
#         time.sleep(1)
#
#     print(f"\nFINAL REPORT FOR ENVIRONMENT: {environment}")
#     for ext_key in ext_to_pass_fail_counts_queue:
#         success = ext_to_pass_fail_counts_queue[ext_key]['pass']
#         failed = ext_to_pass_fail_counts_queue[ext_key]['fail']
#
#         tab_break = "\t"
#         if len(ext_key) < 11:
#             tab_break = tab_break + "\t"
#
#         print(f"{ext_key}:{tab_break}success({success})\tfails({failed})")


# if __name__ == "__main__":
#     fxc = FuncXClient()
#     run(environment='jetstream', fxc=fxc)
