
import funcx
import time
import json
import os
import requests
from extractors.xtract_matio import serialize_fx_inputs, matio_test, hello_world
from fair_research_login import NativeClient
from funcx.serialize import FuncXSerializer
from queue import Queue
from mdf_matio.validator import MDFValidator
from materials_io.utils.interface import ParseResult
from typing import Iterable, Set, List
from functools import reduce, partial
from mdf_matio.grouping import groupby_file, groupby_directory


from mdf_toolbox import dict_merge

_merge_func = partial(dict_merge, append_lists=True)


def _merge_directories(parse_results: Iterable[ParseResult], dirs_to_group: List[str])\
        -> Iterable[ParseResult]:
    """Merge records from user-specified directories
    Args:
        parse_results (ParseResult): Generator of ParseResults
    Yields:
        (ParseResult): ParserResults merged for each record
    """

    # Add a path separator to the end of each directory
    #  Used to simplify checking whether each file is a subdirectory of the matched groups
    dirs_to_group = [d + os.path.sep for d in dirs_to_group]

    def is_in_directory(f):
        """Check whether a file is in one fo the directories to group"""
        f = os.path.dirname(f) + os.path.sep
        return any(f.startswith(d) for d in dirs_to_group)

    # Gather records that are in directories to group or any of their subdirectories
    flagged_records = []
    for record in parse_results:
        if any(is_in_directory(f) for f in record.group):
            flagged_records.append(record)
        else:
            yield record

    # Once all of the parse results are through, group by directory
    for group in groupby_directory(flagged_records):
        yield _merge_records(group)


def _merge_files(parse_results: Iterable[ParseResult]) -> Iterable[ParseResult]:
    """Merge metadata of records associated with the same file(s)
    Args:
        parse_results (ParseResult): Generator of ParseResults
    Yields:
        (ParseResult): ParserResults merged for each file.
    """
    return map(_merge_records, groupby_file(parse_results))


def _merge_records(group: List[ParseResult]):
    """Merge a group of records
    Args:
        group ([ParseResult]): List of parse results to group
    """

    # Group the file list and parsers
    group_files = list(set(sum([tuple(x.group) for x in group], ())))
    group_parsers = '-'.join(sorted(set(sum([[x.parser] for x in group], []))))

    # Merge the metadata
    is_list = [isinstance(x.metadata, list) for x in group]
    if sum(is_list) > 1:
        raise NotImplementedError('We have not defined how to merge >1 list-type data')
    elif sum(is_list) == 1:
        list_data = group[is_list.index(True)].metadata
        if len(is_list) > 1:
            other_metadata = reduce(_merge_func,
                                    [x.metadata for x, t in zip(group, is_list) if not t])
            group_metadata = [_merge_func(x, other_metadata) for x in list_data]
        else:
            group_metadata = list_data
    else:
        group_metadata = reduce(_merge_func, [x.metadata for x in group])
    return ParseResult(group_files, group_parsers, group_metadata)


fxc = funcx.FuncXClient()
fx_ser = FuncXSerializer()

vald_obj = MDFValidator(schema_branch="master")

post_url = 'https://dev.funcx.org/api/v1/submit'
get_url = 'https://dev.funcx.org/api/v1/{}/status'
# globus_ep = "1adf6602-3e50-11ea-b965-0e16720bb42f"
globus_ep = "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec"

fx_ep = "82ceed9f-dce1-4dd1-9c45-6768cf202be8"
n_tasks = 5000

burst_size = 5

batch_size = 5

container_id = fxc.register_container(location='039706667969.dkr.ecr.us-east-1.amazonaws.com/xtract-matio:latest',
                                      container_type='docker',
                                      name='kube-matio5',
                                      description='I don\'t think so!')
fn_id = fxc.register_function(matio_test,
                              container_uuid=container_id,
                              description="A sum function")


print(f"Function UUID: {fn_id}")

# Get the Headers....
client = NativeClient(client_id='7414f0b4-7d05-4bb6-bb00-076fa3f17cf5')
tokens = client.login(
    requested_scopes=['https://auth.globus.org/scopes/56ceac29-e98a-440a-a594-b41e7a084b62/all',
                      'urn:globus:auth:scope:transfer.api.globus.org:all',
                     "https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all",
                    "urn:globus:auth:scope:data.materialsdatafacility.org:all",
                     'email', 'openid'],
    no_local_server=True,
    no_browser=True)

# print(tokens)
# exit()

auth_token = tokens["petrel_https_server"]['access_token']
transfer_token = tokens['transfer.api.globus.org']['access_token']
mdf_token = tokens["data.materialsdatafacility.org"]['access_token']
funcx_token = tokens['funcx_service']['access_token']

headers = {'Authorization': f"Bearer {funcx_token}", 'Transfer': transfer_token, 'FuncX': funcx_token, 'Petrel': mdf_token}
print(f"Headers: {headers}")

# NCSA file
old_mdata = {"files": ["/mdf_open/tsopanidis_wha_amaps_v1.1/WHA Dataset/Cropped Images/A_102_1.png"]}


# ASE/crystal
# old_mdata = {"files": ["/MDF/mdf_connect/prod/data/h2o_13_v1-1/split_xyz_files/watergrid_60_HOH_180__0.7_rOH_1.8_vario_PBE0_AV5Z_delta_PS_data/watergrid_PBE0_record-1237.xyz"]}

# Images
# old_mdata = {"files": ["/MDF/mdf_connect/prod/data/klh_1_v1/exposure1_jpg.jpg/01nov26b.001.002.001.001.jpg"]*batch_size}

# DFT
# old_mdata ={"files": ["/MDF/mdf_connect/prod/data/_test_einstein_9vpflvd_v1.1/INCAR", "/MDF/mdf_connect/prod/data/_test_einstein_9vpflvd_v1.1/OUTCAR", "/MDF/mdf_connect/prod/data/_test_einstein_9vpflvd_v1.1/POSCAR"]}

data = {"inputs": [], "transfer_token": transfer_token, "source_endpoint": 'e38ee745-6d04-11e5-ba46-22000b92c6ec',
        "dest_endpoint": globus_ep}

dataset = {'dc': {'titles': [{'title': 'Xtract Integration Sample Metadata'}],
  'creators': [{'creatorName': 'Bar, Foo',
    'familyName': 'Bar',
    'givenName': 'Foo',
    'affiliations': ['Totally Legitimate University']},
   {'creatorName': 'Bash Sr., Baz',
    'familyName': 'Bash Sr.',
    'givenName': 'Baz',
    'affiliations': ['Totally Legitimate University']},
   {'creatorName': 'Science',
    'familyName': 'Science',
    'givenName': '',
    'affiliations': ['Totally Legitimate University']}],
  'publisher': 'Materials Data Facility',
  'publicationYear': '2020',
  'resourceType': {'resourceTypeGeneral': 'Dataset',
   'resourceType': 'Dataset'},
  'descriptions': [{'description': 'A dataset for testing Xtract integrations, including validation of valid metadata.',
    'descriptionType': 'Other'}]},
 'mdf': {'source_id': 'bar_xtract_integration_metadata_v1.1',
  'source_name': 'bar_xtract_integration_metadata',
  'version': 1,
  'acl': ['public']},
 'data': {'endpoint_path': 'globus://False/MDF/mdf_connect/dev/data/bar_xtract_integration_metadata_v1.1/',
  'link': 'https://app.globus.org/file-manager?origin_id=False&origin_path=/MDF/mdf_connect/dev/data/bar_xtract_integration_metadata_v1.1/'}}

validation_info = None

id_count = 0
group_count = 0
groups_in_family = 1
family = {"family_id": "test-family", "files": {}, "groups": {}}

for i in range(groups_in_family):
    group = {'group_id': group_count, 'files': [], 'parser': 'image'}
    group_count += 1

    # Here we populate the groups.
    for f_obj in old_mdata["files"]:
        # file_url = f'https://e38ee745-6d04-11e5-ba46-22000b92c6ec.e.globus.org{f_obj}'
        file_url = f'https://82f1b5c6-6e9b-11e5-ba47-22000b92c6ec.e.globus.org{f_obj}'
        print(file_url)
        payload = {
            'url': file_url,
            'headers': headers, 'file_id': id_count}
        id_count += 1
        # Here we add file to family list if it doesn't already exist there.
        if file_url not in family['files']:
            family['files'][file_url] = payload
        group['files'].append(file_url)
    family["groups"][group_count-1] = group


data["inputs"].append(family)
# print(data)

# exit()
task_dict = {"active": Queue(), "pending": Queue(), "results": [], "failed": Queue()}
t_launch_times = {}

assert(n_tasks%burst_size == 0)

for n in range(int(n_tasks/burst_size)):
    print(f"Processing burst: {n}")

    for i in range(burst_size):
        print(headers)
        res = requests.post(url=post_url,
                            headers=headers,
                            json={'endpoint': fx_ep,
                                  'func': fn_id,
                                  'payload': serialize_fx_inputs(
                                      event=data)
                                  }
                            )

        print(res.content)
        if res.status_code == 200:
            task_uuid = json.loads(res.content)['task_uuid']
            task_dict["active"].put(task_uuid)
            t_launch_times[task_uuid] = time.time()
        print(i)

    # TODO: Add ghetto-retry for pending tasks to catch lost ones.
    # TODO x10000: move this logic
    timeout = 120
    failed_counter = 0
    while True:

        if task_dict["active"].empty():
            print("Active task queue empty... sleeping... ")
            time.sleep(0.5)
            break  # This should jump out to main_loop

        cur_tid = task_dict["active"].get()
        print(cur_tid)
        status_thing = requests.get(get_url.format(cur_tid), headers=headers).json()

        if 'result' in status_thing:
            result = fx_ser.deserialize(status_thing['result'])
            print(f"Result: {result}")
            task_dict["results"].append(result)

            unchecked_metadata = result['matio']
            parse_results = _merge_directories(unchecked_metadata, [])
            print(f"With merged directories: {parse_results}")

            merged_records = _merge_files(parse_results)
            print(f"With merged files: {merged_records}")

            vald_gen = vald_obj.validate_mdf_dataset(dataset)
            dataset_entry = next(vald_gen)
            vald_gen.send(None)

            for group in merged_records:
                metadata = group.metadata if isinstance(group.metadata, list) else [group.metadata]
                print(f"Metadata for group w/ parser {group.parser} is: {metadata}")

                for record in metadata:
                    print(f"The record: {record}")
                    #record_entry = vald_gen.send(record)  # TODO 1

                # print("RECORD ENTRY")
                # vald_gen.send(None)  # TODO 2

            print(len(task_dict["results"]))

        elif 'exception' in status_thing:
            print(f"Exception: {fx_ser.deserialize(status_thing['exception'])}")
            # break
        else:
            task_dict["active"].put(cur_tid)

