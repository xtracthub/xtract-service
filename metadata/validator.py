
# Build from: https://python-jsonschema.readthedocs.io/en/stable/
from jsonschema import validate
from metadata.schemas import *
import json
from mdf_matio.validator import MDFValidator, ValidationError
from mdf_matio.validation import validate_against_mdf_schemas
from materials_io.utils.interface import ParseResult
import os

from mdf_matio.version import __version__  # noqa: F401
from materials_io.utils.interface import (get_available_adapters, ParseResult,
                                          get_available_parsers, run_all_parsers)
from mdf_matio.grouping import groupby_file, groupby_directory
from mdf_matio.validator import MDFValidator
from mdf_toolbox import dict_merge
from typing import Iterable, Set, List
from functools import reduce, partial
import logging
import os
_merge_func = partial(dict_merge, append_lists=True)



dataset_mdata = {'dc': {'titles': [{'title': 'aaa'}],
  'creators': [{'creatorName': 'a',
    'familyName': 'b',
    'givenName': 'ab.',
    'affiliations': ['Cagy']}],
  'publisher': 'Materials Data Facility',
  'publicationYear': '2020',
  'resourceType': {'resourceTypeGeneral': 'Dataset',
   'resourceType': 'Dataset'},
  'descriptions': [{'description': 'MaL thing',
    'descriptionType': 'Other'}]},
 'mdf': {'source_id': 'sstein_stein_bandgap_2020_v1.1',
  'source_name': 'sstein_stein_bandgap_2020',
  'version': 1,
  'acl': ['public']},
 'data': {'endpoint_path': 'globus://jcap/',
  'link': 'https://app.globus.org/file-manager'}}


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


def _merge_files(parse_results: Iterable[ParseResult]) -> Iterable[ParseResult]:
    """Merge metadata of records associated with the same file(s)
    Args:
        parse_results (ParseResult): Generator of ParseResults
    Yields:
        (ParseResult): ParserResults merged for each file.
    """
    return map(_merge_records, groupby_file(parse_results))


class MetadataValidator:

    schema_mappings = {"bert": "bert.json",
                       "image": "image.json",
                       "tabular": "tabular.json",
                       "text_file": "text_file.json"}

    def validate(self, schema, mdata_obj, validator_type="file"):
        """Returns true if metadata adheres to schema.
           Else raises validation error.
           :param schema
           :param mdata_obj

           :raises ValidationError if mdata invalid
           :returns None
        """

        if os.path.isfile(f"schemas/{schema}.json"):
            print("inhere")

        # TODO: Check it

        # validate()

        # TODO: Require strict JSON.
        # TODO: Remove null/None values


def mdf_validate_families(families):
    #print("WAT")
    # family_ls = []
    # validator = MDFValidator(schema_branch="master")
    # vald_gen = validator.validate_mdf_dataset(dataset_mdata)
    #yield next(vald_gen)
    vald = MDFValidator(schema_branch="master")
    vald_gen = vald.validate_mdf_dataset(dataset_mdata)
    yield next(vald_gen)

    for family in families:
        #print(family)

        mdata_ls = family["mdata"]

        f_mdata = [{"length": 1717287078, "filename": "dataset_comp_image_spectra.h5", "globus": "globus://82f1b5c6-6e9b-11e5-ba47-22000b92c6ec/public/demo/jcap/raw/dataset_comp_image_spectra.h5", "data_type": "Hierarchical Data Format (version 5) data"}]

        for group_mdata in mdata_ls:

            matio_mdata = group_mdata['matio']

            # validate_against_mdf_schemas(matio_mdata)

            # for mdata in matio_mdata:


            for group in _merge_files(matio_mdata):
                # Skip records that include only generic metadata
                if group.parser == 'generic':
                    continue

                if group.parser == 'ase':
                    continue
                print("*********************D*D*D*************************************************")
                print(f"Parser: {group.parser}")
                print(group.metadata)

                # Loop over all produced records
                metadata = group.metadata if isinstance(group.metadata, list) else [group.metadata]

                for item in metadata:
                    # TODO: Merge the metadata:
                    final_mdata = {"files": f_mdata,'image': item['image']}
                    print(final_mdata)
                    yield vald_gen.send(final_mdata)
                vald_gen.send(None)


import pickle

with open("../xtract-image.pkl", "rb") as g:
    families = pickle.load(g)["mdata"]
    print(families)


x = mdf_validate_families(families)

search_entries = list(x)

print("PRINTING IT OUT!")
for item in search_entries:
    print(search_entries)
