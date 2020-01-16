
from materials_io.utils.interface import get_parser, get_available_parsers


class MatIOGrouper:

    def __init__(self):
        self.name = "matio"  # TODO: Add to parent class.

    def group(self, file_ls):
        # TODO: Run each grouping and then each parser. Emulate the behavior in
        # https://github.com/materials-data-facility/MaterialsIO/blob/master/materials_io/utils/interface.py

        # TODO [Enhancement]: Get these automatically using Stevedore (or get_all_parsers func?)
        # parser_desc = get_available_parsers()
        # parsers = [*parser_desc]  # This is a lis of all applicable parsers.

        group_coll = {}

        # for parser in parsers:

        # TODO: Uncomment this to add back the other groups.
        # if parser not in ['dft']:
        #     # if parser in ['noop', 'generic']:
        #     continue

        # p = get_parser(parser)
        p = get_parser('dft')
        group = p.group(file_ls)

        # group_coll[parser] = group
        group_coll['dft'] = group

        return group_coll
