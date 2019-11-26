
from materials_io.utils.interface import get_parser, get_available_parsers


class MatIOGrouper:

    def group(self, file_ls):
        # TODO: Run each grouping and then each parser. Emulate the behavior in
        # https://github.com/materials-data-facility/MaterialsIO/blob/master/materials_io/utils/interface.py

        # TODO [Enhancement]: Get these automatically using Stevedore (or get_all_parsers func?)
        parser_desc = get_available_parsers()
        parsers = [*parser_desc]

        print(parsers)

        group_coll = {}

        for parser in parsers:
            parser = get_parser(parser)
            group = parser.group(file_ls)

            group_coll[parser] = group

        return group_coll


# mig = MatIOGrouper()
# files = ['a', 'b', 'c']
#
# mig.group(files)