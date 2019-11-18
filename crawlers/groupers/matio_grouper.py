
from materials_io.utils.interface import get_parser


class MatIOGrouper:

    def group(self, file_ls):
        # TODO: Run each grouping and then each parser. Emulate the behavior in
        # https://github.com/materials-data-facility/MaterialsIO/blob/master/materials_io/utils/interface.py
        parser = get_parser('dft')
        groups = parser.group(file_ls)
        return groups
