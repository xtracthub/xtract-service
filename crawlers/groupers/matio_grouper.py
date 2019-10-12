
from materials_io.utils.interface import get_parser


class MatIOGrouper:

    def group(self, file_ls):
        parser = get_parser('dft')
        groups = parser.group(file_ls)
        return groups
