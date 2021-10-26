
from copy import deepcopy

EXTENSION_MAP = {
    'tabular': ['csv', 'dat', 'xls', 'xlsx'],
    'keyword': ['pdf', 'doc', 'docx', 'md', 'README'],
    'jsonxml': ['json', 'xml'],
    'netcdf': ['nc', 'netcdf', 'nc4', 'netcdf4'],
    'images': ['png', 'jpg', 'jpeg', 'tif', 'tiff'],
    'c_code': ['c', 'h'],
    'hdf': ['hdf', 'h5', 'h4', 'h3'],
    'compressed': ['tar', 'gz', 'zip']
}

_uninverted_index = deepcopy(EXTENSION_MAP)

INVERTED_EXTENSION_MAP = dict()
for key in _uninverted_index:
    for value in _uninverted_index[key]:
        INVERTED_EXTENSION_MAP[value] = key



