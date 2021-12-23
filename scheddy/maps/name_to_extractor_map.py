
from extractors.xtract_tabular import TabularExtractor
from extractors.xtract_keyword import KeywordExtractor
from extractors.xtract_imagesort import ImagesExtractor
from extractors.xtract_jsonxml import JsonXMLExtractor
from extractors.xtract_netcdf import NetCDFExtractor
from extractors.xtract_hdf import HDFExtractor
from extractors.xtract_python import PythonExtractor
from extractors.xtract_c_code import CCodeExtractor
from extractors.xtract_matio import MatIOExtractor
from extractors.xtract_tika import TikaExtractor
from extractors.xtract_xpcs import XPCSExtractor

extractor_map = {
    'tabular': TabularExtractor(),
    'keyword': KeywordExtractor(),
    'images': ImagesExtractor(),
    'jsonxml': JsonXMLExtractor(),
    'netcdf': NetCDFExtractor(),
    'hdf': HDFExtractor(),
    'python': PythonExtractor(),
    'c_code': CCodeExtractor(),
    'matio': MatIOExtractor(),
    'tika': TikaExtractor(),
    'xpcs': XPCSExtractor()
}
