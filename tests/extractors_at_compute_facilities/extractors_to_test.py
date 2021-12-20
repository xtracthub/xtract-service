
from extractors.xtract_tabular import TabularExtractor
from extractors.xtract_keyword import KeywordExtractor
from extractors.xtract_imagesort import ImagesExtractor
from extractors.xtract_jsonxml import JsonXMLExtractor
from extractors.xtract_hdf import HDFExtractor
from extractors.xtract_python import PythonExtractor
from extractors.xtract_c_code import CCodeExtractor
from extractors.xtract_netcdf import NetCDFExtractor

tabular_extractor = TabularExtractor()
keyword_extractor = KeywordExtractor()
images_extractor = ImagesExtractor()
jsonxml_extractor = JsonXMLExtractor()
hdf_extractor = HDFExtractor()
python_extractor = PythonExtractor()
c_code_extractor = CCodeExtractor()
netcdf_extractor = NetCDFExtractor()

all_extractors = {"xtract-tabular": {'extractor': tabular_extractor,
                                     'test_files': ['comma_delim']},
                  "xtract-keyword": {'extractor': keyword_extractor,
                                     'test_files': ['freetext']},
                  "xtract-images": {'extractor': images_extractor,
                                    'test_files': ['animal-photography-olga-barantseva-11.jpg']},
                  "xtract-jsonxml": {'extractor': jsonxml_extractor,
                                    'test_files': ['jsonxml-test']},
                  "xtract-hdf": {'extractor': hdf_extractor,
                                 'test_files': ['test-hdf-1']},
                  "xtract-python": {'extractor': python_extractor,
                                    'test_files': ['xtract_tabular_main.py']},
                  "xtract-c-code": {'extractor': c_code_extractor,
                                    'test_files': ['testfile.c']},
                  "xtract-netcdf": {'extractor': netcdf_extractor,
                                    'test_files': ['test-netcdf.nc']},
                  # "xtract-matio",
                  # "xtract-xpcs",
                  # "xtract-tika"
                  }


