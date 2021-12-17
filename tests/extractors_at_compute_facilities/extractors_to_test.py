
from extractors.xtract_tabular import TabularExtractor
from extractors.xtract_keyword import KeywordExtractor
from extractors.xtract_imagesort import ImagesExtractor


tabular_extractor = TabularExtractor()
keyword_extractor = KeywordExtractor()
images_extractor = ImagesExtractor()

all_extractors = {"xtract-tabular": {'extractor': tabular_extractor,
                                     'test_files': ['comma_delim']},
                  "xtract-keyword": {'extractor': keyword_extractor,
                                     'test_files': ['freetext']},
                  "xtract-images": {'extractor': images_extractor,
                                    'test_files': ['animal-photography-olga-barantseva-11.jpg']}
                  # "xtract-python",
                  # "xtract-c-code",
                  # "xtract-jsonxml",
                  # "xtract-hdf",
                  # "xtract-netcdf",
                  # "xtract-matio",
                  # "xtract-xpcs",
                  # "xtract-tika"
                  }


