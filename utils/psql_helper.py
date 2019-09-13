

import psycopg2

sampler_get = """ SELECT path FROM files WHERE cur_phase='CRAWL';"""
sampler_update = """UPDATE files SET """


# 693
tabular_get = """ SELECT path FROM files WHERE extension='csv' OR extension='tsv' LIMIT {};"""

# 70,000
freetext_get = """ SELECT path FROM files WHERE extension='txt' OR extension='pdf' LIMIT {};"""

# 903,560
matio_get = """SELECT path FROM files WHERE extension='txt' OR extension='xyz' OR extension='lammps' LIMIT {};"""

# 420,330
image_count = """SELECT COUNT(*) FROM files WHERE extension='tiff' OR extension='tif' OR extension='jpg' OR extension='png' OR extension='gif';"""
images_get = """ SELECT path FROM files WHERE extension='tiff' OR extension='tif' OR extension='jpg' OR extension='png' OR extension='gif'; LIMIT {}; """