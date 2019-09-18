
import psycopg2
from psycopg2.extras import Json, DictCursor
import pickle as pkl
import json
import time


def offload_crawled_file(cur, path, data_item):

    # print(path)

    data_item = data_item["physical"]
    mdata = {}

    # TODO: Fix the hardcodings of time stamp, owner.
    query = """INSERT
    INTO
    files (path, size, extension, sample_type, cur_phase, cur_task_id, metadata, created_on, last_extracted, owner, crawl_type)
    VALUES('{}', {}, '{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}'
           );""".format(path, data_item['size'], data_item['extension'], 'NONE', 'CRAWLED', 'INITID',
                        Json(mdata), '2019-08-24 20:10:25-07', '2019-08-24 20:10:25-07', 'MDF',
                        data_item['path_type'])

    # print(query)
    try:
        cur.execute(query)
    except Exception as e:
        print(e)
        print(path)


def check_results(cur):
    query = """ SELECT metadata FROM files2 WHERE path='potato3';"""

    print(query)
    result = cur.execute(query)

    for item in result:
        print(item)


with open('/Users/tylerskluzacek/Desktop/result.json', 'r') as f:

    crawl_data = json.load(f)


t0 = time.time()
i = 0
for item in crawl_data:

    if i < 1946000:
        i += 1
        if i % 100000 == 0:
            print(i)
             #i += 1
        continue

    cur = conn.cursor(cursor_factory=DictCursor)
    offload_crawled_file(cur, item, crawl_data[item])
    i += 1

    if i % 1000 == 0:
        print("Committing up to {} files...".format(i))
        try:
            # print("In here")
            conn.commit()

        # TODO: This error happens on EXECUTE (not commit)
        except Exception as e:
            print(e)
            print(item)
            print("INT TOO BIG. ")







