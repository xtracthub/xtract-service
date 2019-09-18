
import time
import datetime

from queue import Queue
from files import FileObj

import json
import sys
import pickle
import psycopg2
from psycopg2.extras import Json, DictCursor

from funcx.sdk.client import FuncXClient

# TODO: Make a class that represents a group
fxc = FuncXClient()

class Orchestrator:

    def __init__(self, directory, crawl_type, crawl_dict):
        # TODO: Move much of this to config file.
        self.crawl_dict = '/Users/tylerskluzacek/Desktop/result.json'  # TODO: Remove hardcode.
        #self.endpoint_uuid = 'a92945a1-2778-4417-8cd1-4957bc35ce66'
        self.endpoint_uuid = '3cc6d6ce-9a60-4559-bf21-1a3d2ce5da20'
        self.globus_target_uuid = 'e38ee745-6d04-11e5-ba46-22000b92c6ec'
        self.globus_https_base = "https://{}.e.globus.org/".format(self.globus_target_uuid)

        self.counter = 0

        ''' Init metadata_collection with k:v -> (str) file_uri : (dict) metadata'''
        self.metadata_map = {}
        self.active_extraction = set()  # Set of files currently in the extraction process.
        self.max_concurrency = 4
        self.total_files = 0  # Total number of files -- fully updated AFTER CRAWL

        self.extraction_queues = {"sampler_starting": Queue(),
                                  "sampler_completed": Queue(),
                                  "unknown": Queue()}  # (dict) k:v -> (str) extractor_name : (Queue) work_queue.

        self.trash_set = set()  # Set of files that have not returned valid metadata until after extraction
        self.directory = directory
        self.crawl_type = crawl_type
        self.crawl_dict = crawl_dict

    def orchestrate(self, crawl_file):
        """ STEP 1: CRAWL user-provided directory """
        crawl_dict = self.crawler()

        for file_uri in crawl_dict:
            file_obj = FileObj(file_uri, metadata=crawl_dict[file_uri], inflated=False)  # TODO: Check if file was deflated.
            self.metadata_map[file_uri] = file_obj
            self.total_files += 1
            self.extraction_queues["sampler_starting"].put(file_obj)

        """ STEP 2 :: SAMPLE each file by predicting on a few bytes of the file """
        for file_obj in iter(self.extraction_queues["sampler_starting"].get(), None):
            self.sampler(file_obj)

        """ STEP 3 :: SEND OFF EXTRACTION TASKS -- funcX will do the container scheduling :) """
        # STEP 3 :: Send the tasks to the appropriate queue.


        print("Finished sending off tasks to appropriate extractors")

    def get_url(self):
        print("TODO.")

    def get_native_client(self):
        from fair_research_login import NativeClient
        client = NativeClient(client_id='7414f0b4-7d05-4bb6-bb00-076fa3f17cf5')
        tokens = client.login(
            requested_scopes=['https://auth.globus.org/scopes/56ceac29-e98a-440a-a594-b41e7a084b62/all'],
            no_local_server=True, 
            no_browser=True)
        auth_token = tokens["petrel_https_server"]['access_token']
        self.headers = {'Authorization': f'Bearer {auth_token}'}
        # return headers

        # TODO: REASSIGN/CLOSE TASKS THREAD

    # TODO: Right now this is running on Cooley.
    def crawler(self, crawl_type="POSIX"):
        print("[TODO] Crawl UNIX directories")
        print("[TODO] Crawl Globus directories")

    def sampler_loop(self, orch, batch_size=1):
        sampler_query = """ SELECT file_id, path FROM files WHERE cur_phase='CRAWLED' LIMIT {};"""
        sampling_update = """UPDATE files SET cur_phase='SAMPLED', sample_type='{}', cur_task_id='{}', extract_time={}, transfer_time={}, total_time={} WHERE file_id={};"""

        try:
            conn = psycopg2.connect(
        except Exception as e:
            print("Cannot connect to database")
            raise e
        cur = conn.cursor()
        existing_files = 0
        old_task_ids = {}
        task_ids = {}

        while True:
            total_size = 50 - existing_files
            print("ATTEMPTING TO PULL DOWN {} FILES.".format(total_size))
            cur.execute(sampler_query.format(total_size))
            file_records = cur.fetchall()

            print("Pulled down {} more file records!".format(len(file_records)))
            counter = 0
            path_list = []

            for item in file_records:
                file_id, raw_path = item
                path_list.append(item)
                counter += 1
                if counter % batch_size == 0:
                    # TODO: Never launching the last n jobs.
                    print("Invoking batch!")
                    task_id1 = orch.sampler(path_list)
                    task_ids[task_id1] = file_id
                    path_list = []
                time.sleep(0.25)

                if counter % 100 == 0:
                    print(counter)
            # Do checks for new results!
            removable_task_ids = []

            # Iterate over batches
            print("Processing {} task ids!".format(len(task_ids)))
            for id in task_ids:
                status = fxc.get_task_status(id)
                print(status)

                if status['status'] == 'SUCCEEDED':
                    for item in status['details']['result']['metadata']:
                    # mdata = status['details']['result']['metadata'][0]
                        # print(mdata)
                        extract_time = item['extract time']
                        transfer_time = status['details']['result']['trans_time']
                        tot_time = status['details']['result']['tot_time']

                        f_type = None
                        for key in item['sampler']:
                            f_type = item['sampler'][key]
                            break
                        update_query = sampling_update.format(f_type, id, extract_time, transfer_time, tot_time,
                                                              item['file_id'])
                        print(update_query)
                        cur.execute(update_query)
                    print("Committing to DB!")
                    conn.commit()
                    removable_task_ids.append(id)
                elif status['status'] == 'FAILED':
                    print("Fancy retry scenario. ****************")
                    # time.sleep(10)
            # Clean up the searchable dict
            for id in removable_task_ids:
                del task_ids[id]
            # for id in old_task_ids:
            #     if id in task_ids:
            #         del task_ids[id]
            old_task_ids = task_ids
            existing_files = len(task_ids)*batch_size  # TODO: Not quite accurate in case batch isn't full.
            print("restarting!")
            time.sleep(1)

    # TODO: Right now training this can't be run in PetrelKube.
    def sampler(self, filepaths):
        """ Sample files from the sampler_starting queue. """

        sampler_func_id = 'c5432da1-4899-4639-aa95-7aeb159b55f4'

        data = {'inputs': []}

        for item in filepaths:

            file_id, filepath = item

            url = '{}{}'.format(self.globus_https_base, filepath.replace('//', '/'))
            url = url.replace('/~/', '/')
            url = url.replace('e.globus.org/', 'e.globus.org')

            payload = {'url': url,
                       'headers': self.headers, 'file_id': file_id}

            data['inputs'].append(payload)

        # print("DATA: {}".format(data))
        task_id = fxc.run(data, self.endpoint_uuid, sampler_func_id, asynchronous=True, async_poll=20)

        return task_id

    def tabular_extract(self, filename=None):
        # filename = "/~/MDF/mdf_connect/prod/data/binary_metallic_alloys_ab_initio_v1/mmc1.csv"

        func_uuid = 'cd75cfe7-b4d0-4f16-bd0d-cc5d30b2ffa9'

        # Creating the payload
        # payload = {'url': 'https://e38ee745-6d04-11e5-ba46-22000b92c6ec.e.globus.org/MDF/mdf_connect/prod/data/binary_metallic_alloys_ab_initio_v1/mmc1.csv', 'headers': self.headers}

        url = '{}{}'.format(self.globus_https_base, filename.replace('//', '/'))
        url = url.replace('/~/', '/')
        url = url.replace('e.globus.org/', 'e.globus.org')
        print(url)
        payload = {'url': url, 'headers': self.headers}
        print("Payload: {}".format(payload))
        # payload = {'url': 'https://e38ee745-6d04-11e5-ba46-22000b92c6ec.e.globus.org/MDF/mdf_connect/prod/data/_test_einstein_9vpflvd_v1.1/INCAR'}  # MDF
        # payload = {'url': 'https://3a261574-3a83-11e8-b997-0ac6873fc732.e.globus.org/weather_2012.csv'}  # CDIAC
        # print("Payload is {}".format(payload))
        print("Sending payload to endpoint...")
        res = fxc.run(payload, self.endpoint_uuid, func_uuid, asynchronous=True, async_poll=60)

        # print("Waiting for result...")
        # result = res.result()
        return res

        # TODO:  If preamble, then requeue for keyword w/ freetext_payload.


    def keyword_extract(self, filepath=None, freetext_payload=None, prior_extractor=None):
        func_uuid = '9a22f1e1-f7b1-476a-a6c2-42c7fd2eba1a'

        # Creating the payload
        payload = {'url': '{}{}'.format(self.globus_https_base, filepath),
                   'headers': self.headers}

        print("Payload is {}".format(payload))

        res = fxc.run(payload, self.endpoint_uuid, func_uuid, asynchronous=True)

        print("Waiting for result...")
        result = res.result()
        return result


    def image_loop(self, orch):
        images_query = """ SELECT file_id, path FROM files WHERE cur_phase NOT LIKE 'IMAGES%' and (extension='jpg' or extension='tif' or extension='tiff' or extension='png') LIMIT {};"""

        images_intermed = """UPDATE files SET cur_phase='IMAGES0' WHERE file_id={};"""

        images_update = """UPDATE files SET cur_phase='IMAGES1', cur_task_id='{}', metadata={}, downstream_time={}, transfer_time={} WHERE file_id={};"""

        try:
            conn = psycopg2.connect(
        except Exception as e:
            print("Cannot connect to database")
            raise e

        cur = conn.cursor()

        existing_files = 0
        old_task_ids = {}

        task_ids = {}

        while True:

            total_size = 4000 - existing_files

            print("ATTEMPTING TO PULL DOWN {} FILES.".format(total_size))

            cur.execute(images_query.format(total_size))

            file_records = cur.fetchall()

            print("Pulled down {} more file records!".format(len(file_records)))

            counter = 0

            path_list = []

            for item in file_records:
                file_id, raw_path = item

                path_list.append(item)


                query = images_intermed.format(file_id)
                cur.execute(query)

                query = images_intermed.format(file_id)

                counter += 1
                if counter % batch_size == 0:
                    # TODO: Never launching the last n jobs.
                    print("Invoking batch!")
                    task_id1 = orch.image_extract(path_list)

                    task_ids[task_id1] = file_id
                    path_list = []


                    conn.commit()
                time.sleep(0.1)

                if counter % 100 == 0:
                    print(counter)


            # Do checks for new results!
            removable_task_ids = []

            # Iterate over batches

            print("Processing {} task ids!".format(len(task_ids)))
            for id in task_ids:

                status = fxc.get_task_status(id)

                print(status)
                time.sleep(0.1)

                if status['status'] == 'SUCCEEDED':

                    if 'metadata' in status['details']['result']:
                        for item in status['details']['result']['metadata']:

                            # mdata = status['details']['result']['metadata'][0]

                            # print(mdata)

                            try:
                                extract_time = item['extract time']
                                transfer_time = item['trans_time']
                                task_id = status['task_id']
                                # tot_time = status['details']['result']['tot_time']

                                # img_type = Json({'img_type': item['image-sort']['img_type']})

                                # cur_task_id='{}', metadata={}, downstream_time={}, transfer_time={} WHERE file_id={}
                                update_query = images_update.format(task_id, extract_time, transfer_time, item['file_id'])
                                print(update_query)

                                cur.execute(update_query)
                            except:
                                print("FAILURE!!!")
                                # removable_task_ids.append(id)

                        print("Committing to DB!")
                        conn.commit()

                        removable_task_ids.append(id)

                elif status['status'] == 'FAILED':
                    print("Fancy retry scenario. ****************")
                    removable_task_ids.append(id)
                    # time.sleep(10)

            # Clean up the searchable dict
            for id in removable_task_ids:
                del task_ids[id]

            # for id in old_task_ids:
            #     if id in task_ids:
            #         del task_ids[id]

            old_task_ids = task_ids

            existing_files = len(task_ids) * batch_size  # TODO: Not quite accurate in case batch isn't full.

            print("restarting!")
            time.sleep(20)

    def image_extract(self, filepaths, prior_extractor=None):

        func_uuid = "74fa1add-bb10-4726-9197-292a0ac97392"

        data = {'inputs': []}

        for item in filepaths:

            file_id, filepath = item

            url = '{}{}'.format(self.globus_https_base, filepath.replace('//', '/'))
            url = url.replace('/~/', '/')
            url = url.replace('e.globus.org/', 'e.globus.org')

            payload = {'url': url,
                    'headers': self.headers, 'file_id': file_id}

            data['inputs'].append(payload)
        
        dlhub_endpoint = 'a92945a1-2778-4417-8cd1-4957bc35ce66'

        if self.counter % 2 == 0:
            endpoint_uuid = dlhub_endpoint
        else:
            endpoint_uuid = self.endpoint_uuid

        self.counter += 1
        task_id = fxc.run(data, endpoint_uuid, func_uuid, asynchronous=True, async_poll=20)

        # print("[TODO] Automatically make calls to map extractor.")
        # print("[TODO] Send keywords to keyword extractor")

        return task_id


    def json_xml_extract(self, filepath):
        func_uuid = "368dcef9-5eed-4caa-8eea-e1249e42b0ed"

        data = {'inputs': []}

        for item in filepaths:
            file_id, filepath = item

            url = '{}{}'.format(self.globus_https_base, filepath.replace('//', '/'))
            url = url.replace('/~/', '/')
            url = url.replace('e.globus.org/', 'e.globus.org')

            payload = {'url': url,
                       'headers': self.headers, 'file_id': file_id}

            data['inputs'].append(payload)

        dlhub_endpoint = 'a92945a1-2778-4417-8cd1-4957bc35ce66'

        if self.counter % 2 == 0:
            endpoint_uuid = dlhub_endpoint
        else:
            endpoint_uuid = self.endpoint_uuid

        self.counter += 1
        task_id = fxc.run(data, endpoint_uuid, func_uuid, asynchronous=True, async_poll=20)

        return task_id

    def netcdf_extract(self, filepath):
        print("Extractor NetCDF files here")
        print("Send freetext data to FREETEXT")

    def maps_extract(filepath):
        print("Extract from maps")


    def materials_loop(self, orch):
        materials_query = """ SELECT file_id, path FROM files WHERE cur_phase NOT LIKE 'MATERIALS%' and (extension='xyz') LIMIT {};"""
        materials_intermed = """UPDATE files SET cur_phase='MATERIALS0' WHERE file_id={};"""
        materials_update = """UPDATE files SET cur_phase='MATERIALS-XYZ1', cur_task_id='{}', downstream_time={}, transfer_time={} WHERE file_id={};"""

        try:
            conn = psycopg2.connect(
        except Exception as e:
            print("Cannot connect to database")
            raise e

        cur = conn.cursor()

        existing_files = 0
        old_task_ids = {}

        task_ids = {}

        while True:

            total_size = 000 - existing_files

            print("ATTEMPTING TO PULL DOWN {} FILES.".format(total_size))

            cur.execute(materials_query.format(total_size))

            file_records = cur.fetchall()

            print("Pulled down {} more file records!".format(len(file_records)))

            counter = 0

            path_list = []

            for item in file_records:
                file_id, raw_path = item

                path_list.append(item)

                query = materials_intermed.format(file_id)
                cur.execute(query)

                query = materials_intermed.format(file_id)

                counter += 1
                if counter % batch_size == 0:
                    # TODO: Never launching the last n jobs.
                    print("Invoking batch!")
                    task_id1 = orch.materials_extract(path_list)
                    task_ids[task_id1] = file_id
                    path_list = []
                # time.sleep(0.5)

                if counter % 100 == 0:
                    print(counter)

            # Do checks for new results!
            removable_task_ids = []

            # Iterate over batches
            print("Processing {} task ids!".format(len(task_ids)))
            for id in task_ids:

                status = fxc.get_task_status(id)

                print(status)
                time.sleep(0.1)

                if status['status'] == 'SUCCEEDED':

                    if 'metadata' in status['details']['result']:
                        for item in status['details']['result']['metadata']:

                            # mdata = status['details']['result']['metadata'][0]

                            # print(mdata)

                            try:
                                extract_time = item['extract time']
                                transfer_time = item['trans_time']
                                task_id = status['task_id']
                                # tot_time = status['details']['result']['tot_time']

                                # img_type = Json({'img_type': item['image-sort']['img_type']})

                                # cur_task_id='{}', metadata={}, downstream_time={}, transfer_time={} WHERE file_id={}
                                update_query = materials_update.format(task_id, extract_time, transfer_time,
                                                                       item['file_id'])
                                print(update_query)

                                cur.execute(update_query)
                            except:
                                print("FAILURE!!!")
                                # removable_task_ids.append(id)

                        print("Committing to DB!")
                        conn.commit()

                        removable_task_ids.append(id)

                elif status['status'] == 'FAILED':
                    print("Fancy retry scenario. ****************")
                    removable_task_ids.append(id)
                    # time.sleep(10)

            # Clean up the searchable dict
            for id in removable_task_ids:
                del task_ids[id]

            # for id in old_task_ids:
            #     if id in task_ids:
            #         del task_ids[id]

            old_task_ids = task_ids

            existing_files = len(task_ids) * batch_size  # TODO: Not quite accurate in case batch isn't full.

            print("restarting!")
            time.sleep(30)


    def materials_extract(self, filepaths, group_logic='file'):
        if group_logic == 'file':
            func_uuid = '4925e5bc-59f0-4b14-b655-e771fe74f603'

            data = {'inputs': []}

            for item in filepaths:

                file_id, filepath = item

                # print(filepath)
                url = '{}{}'.format(self.globus_https_base, filepath.replace('//', '/'))
                url = url.replace('/~/', '/')
                url = url.replace('e.globus.org/', 'e.globus.org')

                payload = {'url': url,
                           'headers': self.headers, 'file_id': file_id}

                data['inputs'].append(payload)

            if self.counter % 2 == 0:
                endpoint_uuid = self.endpoint_uuid
            else:
                endpoint_uuid = "a92945a1-2778-4417-8cd1-4957bc35ce66"
            self.counter += 1

            print("DATA: {}".format(data))
            task_id = fxc.run(data, endpoint_uuid, func_uuid, asynchronous=True, async_poll=20)
            # Note that this will change each time the function is re-registered.
            # TODO: MAKE ISSUE: [CACHE] Database keeping track of function registration changes.

            # payload = {'url': '{}{}'.format(self.globus_https_base, uri_path),
            #            'headers': self.headers}
            # print("Payload is {}".format(payload))
            #
            # res = fxc.run(payload, self.endpoint_uuid, func_uuid, asynchronous=True)
            #
            # print("Waiting for result...")
            # # result = res.result()
            return task_id


if __name__ == "__main__":

    orch = Orchestrator()

    # Get a new token.
    orch.get_native_client()

    t0 = time.time()
    # print(orch.tabular_extract('bumbum'))
    # print(orch.keyword_extract('/MDF/mdf_connect/prod//data//mdr_item_1505_v2//iprPy_v0_6.tar//iprPy_v0_6//iprPy//records//LAMMPS-potential//README.md'))

    task_ids = []

    import json

    # with open('/Users/tylerskluzacek/Desktop/result.json', 'r') as f:
    #
    #     crawl_data = json.load(f)

    i = 0

    # Images here!
    batch_size = 1

    max_files = 25
    filepaths = []

    file_set = set()  # helper to ensure we're not sending duplicate filepaths.

    # orch.sampler_loop(orch, batch_size=batch_size)

    jsonxml_query = """ SELECT file_id, path FROM files WHERE cur_phase NOT LIKE 'JSONXML%' and (extension='json' or extension='xml') LIMIT {};"""
    jsonxml_intermed = """UPDATE files SET cur_phase='JSONXML0' WHERE file_id={};"""
    jsonxml_update = """UPDATE files SET cur_phase='JSONXML1', cur_task_id='{}', downstream_time={}, transfer_time={}, extract_time={} WHERE file_id={};"""

    try:
        conn = psycopg2.connect(
    except Exception as e:
        print("Cannot connect to database")
        raise e

    cur = conn.cursor()

    existing_files = 0
    old_task_ids = {}

    task_ids = {}

    while True:

        total_size = 100 - existing_files

        print("ATTEMPTING TO PULL DOWN {} FILES.".format(total_size))

        cur.execute(jsonxml_query.format(total_size))

        file_records = cur.fetchall()

        print("Pulled down {} more file records!".format(len(file_records)))

        counter = 0

        path_list = []

        for item in file_records:
            file_id, raw_path = item

            path_list.append(item)

            query = jsonxml_intermed.format(file_id)
            cur.execute(query)

            # query = jsonxml_intermed.format(file_id)

            counter += 1
            if counter % batch_size == 0:
                # TODO: Never launching the last n jobs.
                print("Invoking batch!")
                task_id1 = orch.json_xml_extract(path_list)
                task_ids[task_id1] = file_id
                path_list = []
            # time.sleep(0.5)

            if counter % 100 == 0:
                print(counter)

        # Do checks for new results!
        removable_task_ids = []

        # Iterate over batches
        print("Processing {} task ids!".format(len(task_ids)))
        for id in task_ids:

            status = fxc.get_task_status(id)

            print(status)
            time.sleep(0.1)

            if status['status'] == 'SUCCEEDED':

                # print(status)

                if status['details']['result']:
                    for item in status['details']['result']['metadata']:

                        # mdata = status['details']['result']['metadata'][0]

                        # print(mdata)

                        #try:
                        extract_time = item['extract time']
                        transfer_time = item['trans_time']
                        task_id = status['task_id']
                        # tot_time = status['details']['result']['tot_time']

                        # img_type = Json({'img_type': item['image-sort']['img_type']})

                        # cur_task_id='{}', metadata={}, downstream_time={}, transfer_time={} WHERE file_id={}
                        update_query = jsonxml_update.format(task_id, extract_time, transfer_time, item['file_id'])
                        print(update_query)

                        cur.execute(update_query)
                        # except:
                        #     print("FAILURE!!!")
                        #     removable_task_ids.append(id)

                    print("Committing to DB!")
                    conn.commit()

                removable_task_ids.append(id)

            elif status['status'] == 'FAILED':
                print("Fancy retry scenario. ****************")
                removable_task_ids.append(id)
                # time.sleep(10)

        # Clean up the searchable dict
        for id in removable_task_ids:
            del task_ids[id]

        # for id in old_task_ids:
        #     if id in task_ids:
        #         del task_ids[id]

        old_task_ids = task_ids

        existing_files = len(task_ids) * batch_size  # TODO: Not quite accurate in case batch isn't full.

        print("restarting!")
        time.sleep(3)


    # print(t1-t0)
