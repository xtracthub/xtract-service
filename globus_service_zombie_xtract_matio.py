
import threading
import requests
import logging
import psycopg2
import pickle
import time
import json


from funcx.serialize import FuncXSerializer
from queue import Queue


from utils.pg_utils import pg_conn


fx_ser = FuncXSerializer()

# TODO: Figure out how to be smart about these DB requests -- should I have multiple cursors? Commit each? Batch commit?
# TODO: Do a proper logger to a file.


def serialize_fx_inputs(*args, **kwargs):
    from funcx.serialize import FuncXSerializer
    fx_serializer = FuncXSerializer()
    ser_args = fx_serializer.serialize(args)
    ser_kwargs = fx_serializer.serialize(kwargs)
    payload = fx_serializer.pack_buffers([ser_args, ser_kwargs])
    return payload


class MatioExtractor:
    # TODO: Cleanup unnnecessary args.
    def __init__(self, crawl_id, headers, funcx_eid, source_eid, dest_eid,
                 mdata_store_path, logging_level='debug', suppl_store=False):
        self.funcx_eid = funcx_eid
        self.func_id = "148c9dcc-d9c6-430b-b3a7-ddd02193f418"
        self.source_endpoint = source_eid
        self.dest_endpoint = dest_eid

        self.task_dict = {"active": Queue(), "pending": Queue(), "failed": Queue()}

        self.headers = headers
        self.batch_size = 1
        self.max_active_batches = 10
        self.crawl_id = crawl_id

        self.mdata_store_path = mdata_store_path
        self.suppl_store = suppl_store

        self.conn = pg_conn()
        self.fx_headers = {"Authorization": f"Bearer {self.headers['FuncX']}", 'FuncX': self.headers['FuncX']}
        self.post_url = 'https://dev.funcx.org/api/v1/submit'
        self.get_url = 'https://dev.funcx.org/api/v1/{}/status'

        self.logging_level = logging_level

        self.logger = logging.getLogger(__name__)
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.DEBUG)

        # TODO: Add a preliminary loop-polling 'status check' on the endpoint that returns a noop
        # TODO: And do it here in the init.

    def get_next_groups(self):
        try:
            query = f"SELECT group_id FROM groups WHERE status='crawled' " \
                f"and crawl_id='{self.crawl_id}' LIMIT {self.batch_size};"
            cur = self.conn.cursor()
            cur.execute(query)
            self.logger.debug(f"Successfully retrieved extractable file list of {self.batch_size} items from database.")
            gids = cur.fetchall()
            return gids

        # TODO: Catch connection errors.
        except:
            # TODO: Don't exit lol.
            print("FUCK. ")
            # exit()

    def get_test_files(self):

        try:
            query = f"SELECT group_id FROM groups " \
                f"and crawl_id='{self.crawl_id}' LIMIT {self.batch_size};"
            cur = self.conn.cursor()
            cur.execute(query)
            self.logger.debug(f"Successfully retrieved extractable file list of {self.batch_size} items from database.")
            gids = cur.fetchall()
            return gids
        except:
            print("[Xtract] Unable to fetch new group_ids from DB")

    def send_files(self):
        # Just keep looping in case something comes up in crawl.
        while True:
            data = {'inputs': []}

            gids = self.get_next_groups()
            self.logger.info(f"Received {len(gids)} tasks from DB!")

            for gid in gids:
                self.logger.debug(f"Processing GID: {gid[0]}")

                # Update DB to flag group_id as 'EXTRACTING'.
                cur = self.conn.cursor()
                try:
                    update_q = f"UPDATE groups SET status='EXTRACTING' WHERE group_id='{gid[0]}';"
                    cur.execute(update_q)
                    self.conn.commit()
                except Exception as e:
                    print("[Xtract] Unable to update status to 'EXTRACTING'.")
                    print(e)

                # Get the metadata for each group_id
                try:
                    get_mdata = f"SELECT metadata FROM group_metadata_2 where group_id='{gid[0]}' LIMIT 1;"
                    cur.execute(get_mdata)
                    old_mdata = pickle.loads(bytes(cur.fetchone()[0]))
                except Exception as e:  # TODO: this MUST be less broad.
                    print("[Xtract] Unable to retrieve metadata")
                    print(e)

                # TODO: Partial groups can occur here.
                # Take all of the files and append them to our payload.
                for f_obj in old_mdata["files"]:
                    payload = {
                        # TODO: Un-hardcode. This is hardcoded to Petrel data addresses.
                        'url': f'https://e38ee745-6d04-11e5-ba46-22000b92c6ec.e.globus.org{f_obj}',
                        'headers': self.fx_headers, 'file_id': gid[0]}
                    data["inputs"].append(payload)
                    data["transfer_token"] = self.headers['Transfer']

                    data["source_endpoint"] = self.source_endpoint
                    data["dest_endpoint"] = self.dest_endpoint

                res = requests.post(url=self.post_url,
                                    headers=self.fx_headers,
                                    json={
                                        'endpoint': self.funcx_eid,
                                          'func': self.func_id,
                                          'payload': serialize_fx_inputs(
                                              event=data)}
                                    )

                fx_res = json.loads(res.content)

                # TODO: Actually do something here.
                if fx_res["status"] == "failed":
                    self.logger.error("TODO: DO SOMETHING IF BAD FUNCX ENDPOINT GIVEN. ")

                task_uuid = fx_res['task_uuid']

                self.logger.info(f"Placing Task ID {task_uuid} onto live queue")
                self.task_dict["active"].put(task_uuid)
                self.logger.info(f"Approximate current size of live task queue: {self.task_dict['active'].qsize()}")

            while True:
                if self.task_dict["active"].qsize() < self.max_active_batches:
                    self.logger.debug(f"FEWER THAN {self.max_active_batches} LIVE TASKS! ")
                    break
                else:
                    pass

                cur = self.conn.cursor()
                crawled_query = f"SELECT COUNT(*) FROM groups WHERE status='crawled' AND crawl_id='{self.crawl_id}';"
                cur.execute(crawled_query)
                count_val = cur.fetchall()[0][0]

                self.logger.debug(f"Num active Task IDs: {self.task_dict['active'].qsize()}")
                self.logger.debug(f"Files left to extract: {count_val}")

                if count_val == 0:
                    self.logger.info("There are no more unprocessed objects")
                    break

                time.sleep(1)

    def launch_extract(self):
        ex_thr = threading.Thread(target=self.send_files, args=())
        ex_thr.start()

    def launch_poll(self):
        po_thr = threading.Thread(target=self.poll_responses, args=())
        po_thr.start()

    def poll_responses(self):
        success_returns = 0
        while True:
            if self.task_dict["active"].empty():
                self.logger.debug("No live IDs... sleeping...")
                time.sleep(1)
                continue

            num_elem = self.task_dict["active"].qsize()
            for _ in range(0, num_elem):
                ex_id = self.task_dict["active"].get()
                status_thing = requests.get(self.get_url.format(ex_id), headers=self.fx_headers)
                status_thing = json.loads(status_thing.content)

                self.logger.debug(f"Status: {status_thing}")

                if "result" in status_thing:
                    res = fx_ser.deserialize(status_thing['result'])
                    if res == True:
                        success_returns += 1
                        print(success_returns)
                    res = fx_ser.deserialize(status_thing['result'])
                    self.logger.debug(f"Received response: {res}")

                    for g_obj in res["metadata"]:
                        gid = g_obj["group_id"]

                        cur = self.conn.cursor()
                        # TODO: [Optimize] Do something smarter than pulling this down a second time (cache???)
                        get_mdata = f"SELECT metadata FROM group_metadata_2 where group_id='{gid}';"
                        cur.execute(get_mdata)
                        #     cur.execute(get_mdata)  # TODO: Is this second one necessary?
                        old_mdata = pickle.loads(bytes(cur.fetchone()[0]))
                        old_mdata["matio"] = g_obj["matio"]

                        self.logger.debug("Pushing freshly-retrieved metadata to DB (Update: 1/2)")
                        update_mdata = f"UPDATE group_metadata_2 SET metadata={psycopg2.Binary(pickle.dumps(old_mdata))} where group_id='{gid}';"
                        cur.execute(update_mdata)

                        # TODO: Make sure we're updating the parser-list in the db.
                        self.logger.debug("Updating group status (Update: 2/2)")
                        update_q = f"UPDATE groups SET status='EXTRACTED' WHERE group_id='{gid}';"
                        cur.execute(update_q)
                        self.conn.commit()
                    break
                elif 'exception' in status_thing:
                    exc = fx_ser.deserialize(status_thing['exception'])
                    self.logger.error(f"Caught Exception: {exc}")
                else:
                    self.task_dict["active"].put(ex_id)

            # TODO: Get rid of sleep soon.
            time.sleep(2)


# TODO: .put() data for HTTPS on Petrel.

# TODO: Smarter separation of groups in file system (but not important at smaller scale).
# TODO: Move these functions to outside this file (like a dir of functions.

def fatio_test(event):
    # from __future__ import with_statement
    import gc
    import sys
    f = open('/dev/null', 'w')
    sys.stdout = f
    sys.stderr = f

    # return "HERE

    def extract_metadata(path):

        return "In function -- returning before we do anything. "

        import sys
        f = open('/dev/null', 'w')
        sys.stdout = f
        sys.stderr = f

        from pymatgen.io.ase import AseAtomsAdaptor
        from pymatgen.core import Structure

        # TODO: CHECK 3 -- Made it here: 2483/2500 tasks loading JUST the pymatgen libs (w/o devnull)
        # return "Loaded the pymatgen libraries only (Check 2)"

        from mdf_toolbox import dict_merge
        from ase.io import read



        # TODO: CHECK 2 -- Made it here: 2497/2500 tasks completed successfully loading all 4 libs! (w/o devnull)
        # return "Loaded the libraries!"

        # from materials_io.base import BaseSingleFileParser

        class CrystalStructureParser():
            """Parse information about a crystal structure"""

            def _parse_file(self, path, context=None):
                record = {}
                material = {}
                crystal_structure = {}
                # Attempt to read the file
                try:
                    # Read with ASE
                    ase_res = read(path)
                    # Check data read, validate crystal structure
                    if not ase_res or not all(ase_res.get_pbc()):
                        raise ValueError("No valid data")
                    else:
                        # Convert ASE Atoms to Pymatgen Structure
                        pmg_s = AseAtomsAdaptor.get_structure(ase_res)
                # ASE failed to read file
                except Exception:
                    try:
                        # Read with Pymatgen
                        pmg_s = Structure.from_file(path)
                    except Exception:
                        # Can't read file
                        raise ValueError('File not readable by pymatgen or ase: {}'.format(path))

                # Parse material block
                material["composition"] = pmg_s.formula.replace(" ", "")

                # Parse crystal_structure block
                crystal_structure["space_group_number"] = pmg_s.get_space_group_info()[1]
                crystal_structure["number_of_atoms"] = float(pmg_s.composition.num_atoms)
                crystal_structure["volume"] = float(pmg_s.volume)
                crystal_structure["stoichiometry"] = pmg_s.composition.anonymized_formula

                # Add to record
                record = dict_merge(record, {"material": material,
                                             "crystal_structure": crystal_structure})
                return record

            def implementors(self):
                return ['Jonathon Gaff']

            def version(self):
                return '0.0.1'

        parser = CrystalStructureParser()
        mdata = parser._parse_file(path=path)
        return mdata


    """
    Function
    :param event (dict) -- contains auth header and list of HTTP links to extractable files:
    :return metadata (dict) -- metadata as gotten from the materials_io library:
    """
    import threading
    import os
    import time
    import shutil
    import sys
    import random
    import tempfile
    from queue import Queue
    from datetime import datetime
    import subprocess
    import requests

    # subprocess.call(['python3', '-m', 'pip', 'install', 'home-run'])

    try:
        from home_run.base import _get_file
    except Exception as e:
        return e

    post_globus_q = Queue()
    post_extract_q = Queue()

    # sys.path.insert(1, '/home/tskluzac/xtract-matio')
    # import xtract_matio_main


    # A list of file paths
    all_files = event['inputs']
    transfer_token = event['transfer_token']
    dest_endpoint = event['dest_endpoint']
    source_endpoint = event['source_endpoint']
    https_bool = True  # TODO event['https_bool']


    dir_name = None
    if https_bool:
        dir_name = tempfile.mkdtemp()
        os.chdir(dir_name)

    # else: # OTHERWISE, if using Globus Transfer service.
    #     # Make a temp dir and download the data
    #     dir_name = f'/home/tskluzac/tmpfileholder-{random.randint(0,999999999)}'
    #     if os.path.exists(dir_name):
    #         shutil.rmtree(dir_name)
    #     os.makedirs(dir_name, exist_ok=True)
    #
    #     try:
    #         os.chdir(dir_name)
    #         os.chmod(dir_name, 0o777)
    #     except Exception as e:
    #         return str(e)

    # """ ADDING DEPENDENT GLOBUS SERVICE FUNCTION """
    # def globus_service_transfer(transfer_token, source_endpoint, dest_endpoint, files_dict):
    #     from globus_sdk import TransferClient, AccessTokenAuthorizer
    #     import globus_sdk
    #     import threading
    #
    #     i = 0
    #     try:
    #         authorizer = AccessTokenAuthorizer(transfer_token)
    #         tc = TransferClient(authorizer=authorizer)
    #         tdata = globus_sdk.TransferData(tc, source_endpoint,
    #                                         dest_endpoint,
    #                                         label=str(i))
    #         i += 1
    #     except Exception as e:
    #         return e
    #
    #     for file_id in files_dict:
    #         tdata.add_item(f'{files_dict[file_id]["file_path"]}', files_dict[file_id]["dest_file_path"],
    #                        recursive=False)
    #
    #     tc.endpoint_autoactivate(source_endpoint)
    #     tc.endpoint_autoactivate(dest_endpoint)
    #
    #     try:
    #         submit_result = tc.submit_transfer(tdata)
    #     except Exception as e:
    #         return str(e).split('\n')
    #
    #     gl_task_id = submit_result['task_id']
    #
    #     finished = tc.task_wait(gl_task_id, timeout=600)
    #
    #     def task_loop():
    #         while True:
    #             r = tc.get_task(submit_result['task_id'])
    #             if r['status'] == "SUCCEEDED":
    #                 break
    #             else:
    #                 time.sleep(0.5)
    #         return "SUCCESS1"
    #
    #     try:
    #         t1 = threading.Thread(target=task_loop, args=())
    #         t1.start()
    #     except Exception as e:
    #         return str(e)
    # """ /END OF GLOBUS SERVICE FUNCTION """

    t0 = time.time()
    mdata_list = []

    # return 0

    time.sleep(0.1)

    def get_file(file_path, headers, dir_name, file_id):
        try:
            req = requests.get(file_path, headers)
        except Exception as e:
            return e

        local_file_path = f"{dir_name}/{file_id}"
        # TODO: if response is 200...
        # TODO: Should really be dirname/groupid/fileid
        with open(local_file_path, 'wb') as f:
            f.write(req.content)

    file_dict = {}
    for item in all_files:

        item['headers']['Authorization'] = f"Bearer {item['headers']['Petrel']}"

        file_id = item['file_id']
        file_path = item["url"]

        time.sleep(0.1)

        timeout = 10
        # start_time = time.time()

        download_thr = threading.Thread(target=get_file, args=(file_path, item['headers'], dir_name, file_id))
        download_thr.start()
        download_thr.join(timeout=timeout)
        if download_thr.is_alive():
            return "The HTTPS download timed out!"


        # time.sleep(0.1)
        # req_file = requests.get(file_path, headers=item['headers'])

        # try:
        #     f = open(f"{dir_name}/{file_id}", 'rb')
        #     return f.read()
        # except Exception as e:
        #     return e

        #     file_path = item["url"].split('globus.org')[1]
        #     dest_file_path = dir_name + '/' + file_path.split('/')[-1]

        # time.sleep(0.1)
        file_dict[file_id] = {}
        file_dict[file_id]["file_path"] = file_path

        # if dir_name is not None:
        #     try:
        #         print("DERP")
        #         # return item
        #     except Exception as e:
        #         return e
        #     file_dict[file_id]["dest_file_path"] = '/'.join(dest_file_path.split('/')[0:-1])
        #     return dest_file_path
        # else:
        #     return "The dirname is None :( "

    # return 1

    # return os.listdir(dir_name)


     # return dest_file_path

    #     # Otherwise, we should use the proper Globus service.
    #     else:
    #         trans_status = globus_service_transfer(transfer_token, source_endpoint, dest_endpoint, file_dict)
    #
    #         if "SUCCESS" in trans_status:
    #             post_globus_q.put(trans_status)
    #
    #     t_init = time.time()
    #     while True:
    #         if not post_globus_q.empty():
    #             break
    #         if time.time() - t_init >= 120:
    #             return "Transfer OPERATION TIMED OUT AFTER 120 seconds"
    #         time.sleep(1)
    #

    # return "COMPLETED"

    # TODO: UNCOMMENT THIS BLOCK TO EXTRACT METADATA.
    # try:
    #     # new_mdata = xtract_matio_main.extract_matio(dir_name)
    #     new_mdata = extract_metadata(f"{dir_name}/{file_id}")
    #     post_extract_q.put(new_mdata)
    # except Exception as e:
    #     return str(e)
    #
    # # return new_mdata
    # t_ext_st = time.time()
    # while True:
    #     if not post_extract_q.empty():
    #         break
    #
    #     # TODO: Removing fake timeout.
    #     # if time.time() - t_ext_st >= 120:
    #     #     return "Extract TIMED OUT AFTER 20 seconds"
    #
    # new_mdata = post_extract_q.get()
    #
    # new_mdata['group_id'] = file_id
    # # TODO: Bring this back.
    # # new_mdata['trans_time'] = tb-ta
    # mdata_list.append(new_mdata)

    # Don't be an animal -- clean up your mess!
    shutil.rmtree(dir_name)
    t1 = time.time()
    gc.collect()
    return {'metadata': mdata_list, 'tot_time': t1-t0}


# TODO: Batch metadata saving requests (And store in different file).
def save_mdata(event):

    import json

    main_dir = event['dirname']
    crawl_id = str(event['crawl_id'])
    group_id = str(event['group_id'])
    mdata = event['metadata']

    with open(f"{main_dir}{crawl_id}/{group_id}", 'w') as f:
        json.dump(mdata, f)
    return None

def matio_test(event):
    return "Hello World!"