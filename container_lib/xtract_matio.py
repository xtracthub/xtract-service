
import threading
import requests
import logging
import psycopg2
import pickle
import time
import json

from funcx.serialize import FuncXSerializer
from queue import Queue

from exceptions import XtractError, HttpsDownloadTimeout, PetrelRetrievalError, ExtractorError


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
        self.func_id = "4ceec45c-fbc8-4584-9687-4f5fd8162403"
        self.source_endpoint = source_eid
        self.dest_endpoint = dest_eid

        self.task_dict = {"active": Queue(), "pending": Queue(), "failed": Queue()}

        self.headers = headers
        self.batch_size = 1
        self.max_active_batches = 25
        self.crawl_id = crawl_id

        self.mdata_store_path = mdata_store_path
        self.suppl_store = suppl_store

        self.conn = pg_conn()
        self.fx_headers = {"Authorization": f"Bearer {self.headers['FuncX']}", 'FuncX': self.headers['FuncX']}

        if 'Petrel' in self.headers:
            self.fx_headers['Petrel'] = self.headers['Petrel']

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


    # TODO: Things about to get wild here.
    # TODO: First find a family that is yet to be processed.
    def get_next_groups(self):
        try:
            # TODO: Step 1. Get family where staus = 'INIT'.
            fam_query = f"SELECT family FROM families WHERE "

            query = f"SELECT group_id FROM groups WHERE status='crawled' " \
                f"and crawl_id='{self.crawl_id}'" #" LIMIT {self.batch_size};"  # TODO: Still need something w/ batch size so we don't get everytthing.
            cur = self.conn.cursor()
            cur.execute(query)
            gids = cur.fetchall()
            return gids

        except psycopg2.OperationalError:
            self.logger.error("[SEND] Unable to connect to Postgres database...")

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
            print("[SEND] Unable to fetch new group_ids from DB")

    def send_files(self):
        # Just keep looping in case something comes up in crawl.
        while True:
            data = {'inputs': []}
            gids = self.get_next_groups()

            for gid in gids:
                self.logger.debug(f"Processing GID: {gid[0]}")

                # Update DB to flag group_id as 'EXTRACTING'.
                cur = self.conn.cursor()
                try:
                    update_q = f"UPDATE groups SET status='EXTRACTING' WHERE group_id='{gid[0]}';"
                    cur.execute(update_q)
                    self.conn.commit()
                except Exception as e:
                    print("[SEND] Unable to update status to 'EXTRACTING'.")
                    print(e)

                # Get the metadata for each group_id
                try:
                    get_mdata = f"SELECT metadata FROM group_metadata_2 where group_id='{gid[0]}' LIMIT 1;"
                    cur.execute(get_mdata)
                    old_mdata = pickle.loads(bytes(cur.fetchone()[0]))

                    parser = old_mdata['parser']
                    print(f"[DEBUG] :: OLD METADATA: {old_mdata}")
                except psycopg2.OperationalError as e:
                    print("[Xtract] Unable to retrieve metadata from Postgres. Error: {e}")
                    print(e)
                    continue  # TODO: If we hit this point, should likely update file to 'FAILED' or something...

                # TODO: Extend this to be more than just 1 parser.
                group = {'group_id': gid[0], 'files': [], 'parsers': [parser]}

                # Take all of the files and append them to our payload.
                for f_obj in old_mdata["files"]:
                    print(self.fx_headers)
                    payload = {
                        'url': f'https://{self.source_endpoint}.e.globus.org{f_obj}',
                        'headers': self.fx_headers, 'file_id': gid[0]}
                    group["files"].append(payload)
                    data["transfer_token"] = self.headers['Transfer']

                    data["source_endpoint"] = self.source_endpoint
                    data["dest_endpoint"] = self.dest_endpoint
                data["inputs"].append(group)

                res = requests.post(url=self.post_url,
                                    headers=self.fx_headers,
                                    json={
                                        'endpoint': self.funcx_eid,
                                          'func': self.func_id,
                                          'payload': serialize_fx_inputs(
                                              event=data)}
                                    )
                try:
                    fx_res = json.loads(res.content)
                except json.decoder.JSONDecodeError as e:
                    # TODO: MARK AS FAILED.
                    print("MARK AS FAILED.")
                    continue

                # TODO: Actually do something here.
                if fx_res["status"] == "failed":
                    self.logger.error("TODO: DO SOMETHING IF BAD FUNCX ENDPOINT GIVEN. ")

                task_uuid = fx_res['task_uuid']

                self.logger.info(f"Placing Task ID {task_uuid} onto live queue")
                self.task_dict["active"].put(task_uuid)
                self.logger.info(f"Approximate current size of live task queue: {self.task_dict['active'].qsize()}")

            while True:
                if self.task_dict["active"].qsize() < self.max_active_batches:
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
        failed_returns = 0
        while True:
            if self.task_dict["active"].empty():
                self.logger.debug("No live IDs... sleeping...")
                time.sleep(1)
                continue

            num_elem = self.task_dict["active"].qsize()
            for _ in range(0, num_elem):
                ex_id = self.task_dict["active"].get()
                status_thing = requests.get(self.get_url.format(ex_id), headers=self.fx_headers)

                try:
                    status_thing = json.loads(status_thing.content)
                except json.decoder.JSONDecodeError as e:
                    self.logger.error(e)
                    self.logger.error("Status unloadable. Marking as failed.")
                    continue
                    # TODO. What exactly SHOULD I do?

                self.logger.debug(f"Status: {status_thing}")

                print("result" in status_thing)
                if "result" in status_thing:
                    res = fx_ser.deserialize(status_thing['result'])
                    # print(f"Result: {res}")
                    success_returns += 1
                    self.logger.debug(f"Success Counter: {success_returns}")
                    self.logger.debug(f"Failure Counter: {failed_returns}")
                    self.logger.debug(f"Received response: {res}")

                    cur = None
                    # TODO: Still need to return group_ids so we can mark accordingly...
                    if "metadata" not in res:
                        if "exception" not in res:
                            raise XtractError("[POLL] Received undefined empty from funcX. Trapping. Marking as failed")
                        else:
                            failed_returns += 1
                            try:
                                res['exception'].reraise()
                            except HttpsDownloadTimeout as e1:
                                self.logger.error(e1)
                                self.logger.error("May need longer timeout or lower network load. "
                                                  "Marking as FAILED for later retry.")

                                cur = self.conn.cursor()
                                for group in res['groups']:
                                    gid = group["group_id"]
                                    update_q = f"UPDATE groups SET status='FAILED' WHERE group_id='{gid}';"
                                    cur.execute(update_q)
                                self.conn.commit()

                            except PetrelRetrievalError as e2:
                                self.logger.error(e2)
                                # TODO: Should have max_retries that are tracked here.
                                self.logger.error("Petrel Retrieval Error. "
                                                  "Re-queuing immediately...")
                                self.task_dict["active"].put(ex_id)
                            except ExtractorError as e3:
                                self.logger.error(e3)
                                self.logger.error("Not a retry class! "
                                                  "Marking as failed and deleting from work queue!")
                                cur = self.conn.cursor()
                                for group in res['groups']:
                                    gid = group["group_id"]
                                    update_q = f"UPDATE groups SET status='FAILED' WHERE group_id='{gid}';"
                                    cur.execute(update_q)
                                self.conn.commit()
                        continue

                            # TODO: Should mark as failed in db.
                    for g_obj in res["metadata"]:
                        for parser in res["metadata"][g_obj]:
                            gid = g_obj
                            print(f"gid: {g_obj}")
                            cur = self.conn.cursor()
                            # TODO: [Optimize] Do something smarter than pulling this down a second time (cache???)
                            get_mdata = f"SELECT metadata FROM group_metadata_2 where group_id='{gid}';"
                            cur.execute(get_mdata)
                            old_mdata = pickle.loads(bytes(cur.fetchone()[0]))
                            old_mdata["matio"] = res["metadata"][g_obj][parser]["matio"]

                        self.logger.debug("Pushing freshly-retrieved metadata to DB (Update: 1/2)")
                        update_mdata = f"UPDATE group_metadata_2 " \
                            f"SET metadata={psycopg2.Binary(pickle.dumps(old_mdata))} where group_id='{gid}';"
                        cur.execute(update_mdata)

                        # TODO: Make sure we're updating the parser-list in the db.
                        self.logger.debug("Updating group status (Update: 2/2)")
                        update_q = f"UPDATE groups SET status='EXTRACTED' WHERE group_id='{gid}';"
                        cur.execute(update_q)
                        self.conn.commit()
                        break
                else:
                    self.task_dict["active"].put(ex_id)

            # TODO: Get rid of sleep soon.
            print("sleeping...")
            time.sleep(2)


# TODO: .put() data for HTTPS on Petrel.
# TODO: Move these functions to outside this file (like a dir of functions.

def matio_test(event):

    """
    Function
    :param event (dict) -- contains auth header and list of HTTP links to extractable files:
    :return metadata (dict) -- metadata as gotten from the materials_io library:
    """

    import os
    import sys
    import time
    import shutil
    import requests
    import tempfile
    import threading

    t0 = time.time()

    try:
        sys.path.insert(1, '/')
        import xtract_matio_main
        from exceptions import RemoteExceptionWrapper, HttpsDownloadTimeout, ExtractorError, PetrelRetrievalError

    except Exception as e:
        return e

    # A list of file paths
    all_families = event['inputs']
    https_bool = True
    dir_name = None

    if https_bool:
        dir_name = tempfile.mkdtemp()
        os.chdir(dir_name)

    def get_file(file_path, headers, dir_name, file_id, family_id):
        try:
            req = requests.get(file_path, headers)
        except Exception as e:
            try:
                raise PetrelRetrievalError(f"Caught the following error when downloading from Petrel: {e}")
            except PetrelRetrievalError:
                return RemoteExceptionWrapper(*sys.exc_info())

        os.makedirs(f"{dir_name}/{family_id}", exist_ok=True)
        local_file_path = f"{dir_name}/{family_id}/{file_path.split('/')[-1]}"

        # TODO: if response is 200...
        # TODO: IT IS POSSIBLE TO GET A CI LOGON HTML PAGE RETURNED HERE!!!
        with open(local_file_path, 'wb') as f:
            f.write(req.content)

    thread_pool = []
    timeout = 30

    if len(all_families) == 0:
        return "Received no file-group families. Returning!"

    t_download_start = time.time()
    for family in all_families:
        for file_url in family['files']:
            file_payload = family['files'][file_url]

            try:
                try:
                    file_payload['headers']['Authorization'] = f"Bearer {file_payload['headers']['Petrel']}"
                    file_id = file_payload['file_id']
                    file_path = file_payload["url"]
                except Exception:
                    raise PetrelRetrievalError("Unable to establish connection with Petrel.")
            except PetrelRetrievalError:
                return {'exception': RemoteExceptionWrapper(*sys.exc_info()), 'groups': 'todo-- add this back'}

            download_thr = threading.Thread(target=get_file,
                                            args=(file_path,
                                                  file_payload['headers'],
                                                  dir_name,
                                                  file_id,
                                                  family['family_id']))  # TODO: relax for families.
            download_thr.start()
            thread_pool.append(download_thr)

    # Walk through the thread-pool until they've all completed.
    for thr in thread_pool:
        thr.join(timeout=timeout)

        if thr.is_alive():
            try:
                raise HttpsDownloadTimeout(f"Download failed with timeout: {timeout}")
            except HttpsDownloadTimeout:
                return {'exception': RemoteExceptionWrapper(*sys.exc_info()), 'groups': 'todo-- add this back'}
    t_download_finish = time.time()

    total_download_time = t_download_finish - t_download_start

    mat_mdata = None
    family_metadata = {}
    for family in all_families:

        all_groups = family['groups']

        # TODO: In the future, we can parallelize parser execution?
        mat_mdata = {}
        for group in all_groups:
            parser = all_groups[group]['parser']
            native_file_collection = all_groups[group]['files']

            local_file_collection = []
            for file_item in native_file_collection:
                local_file_collection.append(dir_name + '/' + family['family_id'] + '/' + file_item.split('/')[-1])

            all_items = []
            try:
                for item in local_file_collection:
                    all_items.append(item)
            except Exception as e:
                return e

            mat_mdata[group] = {}
            try:
                new_mdata = xtract_matio_main.extract_matio(all_items, parser)
                mat_mdata[group][parser] = new_mdata

            except Exception as e:
                try:
                    raise XtractError(f"Caught the following error trying to extract metadata: {e}")
                except XtractError:
                    return {'exception': RemoteExceptionWrapper(*sys.exc_info())}
                    # TODO: I think we want more returned here (filenames?).

        family_metadata[family["family_id"]] = {'trans_time': total_download_time, "metadata": mat_mdata}

    # Don't be an animal -- clean up your mess!
    shutil.rmtree(dir_name)
    t1 = time.time()
    return {'metadata': family_metadata, 'tot_time': t1-t0}


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


def hello_world(event):
    import time
    import os
    time.sleep(5)
    return f"Container Version: {os.environ['container_version']}"
