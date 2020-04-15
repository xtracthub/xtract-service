
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
        self.func_id = "d3a94815-f92d-4998-9bf9-b3c21d4ab014"
        self.source_endpoint = source_eid
        self.dest_endpoint = dest_eid

        self.task_dict = {"active": Queue(), "pending": Queue(), "failed": Queue()}

        self.headers = headers
        self.batch_size = 1
        self.max_active_batches = 25
        self.families_limit = 1  # TODO: right now this HAS to be 1 or it'll break.
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


    # TODO: Smarter batching that takes into account file size.
    def get_next_family(self):
        while True:
            try:
                # Step 1. Get n families where status = 'INIT'.
                cur1 = self.conn.cursor()
                fam_get_query = f"SELECT family_id FROM families WHERE status='INIT' and crawl_id='{self.crawl_id}' " \
                    f"LIMIT {self.families_limit};"

                t_families_get_start = time.time()
                cur1.execute(fam_get_query)
                fid = cur1.fetchall()[0][0]
                t_families_get_end = time.time()
                print(f"Time to get families: {t_families_get_end - t_families_get_start}")

                # Step 2. For each family, update the family's status to 'PROCESSING'. THEN commit.
                t_update_families_start = time.time()
                cur2 = self.conn.cursor()
                fam_update_query = f"UPDATE families SET status='PROCESSING' where family_id='{fid}';"
                cur2.execute(fam_update_query)
                self.conn.commit()
                t_update_families_end = time.time()
                print(f"Time to get families: {t_update_families_end - t_update_families_start}")

                t_get_groups_start = time.time()
                # Step 3. Get groups corresponding to a given family ID.
                get_groups_query = f"SELECT group_id FROM group_metadata_2 WHERE family_id='{fid}';"
                cur3 = self.conn.cursor()
                cur3.execute(get_groups_query)
                gids = cur3.fetchall()
                t_get_groups_end = time.time()

                print(f"Time to get groups: {t_get_groups_end - t_get_groups_start}")

                return fid, gids

            except psycopg2.OperationalError:
                self.logger.error("[SEND] Unable to connect to Postgres database...")
            except IndexError:
                print("[SEND FILES] No new families found from crawl! Sleeping and retrying!")
                time.sleep(2)
                pass

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
            data = {'inputs': [], "transfer_token": self.headers["Transfer"], "source_endpoint": 'e38ee745-6d04-11e5-ba46-22000b92c6ec', "dest_ep": "1adf6602-3e50-11ea-b965-0e16720bb42f"}

            # TODO: should be, like, get families (and those families can have groups in them)
            # TODO: SHOULD package the exact same way as the family-test example.

            fid, gids = self.get_next_family()

            print(f"Family ID: {fid}")
            print(f"Group IDs: {gids}")

            # TODO: This doesn't need to be a loop if we just batch update the groups based on family_id :)
            fam_dict = {"family_id": fid, "files": {}, "groups": {}}

            # TODO: Make this faster (fewer DB calls)
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

                group = {'group_id': gid[0], 'files': [], 'parser': parser}

                # Take all of the files and append them to our payload.
                for f_obj in old_mdata["files"]:
                    id_count = 0
                    print(self.fx_headers)

                    # Keep matching here to families.
                    file_url = f'https://{self.source_endpoint}.e.globus.org{f_obj}'
                    print(file_url)
                    payload = {
                        'url': file_url,
                        'headers': self.fx_headers,
                        'file_id': id_count}
                    id_count += 1

                    if file_url not in fam_dict['files']:
                        fam_dict['files'][file_url] = payload

                    group["files"].append(file_url)
                    data["transfer_token"] = self.headers['Transfer']

                    # TODO: I should need these lines when not HTTPS!
                    # data["source_endpoint"] = self.source_endpoint
                    # data["dest_endpoint"] = self.dest_endpoint
                fam_dict["groups"][gid[0]] = group
                data["inputs"].append(fam_dict)

            print(f"Family: {fam_dict}")
            # print("MADE IT HERE!!!!")

            # print(self.fx_headers)
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
                print(e)
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
                # time.sleep(1)
                time.sleep(0.25)
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

                if "result" in status_thing:
                    res = fx_ser.deserialize(status_thing['result'])
                    # print(f"Result: {res}")
                    success_returns += 1
                    self.logger.debug(f"Success Counter: {success_returns}")
                    self.logger.debug(f"Failure Counter: {failed_returns}")
                    self.logger.debug(f"Received response: {res}")

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

                    for fam_id in res["metadata"]:

                        group_coll = res["metadata"][fam_id]["metadata"]
                        trans_time = res["metadata"][fam_id]["metadata"]

                        for gid in group_coll:


                            print(f"gid: {gid}")
                            cur = self.conn.cursor()
                            # TODO: [Optimize] Do something smarter than pulling this down a second time (cache???)
                            get_mdata = f"SELECT metadata FROM group_metadata_2 where group_id='{gid}';"
                            cur.execute(get_mdata)
                            old_mdata = pickle.loads(bytes(cur.fetchone()[0]))

                            print(old_mdata)

                            print("We made it here, and that's progress! ")
                            print(group_coll[gid])

                            # TODO: Should we catch the metadata that 'successfully' returned nothing here?
                            # TODO: Answer -- yes!
                            parser = list(group_coll[gid].keys())[0]  # TODO: This is weird. There's only 1!!
                            old_mdata["matio"] = group_coll[gid][parser]["matio"]

                            self.logger.debug("Pushing freshly-retrieved metadata to DB (Update: 1/2)")
                            update_mdata = f"UPDATE group_metadata_2 " \
                                f"SET metadata={psycopg2.Binary(pickle.dumps(old_mdata))} where group_id='{gid}';"
                            cur.execute(update_mdata)

                            self.logger.debug("Updating group status (Update: 2/2)")
                            update_q = f"UPDATE groups SET status='EXTRACTED' WHERE group_id='{gid}';"
                            cur.execute(update_q)
                            self.conn.commit()
                            #break
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
                except Exception as e:
                    raise PetrelRetrievalError(e)
            except PetrelRetrievalError as e:
                return {'exception': RemoteExceptionWrapper(*sys.exc_info()), 'groups': str(e)}

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
