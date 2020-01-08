
import os
import sys
import json
import uuid
import time
import logging


from datetime import datetime
from utils.pg_utils import pg_conn, pg_list


from queue import Queue
from globus_sdk.exc import GlobusAPIError, TransferAPIError
from globus_sdk import (TransferClient, AccessTokenAuthorizer, ConfidentialAppAuthClient)


from .groupers import matio_grouper

from .base import Crawler


class GlobusCrawler(Crawler):

    def __init__(self, eid, path, crawl_id, trans_token, auth_token, grouper_name=None, logging_level='info'):
        Crawler.__init__(self)
        self.path = path
        self.eid = eid
        self.group_count = 0
        self.transfer_token = trans_token
        self.auth_token = auth_token
        self.conn = pg_conn()
        self.crawl_id = crawl_id
        self.crawl_hb = 10

        if grouper_name == 'matio':
            self.grouper = matio_grouper.MatIOGrouper()

        try:
            self.token_owner = self.get_uid_from_token()
        except:  # TODO: Real auth that's not just printing.
            logging.info("Unable to authenticate user: Invalid Token. Aborting crawl.")

        self.logging_level = logging_level

        if self.logging_level == 'debug':
            logging.basicConfig(format='%(asctime)s - %(message)s', filename='crawler.log', level=logging.DEBUG)
        elif self.logging_level == 'info':
            logging.basicConfig(format='%(asctime)s - %(message)s', filename='crawler.log', level=logging.INFO)
        else:
            raise KeyError("Only logging levels '-d / debug' and '-i / info' are supported.")

    def add_group_to_db(self, group_id, num_files):

        # TODO try/catch the postgres things.
        cur = self.conn.cursor()

        now_time = datetime.now()
        query1 = f"INSERT INTO groups (group_id, grouper, num_files, created_on, crawl_id) VALUES " \
            f"('{group_id}', '{self.grouper.name}', {num_files}, '{now_time}', '{self.crawl_id}');"

        query2 = f"INSERT INTO group_status (group_id, status) VALUES ('{group_id}', 'crawled');"

        cur.execute(query1)
        cur.execute(query2)

        # TODO: Don't need to commit every dang single time.
        return self.conn.commit()

    def db_crawl_end(self):
        cur = self.conn.cursor()
        query = f"UPDATE crawls SET status='complete' WHERE crawl_id='{self.crawl_id}';"
        cur.execute(query)

        return self.conn.commit()

    def get_extension(self, filepath):
        """Returns the extension of a filepath.
        Parameter:
        filepath (str): Filepath to get extension of.
        Return:
        extension (str): Extension of filepath.
        """
        filename = filepath.split('/')[-1]
        extension = None

        if '.' in filename:
            extension = filename.split('.')[-1]
        return extension

    def get_uid_from_token(self):
        # Step 1: Get Auth Client with Secrets.
        client_id = os.getenv("GLOBUS_FUNCX_CLIENT")
        secret = os.getenv("GLOBUS_FUNCX_SECRET")

        # Step 2: Transform token and introspect it.
        conf_app_client = ConfidentialAppAuthClient(client_id, secret)
        token = str.replace(str(self.auth_token), 'Bearer ', '')
        auth_detail = conf_app_client.oauth2_token_introspect(token)

        uid = auth_detail['username']

        return uid

    def gen_group_id(self):
        return uuid.uuid4()

    def get_transfer(self):
        transfer_token = self.transfer_token
        authorizer = AccessTokenAuthorizer(transfer_token)
        transfer = TransferClient(authorizer=authorizer)

        # Print out a directory listing from an endpoint
        try:
            transfer.endpoint_autoactivate(self.eid)
        except GlobusAPIError as ex:
            logging.error(ex)
            if ex.http_status == 401:
                sys.exit('Refresh token has expired. '
                         'Please delete refresh-tokens.json and try again.')
            else:
                raise ex
        return transfer

    def crawl(self, transfer):
        dir_name = "./xtract_metadata"
        os.makedirs(dir_name, exist_ok=True)

        mdata_blob = {}
        failed_dirs = {"failed": []}
        failed_groups = {"illegal_char": []}

        to_crawl = Queue()
        to_crawl.put(self.path)

        cur = self.conn.cursor()
        now_time = datetime.now()
        crawl_update = f"INSERT INTO crawls (crawl_id, started_on) VALUES " \
            f"('{self.crawl_id}', '{now_time}');"
        cur.execute(crawl_update)
        self.conn.commit()

        t_last = time.time()
        while not to_crawl.empty():

            cur_dir = to_crawl.get()

            try:
                while True:

                    try:
                        dir_contents = transfer.operation_ls(self.eid, path=cur_dir)
                        break
                    # TODO: Be less broad here.
                    except:
                        logging.error("Retrying!")
                        time.sleep(0.5)

                f_names = []
                for entry in dir_contents:

                    full_path = cur_dir + "/" + entry['name']
                    if entry['type'] == 'file':
                        f_names.append(full_path)
                        extension = self.get_extension(entry["name"])
                        mdata_blob[full_path] = {"physical": {'size': entry['size'],
                                                              "extension": extension, "path_type": "globus"}}

                    elif entry['type'] == 'dir':
                        full_path = cur_dir + "/" + entry['name']
                        to_crawl.put(full_path)

                gr_dict = self.grouper.group(f_names)

                for parser in gr_dict:

                    for gr in gr_dict[parser]:
                        logging.debug(f"Group: {gr}")
                        logging.debug(f"Parser: {parser}")

                        gr_id = str(self.gen_group_id())
                        group_info = {"group_id": gr_id, "parser": parser, "files": [], "mdata": []}

                        file_list = list(gr)

                        group_info["files"] = file_list

                        for f in file_list:
                            group_info["mdata"].append({"file": f, "blob": mdata_blob[f]})

                        logging.debug(group_info)

                        from psycopg2.extras import Json

                        cur = self.conn.cursor()

                        try:
                            files = pg_list(group_info["files"])
                            parsers = pg_list(['crawler'])

                        except ValueError as e:
                            logging.error(f"Caught ValueError {e}")
                            failed_groups["illegal_char"].append((group_info["files"], ['crawler']))
                            logging.error("Continuing!")

                        else:
                            query = f"INSERT INTO group_metadata (group_id, metadata, files, parsers, owner) " \
                                f"VALUES ('{gr_id}', {Json(group_info)}, '{files}', '{parsers}', '{self.token_owner}')"
                            self.group_count += 1
                            cur.execute(query)
                            self.conn.commit()

                            self.add_group_to_db(str(group_info["group_id"]), len(group_info['files']))

                        t_new = time.time()

                        if t_new - t_last >= self.crawl_hb:
                            logging.info(f"Total groups processed for crawl_id {self.crawl_id}: {self.group_count}")
                            t_last = t_new

            except TransferAPIError as e:
                logging.error("Problem directory {}".format(cur_dir))
                logging.error("Transfer client received the following error:")
                logging.error(e)
                failed_dirs["failed"].append(cur_dir)
                continue

        logging.info(f"\n***FINAL groups processed for crawl_id {self.crawl_id}: {self.group_count}***")
        logging.info(f"\n*** CRAWL COMPLETE  (ID: {self.crawl_id})***")

        self.db_crawl_end()

        with open('failed_dirs.json', 'w') as fp:
            json.dump(failed_dirs, fp)

        with open('failed_groups.json', 'w') as gp:
            json.dump(failed_groups, gp)

        return mdata_blob
