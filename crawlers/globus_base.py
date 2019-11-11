
import sys
import json
import webbrowser
import uuid
import os
from utils.pg_utils import pg_conn

from datetime import datetime

from queue import Queue
from globus_sdk.exc import GlobusAPIError, TransferAPIError
from globus_sdk import (NativeAppAuthClient, TransferClient, RefreshTokenAuthorizer, AccessTokenAuthorizer)

# TODO: Stevedore to discover these??
from .groupers import matio_grouper


from .base import Crawler

# TODO: Tidy this.
grouper = matio_grouper.MatIOGrouper()


class GlobusCrawler(Crawler):

    def __init__(self, eid, path, crawl_id, trans_token, grouper=None):
        Crawler.__init__(self)
        self.path = path
        self.eid = eid
        self.cid = '079bdf4e-9666-4816-ac01-7eab9dc82b93'
        self.count = 0
        self.token_file = 'refresh-tokens.json'  # TODO: Probably don't need this either.
        self.transfer_token = trans_token
        self.redirect_uri = 'https://auth.globus.org/v2/web/auth-code'
        self.get_input = getattr(__builtins__, 'raw_input', input)
        self.grouper = grouper
        self.conn = pg_conn()
        self.crawl_id = crawl_id

    def load_tokens_from_file(self, token_filepath):
        """Load a set of saved tokens."""
        with open(token_filepath, 'r') as f:
            tokens = json.load(f)

        return tokens

    def save_tokens_to_file(self, token_filepath, tokens):
        """Save a set of tokens for later use."""
        with open(token_filepath, 'w') as f:
            json.dump(tokens, f)

    def update_tokens_file_on_refresh(self, token_response):
        """
        Callback function passed into the RefreshTokenAuthorizer.
        Will be invoked any time a new access token is fetched.
        """
        self.save_tokens_to_file(self.token_file, token_response.by_resource_server)

    def do_native_app_authentication(self, client_id, redirect_uri,
                                     requested_scopes=None):
        """
        Does a Native App authentication flow and returns a
        dict of tokens keyed by service name.
        """
        client = NativeAppAuthClient(client_id=client_id)
        # pass refresh_tokens=True to request refresh tokens
        client.oauth2_start_flow(requested_scopes=requested_scopes,
                                 redirect_uri=redirect_uri,
                                 refresh_tokens=True)

        url = client.oauth2_get_authorize_url()

        print('Native App Authorization URL: \n{}'.format(url))

        # if not is_remote_session():
        webbrowser.open(url, new=1)

        auth_code = self.get_input('Enter the auth code: ').strip()

        token_response = client.oauth2_exchange_code_for_tokens(auth_code)

        # return a set of tokens, organized by resource server name
        return token_response.by_resource_server

    def add_group_to_db(self, group_id, grouper, num_files):

        # TODO try/catch the postgres things.
        cur = self.conn.cursor()

        now_time = datetime.now()
        query1 = f"INSERT INTO groups (group_id, grouper, num_files, created_on, crawl_id) VALUES " \
            f"('{group_id}', '{grouper}', {num_files}, '{now_time}', '{self.crawl_id}');"

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

    def gen_group_id(self):
        return uuid.uuid4()

    def get_transfer(self):
        # tokens = None
        # try:
        #     raise Exception  # TODO: Remove this.
        #     # if we already have tokens, load and use them
        #     tokens = self.load_tokens_from_file(self.token_file)
        # except:
        #     pass
        #
        # if not tokens:
        #     # if we need to get tokens, start the Native App authentication process
        #     tokens = self.do_native_app_authentication(self.cid, self.redirect_uri, self.scopes)
        #
        #     try:
        #         self.save_tokens_to_file(self.token_file, tokens)
        #     except:
        #         pass

        # TODO: Just pass in the transfer token here.
        transfer_token = self.transfer_token

        # auth_client = NativeAppAuthClient(client_id=self.cid)

        # authorizer = RefreshTokenAuthorizer(
        #     tokens['refresh_token'],
        #     auth_client,
        #     access_token=tokens['access_token'],
        #     expires_at=tokens['expires_at_seconds'],
        #     on_refresh=self.update_tokens_file_on_refresh)

        authorizer = AccessTokenAuthorizer(transfer_token)

        transfer = TransferClient(authorizer=authorizer)

        # print out a directory listing from an endpoint
        try:
            transfer.endpoint_autoactivate(self.eid)
        except GlobusAPIError as ex:
            print(ex)
            if ex.http_status == 401:
                sys.exit('Refresh token has expired. '
                         'Please delete refresh-tokens.json and try again.')
            else:
                raise ex
        return transfer

    def crawl(self, transfer):

        dir_name = "~/xtract_metadata"
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)

        mdata_blob = {}
        failed_dirs = {"failed": []}

        to_crawl = Queue()
        to_crawl.put(self.path)

        cur = self.conn.cursor()

        now_time = datetime.now()
        crawl_update = f"INSERT INTO crawls (crawl_id, started_on) VALUES " \
            f"('{self.crawl_id}', '{now_time}');"

        cur.execute(crawl_update)
        self.conn.commit()

        while not to_crawl.empty():

            cur_dir = to_crawl.get()

            try:

                dir_contents = transfer.operation_ls(self.eid, path=cur_dir)

                f_names = []
                for entry in dir_contents:

                    full_path = cur_dir + "/" + entry['name']
                    if entry['type'] == 'file':
                        f_names.append(full_path)
                        extension = self.get_extension(entry["name"])
                        mdata_blob[full_path] = {"physical": {'size': entry['size'],
                                                              "extension": extension, "path_type": "globus"}}
                        self.count += 1
                        if self.count % 20000 == 0:
                            print("COUNT: {}".format(self.count))

                    elif entry['type'] == 'dir':
                        full_path = cur_dir + "/" + entry['name']
                        to_crawl.put(full_path)

                if self.grouper == 'matio':
                    group_list = grouper.group(f_names)

                    for gr in group_list:
                        group_info = {"group_id": self.gen_group_id(), "files": [], "mdata": []}

                        # TODO: It's like the groups are triple nested (require 3 for-loops)... ask Logan about this.
                        for sub_gr in gr:

                            if type(sub_gr) == str:  # str is length 1, then single-file group.
                                # Group is reset here, because single-file group.
                                group_info = {"group_id": str(self.gen_group_id()), "files": [], "mdata": []}

                                group_info["files"].append(sub_gr)
                                group_info["mdata"].append({"file": sub_gr, "blob": mdata_blob[sub_gr]})

                                with open("~/xtract_metadata/" + str(group_info['group_id'])+".mdata", 'w') as f:
                                    json.dump(group_info, f)

                                self.add_group_to_db(str(group_info["group_id"]), self.grouper,
                                                     len(group_info['files']))

                            else:
                                for filename in sub_gr:
                                    group_info["files"].append(filename)
                                    group_info["mdata"].append({"file": filename, "blob": mdata_blob[filename]})

                                    with open("~/xtract_metadata/" + str(group_info['group_id'])+".mdata", 'w') as f:
                                        json.dump(group_info, f)

            except TransferAPIError as e:
                print("Problem directory {}".format(cur_dir))
                print("Transfer client received the following error:")
                print(e)
                failed_dirs["failed"].append(cur_dir)
                continue

        print("FILES PROCESSED: {}".format(self.count))

        # TODO: HERE update DB to show that crawling is completed.

        with open('failed.json', 'w') as fp:
            json.dump(failed_dirs, fp)

        return mdata_blob
