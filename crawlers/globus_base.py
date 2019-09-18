
import sys
import json
import webbrowser

from queue import Queue
from globus_sdk.exc import GlobusAPIError, TransferAPIError
from globus_sdk import (NativeAppAuthClient, TransferClient, RefreshTokenAuthorizer)


from base import Crawler


class GlobusCrawler(Crawler):

    def __init__(self, eid, path):
        Crawler.__init__(self)
        self.path = path
        self.eid = eid
        self.cid = '079bdf4e-9666-4816-ac01-7eab9dc82b93'
        self.count = 0
        self.token_file = 'refresh-tokens.json'
        self.redirect_uri = 'https://auth.globus.org/v2/web/auth-code'
        self.get_input = getattr(__builtins__, 'raw_input', input)
        self.scopes = ('openid email profile '
                  'urn:globus:auth:scope:transfer.api.globus.org:all')

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

    def get_transfer(self):
        tokens = None
        try:
            # if we already have tokens, load and use them
            tokens = self.load_tokens_from_file(self.token_file)
        except:
            pass

        if not tokens:
            # if we need to get tokens, start the Native App authentication process
            tokens = self.do_native_app_authentication(self.cid, self.redirect_uri, self.scopes)

            try:
                self.save_tokens_to_file(self.token_file, tokens)
            except:
                pass

        transfer_tokens = tokens['transfer.api.globus.org']

        auth_client = NativeAppAuthClient(client_id=self.cid)

        authorizer = RefreshTokenAuthorizer(
            transfer_tokens['refresh_token'],
            auth_client,
            access_token=transfer_tokens['access_token'],
            expires_at=transfer_tokens['expires_at_seconds'],
            on_refresh=self.update_tokens_file_on_refresh)

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

        # TODO: Have separate db update thread.
        mdata_blob = {}
        failed_dirs = {"failed": []}

        to_crawl = Queue()
        to_crawl.put(self.path)

        while not to_crawl.empty():

            cur_dir = to_crawl.get()

            try:
                for entry in transfer.operation_ls(self.eid, path=cur_dir):

                    full_path = cur_dir + "/" + entry['name']
                    print(full_path)
                    if entry['type'] == 'file':
                        extension = self.get_extension(entry["name"])
                        mdata_blob[full_path] = {"physical": {'size': entry['size'],
                                                              "extension": extension, "path_type": "globus"}}
                        self.count += 1
                        if self.count % 20000 == 0:
                            print("COUNT: {}".format(self.count))

                    elif entry['type'] == 'dir':
                        full_path = cur_dir + "/" + entry['name']
                        to_crawl.put(full_path)

            except TransferAPIError as e:
                print("Problem directory {}".format(cur_dir))
                print("Transfer client received the following error:")
                print(e)
                failed_dirs["failed"].append(cur_dir)
                continue

        print("FILES PROCESSED: {}".format(self.count))

        with open('result.json', 'w') as fp:
            json.dump(mdata_blob, fp)

        with open('failed.json', 'w') as fp:
            json.dump(mdata_blob, fp)

        return mdata_blob


# if __name__ == "__main__":
crawler = GlobusCrawler('1c115272-a3f2-11e9-b594-0e56e8fd6d5a', '/Users/tylerskluzacek/Desktop')

tc = crawler.get_transfer()

crawler.crawl(tc)
