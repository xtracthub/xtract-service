
import globus_sdk
import os

client_id = "83cd643f-8fef-4d4b-8bcf-7d146c288d81"

data_source = "e38ee745-6d04-11e5-ba46-22000b92c6ec"
data_dest = "af7bda53-6d04-11e5-ba46-22000b92c6ec"
data_path = "/project2/chard/skluzacek/data-to-process/"

max_gb = 50

block_size = max_gb/5

bytes_in_kb = 1024
bytes_in_mb = bytes_in_kb * 1024
bytes_in_gb = bytes_in_mb * 1024

total_bytes = bytes_in_gb * max_gb


def get_size(start_path = '.'):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            # skip if it is symbolic link
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)

    return total_size



client = globus_sdk.NativeAppAuthClient(client_id)
client.oauth2_start_flow(refresh_tokens=True)

print('Please go to this URL and login: {0}'
      .format(client.oauth2_get_authorize_url()))

authorize_url = client.oauth2_get_authorize_url()
print('Please go to this URL and login:\n {0}'.format(authorize_url))

# this is to work on Python2 and Python3 -- you can just use raw_input() or
# input() for your specific version
get_input = getattr(__builtins__, 'raw_input', input)
auth_code = get_input(
    'Please enter the code you get after login here: ').strip()
token_response = client.oauth2_exchange_code_for_tokens(auth_code)

globus_auth_data = token_response.by_resource_server['auth.globus.org']
globus_transfer_data = token_response.by_resource_server['transfer.api.globus.org']

# most specifically, you want these tokens as strings
AUTH_TOKEN = globus_auth_data['access_token']
TRANSFER_TOKEN = globus_transfer_data['access_token']


# a GlobusAuthorizer is an auxiliary object we use to wrap the token. In
# more advanced scenarios, other types of GlobusAuthorizers give us
# expressive power
authorizer = globus_sdk.AccessTokenAuthorizer(TRANSFER_TOKEN)
tc = globus_sdk.TransferClient(authorizer=authorizer)

family_map = dict()

# TODO 0: Looping check to see if size of directory is less than max_gb
while True: 
    t0 = time.time()
    num_bytes = get_size()
    t1 = time.time() 
# TODO 1: Pull down messages from crawl_queue to local. 
# TODO 2: Start by just pulling down 100 at a time. 
# TODO 3: 


# high level interface; provides iterators for list responses
tdata = globus_sdk.TransferData(tc, data_source, data_dest, label="Xtract attempt", sync_level="checksum")
tdata.add_item('/3M/MESH_40/3M_Sim.error', '/home/skluzacek/3M_Sim.error')
transfer_result = tc.submit_transfer(tdata)

print("task_id =", transfer_result["task_id"])
