
from fair_research_login import NativeClient


def get_headers_on_client():
    client = NativeClient(client_id='7414f0b4-7d05-4bb6-bb00-076fa3f17cf5')
    tokens = client.login(
        requested_scopes=['https://auth.globus.org/scopes/56ceac29-e98a-440a-a594-b41e7a084b62/all',
                          'urn:globus:auth:scope:transfer.api.globus.org:all',
                          "https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all",
                          "urn:globus:auth:scope:data.materialsdatafacility.org:all",
                          'email', 'openid'],
        no_local_server=True,
        no_browser=True)

    auth_token = tokens["petrel_https_server"]['access_token']
    transfer_token = tokens['transfer.api.globus.org']['access_token']
    funcx_token = tokens['funcx_service']['access_token']

    # This one gives access to the open materials data facility at NCSA.
    mdf_token = tokens["data.materialsdatafacility.org"]['access_token']

    headers = {'Authorization': f"Bearer {mdf_token}", 'Transfer': transfer_token, 'FuncX': funcx_token,
               'Petrel': auth_token}

    return headers


def download_https(file_list, headers):
    """

        :return: None
    """

    from exceptions import PetrelRetrievalError, RemoteExceptionWrapper, HttpsDownloadTimeout
    import os
    import sys
    import time
    import requests
    import tempfile
    import threading


    # A list of file paths
    dir_name = None

    dir_name = tempfile.mkdtemp()
    os.chdir(dir_name)

    def get_file(file_path, headers, dir_name, sub_dir):
        # subdir can be any strring. I personally like having SOME sort of subdirectory for organizing files.
        try:
            # file_path is https://link
            req = requests.get(file_path, headers=headers)
        except Exception as e:
            try:
                raise PetrelRetrievalError(f"Caught the following error when downloading from Petrel: {e}")
            except PetrelRetrievalError:
                return RemoteExceptionWrapper(*sys.exc_info())

        os.makedirs(f"{dir_name}/{sub_dir}", exist_ok=True)
        local_file_path = f"{dir_name}/{sub_dir}/{file_path.split('/')[-1]}"

        print(f"Downlaoding file to {local_file_path}")

        # TODO: if response is 200...
        # TODO: IT IS POSSIBLE TO GET A CI LOGON HTML PAGE RETURNED HERE!!!
        print(req.content[:100])
        with open(local_file_path, 'wb') as f:
            f.write(req.content)
        return req.content

    thread_pool = []

    # Set this to how long we wait until giving up on the download.
    timeout = 30

    t_download_start = time.time()
    for file_payload in file_list:
        # Weird double-try to properly wrap exception.
        try:
            try:
                # This line looks stupid, but allows you to switch auth tokens at function, if needed...
                #   I'd leave this here for debugging.
                file_payload['headers']['Authorization'] = f"Bearer {file_payload['headers']['Authorization']}"
                file_id = file_payload['file_id']
                file_path = file_payload["url"]
            except Exception as e:
                raise PetrelRetrievalError(e)
        except PetrelRetrievalError as e:
            return {'exception': RemoteExceptionWrapper(*sys.exc_info()), 'groups': str(e)}

        download_thr = threading.Thread(target=get_file,
                                        args=(file_path,
                                              headers,
                                              dir_name,
                                              file_id))
        download_thr.start()
        thread_pool.append(download_thr)

    # Walk through the thread-pool until they've all completed.
    for thr in thread_pool:
        thr.join(timeout=timeout)

        if thr.is_alive():
            try:
                raise HttpsDownloadTimeout(f"Download failed with timeout: {timeout}")
            except HttpsDownloadTimeout:
                return {'exception': RemoteExceptionWrapper(*sys.exc_info())}
    t_download_finish = time.time()

    # Wait until all files downloaded! If any files fail, then will return ExceptionWrapper before this point.
    total_download_time = t_download_finish - t_download_start
    print(f"Total download time: {total_download_time}")


def download_from_client():
    """
    Download files using the Globus Transfer client. Note that this is less ideal
    for large numbers of small file transfers, as Globus has a rate limit of 5
    concurrent transfer jobs


    :return: None
    """
    print("Hi")


if __name__ == "__main__":

    headers = get_headers_on_client()
    # Here we'll just take 5 files with the same URL for fun.
    file_url = "https://data.materialsdatafacility.org/mdf_open/tsopanidis_wha_amaps_v1.1/WHA Dataset/Cropped Images/A_102_1.png"

    payload = {
        'url': file_url,
        'headers': headers, 'file_id': 0}

    file_list = [payload]*3

    download_https(file_list, headers)






