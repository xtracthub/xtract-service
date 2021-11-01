
def xtract_config_fetch(config_path: str, ep_name: str):
    """
    This function is to be used by funcX to fetch the config from
    a funcX endpoint.
    :param config_path : (str) : the path to the .xtract folder on local
    :param ep_name : (str): the name of the endpoint on Xtract.
    :returns config_dict : (dict) : the config as a dictionary object.
    """
    import json
    import os

    full_config_path = os.path.join(config_path, ep_name)

    # return full_config_path

    with open(os.path.join(full_config_path, 'config.json'), 'r') as f:
        return json.load(f)


config_func_info = {'function': xtract_config_fetch, 'function_uuid': 'abc'}
