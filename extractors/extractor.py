
from abc import ABCMeta
from queue import Queue
from utils.fx_utils import invoke_solo_function
from funcx import FuncXClient


class Extractor(metaclass=ABCMeta):

    def __init__(self, extr_id, extr_name, func_id, store_type, store_url):
        self.extr_id = extr_id
        self.func_id = func_id
        self.extr_name = extr_name
        self.store_type = store_type
        self.store_url = store_url
        self.extr_func = None
        self.debug = False  # TODO: if debug=True, reregister function each time.
        self.active_funcx_ids = Queue()
        # self.fxc = FuncXClient()
        # self.fxc.throttling_enabled = False  # We want to shut off throttling.

    def set_extr_func(self, func):
        self.extr_func = func

    def local_extract(self, event):
        """ Function to run the aforementioned function locally.

            NOTE: Will not work unless the aforementioned function can work
            outside of the container, and this env has the same reqs installed
        """
        metadata = self.extr_func(event)
        return metadata

    def remote_extract_solo(self, event, fx_eid, headers):
        task_id = invoke_solo_function(event, fx_eid, headers, func_id=self.func_id)
        return task_id

    def register_function(self):
        from funcx import FuncXClient

        assert(self.extr_func is not None, "Extractor function must first be registered!")

        fxc = FuncXClient()

        container_id = fxc.register_container(
            location=self.store_url,
            container_type='docker',
            name='kube-tabular',
            description='I don\'t think so!')
        self.func_id = fxc.register_function(self.extr_func,
                                             container_uuid=container_id,
                                             description="A sum function")

        print(f"The function has been updated! "
              f"Please copy/paste the following code into {self.func_id} function class:\n")
        print(self.func_id)
        return self.func_id
