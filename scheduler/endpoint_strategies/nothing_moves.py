

class NothingMovesStrategy:
    def __init__(self, debug: bool = False):
        self.debug = debug
        print(f"Debug mode: {self.debug}")

    def schedule(self, fx_eps: dict, families: list):
        sched_dict = dict()  # this will be a mapping of fx_ep_id to a list of families.

        # Here we need to know the fx_ep  # TODO: pass in globus_eid for a file in the metadata.



        return sched_dict