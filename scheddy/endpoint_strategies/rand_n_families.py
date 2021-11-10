
from random import sample, shuffle
from queue import Queue


class RandNFamiliesStrategy:
    def __init__(self, n: float, debug: bool = False):
        assert 0 <= n <= 1, "n must be a configurable percentage between 0 and 1 (i.e., 0.5 means 50%)"
        self.n = n
        self.debug = debug
        print(f"Debug mode: {self.debug}")

    def schedule(self, fx_eps: dict, families: list):
        sched_dict = dict()  # this will be a mapping of fx_ep_id to a list of families.

        n_families_to_offload = int(self.n * len(families))
        families_to_offload_ls = sample(families, n_families_to_offload)
        families_to_offload_q = Queue()
        [families_to_offload_q.put(x) for x in families_to_offload_ls]

        # Now we assign tasks in round-robin WORKER FILL ordering
        # --> if epA has 20 workers, epB has 10 workers, ..., then we fill A with 20 before filling B with 10, etc.
        fx_ep_ids = list(fx_eps.keys())
        shuffle(fx_ep_ids)  # Shuffle to avoid picking the first fx_ep in dictionary-order

        for fx_eid in fx_ep_ids:
            sched_dict[fx_eid] = []

        # Avoid infinite loop in the 'no workers' edge case
        total_workers = 0
        for fx_eid in fx_ep_ids:
            total_workers += fx_eps[fx_eid]['workers']
        if total_workers == 0:
            if self.debug:
                print(f"[strategies/rand_n_families]: No workers on which to schedule. Returning empty schedule!")
            return sched_dict  # sched_dict still empty, so fine to return.

        # Here we round-robin distribute families BY WORKERS to each fx_ep (until no remaining work).
        while n_families_to_offload > 0:  # While our queue isn't empty (but done this way for bookkeeping)
            for fx_eid in fx_ep_ids:
                num_workers = fx_eps[fx_eid]['workers']
                if self.debug:
                    print(f"[strategies/rand_n_families]: Number of workers on {fx_eid}: {num_workers}")
                    print(f"[strategies/rand_n_families]: Number of families left to offload: {n_families_to_offload}")

                if n_families_to_offload <= num_workers:
                    # print(self.debug)
                    if self.debug:
                        print("[strategies/rand_n_families]: case 1: more workers than families (or equal)")
                    num_fams_to_apply = n_families_to_offload  # so we can mute n_families_to_offload in the loop
                    for i in range(0, num_fams_to_apply):
                        task = families_to_offload_q.get()
                        sched_dict[fx_eid].append(task)
                        n_families_to_offload -= 1
                else:
                    if self.debug:
                        print("[strategies/rand_n_families]: case 2: more families than workers")
                    for i in range(0, num_workers):
                        task = families_to_offload_q.get()
                        sched_dict[fx_eid].append(task)
                        n_families_to_offload -= 1

        return sched_dict
