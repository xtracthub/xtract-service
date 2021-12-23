
import os
import json
import time
import boto3
import threading

from random import shuffle
from queue import Queue, PriorityQueue
from xtract_sdk.packagers.family_batch import FamilyBatch
from xtract_sdk.packagers.family import Family

from status_checks import get_crawl_status
from scheddy.endpoint_strategies.rand_n_families import RandNFamiliesStrategy
from scheddy.endpoint_strategies.nothing_moves import NothingMovesStrategy
from scheddy.maps.name_to_extractor_map import extractor_map

from scheddy.extractor_strategies.extension_map import ExtensionMapStrategy
from funcx import FuncXClient
from globus_sdk import AccessTokenAuthorizer


class PriorityEntry(object):

    def __init__(self, priority, data):
        self.data = data
        self.priority = priority

    def __lt__(self, other):
        return self.priority < other.priority


def get_all_extractors(fx_ep_ls):

    from utils.pg_utils import pg_conn
    cur = pg_conn().cursor()  # don't need connection object; don't need to commit.

    # Should be {extractor_name: {'funcx_id": uuid}}
    all_extractors = dict()
    for fx_ep in fx_ep_ls:
        print('here')
        get_query = f"""SELECT ext_name, func_uuid from extractors WHERE fx_eid='{fx_ep}';"""
        cur.execute(get_query)

        for item in cur.fetchall():
            ext_name, func_uuid = item
            if ext_name not in all_extractors:
                all_extractors[ext_name] = dict()
            all_extractors[ext_name][fx_ep] = func_uuid

    return all_extractors


def get_fx_client(headers):
    tokens = headers
    fx_auth = AccessTokenAuthorizer(tokens['Authorization'])
    search_auth = AccessTokenAuthorizer(tokens['Search'])
    openid_auth = AccessTokenAuthorizer(tokens['Openid'])
    print(f"TRYING TO CREATE FUNCX CLIENT")

    print(f"fx_auth: {fx_auth}")
    print(f"search_auth: {search_auth}")
    print(f"openid_auth: {openid_auth}")

    fxc = FuncXClient(fx_authorizer=fx_auth,
                      search_authorizer=search_auth,
                      openid_authorizer=openid_auth)
    return fxc


# TODO 1: need list of all available endpoints (ignoring for demo)
class FamilyLocationScheduler:
    def __init__(self, fx_eps: list, crawl_id: str, headers: dict, load_type: str = "from_queue",
                 max_pull_threads: int = 1, endpoint_strategy: str = 'rand_n_families'):

        # TODO: generalize as kwarg
        self.extractor_scheduler = ExtensionMapStrategy()

        self.counters = {'fx': {'success': 0, 'failed': 0, 'pending': 0},
                         'flagged_unknown': 0,
                         'cumu_pulled': 0,
                         'cumu_orch_enter': 0,
                         'cumu_to_schedule': 0,
                         'cumu_scheduled': 0}

        self.cur_status = "INIT"

        self.new_tasks_flag = True
        self.tasks_to_sched_flag = True

        self.funcx_current_tasks = Queue()

        # Testing variables (do not run unless in debug)
        self.task_cap_until_termination = 50002  # Only want to run 50k files? Set to 50000.
        self.prefetch_remote = False  # In theory this should always be true...
        self.fx_ep_timeout_s = 60

        # General variables
        self.headers = headers
        self.crawl_id = crawl_id
        self.get_families_status = "STARTING"

        # Scheduling strategy setup
        self.endpoint_strategy = endpoint_strategy
        if self.endpoint_strategy == 'rand_n_families':
            self.strategy_exec = RandNFamiliesStrategy(n=0.5)
        elif self.endpoint_strategy == 'nothing_moves':
            self.strategy_exec = NothingMovesStrategy()

        # Variables about funcX endpoints
        self.fx_eps_to_check = fx_eps
        self.fx_eps_ready = dict()

        # *** QUEUES ***
        self.to_schedule_pqs = dict()  # PRIORITY queues for endpoint scheduling.
        self.to_prefetch_q = Queue()  # QUEUE for the order in which files are prefetched.

        # TODO: should be 1 queue for each destination.
        self.to_xtract_q = Queue()  # QUEUES for the order in which files sent to extractors.

        # Variables about pulling data into the scheduler (and start it)
        self.load_type = load_type
        self.max_pull_threads = max_pull_threads
        self.n_families_pull_from_sqs = 0
        self.tasks_pulled_start_time = time.time()

        self.pause_q_consume = False

        self.client = boto3.client('sqs',
                                   aws_access_key_id=os.environ["aws_access"],
                                   aws_secret_access_key=os.environ["aws_secret"], region_name='us-east-1')
        q_prefix = "crawl"
        response = self.client.get_queue_url(
            QueueName=f'{q_prefix}_{self.crawl_id}',
            QueueOwnerAWSAccountId=os.environ["aws_account"]
        )
        self.crawl_queue = response["QueueUrl"]

        if self.load_type == 'from_queue':
            for i in range(0, self.max_pull_threads):
                print(f"Attempting to start get_next_families() as its own thread [{i}]... ")
                consumer_thr = threading.Thread(target=self.task_pulldown_thread, args=())
                consumer_thr.start()
                print(f"Successfully started the get_next_families() thread number {i} ")

        # orch_thread = threading.Thread(target=self.orch_thread, args=(headers,))
        # orch_thread.start()

        # # TODO: right now the prefetcher takes too much info. Should just input a bunch of orders.
        # self.prefetcher = GlobusPrefetcher(transfer_token=self.headers['Transfer'],
        #                                    crawl_id=self.crawl_id,
        #                                    data_source=self.source_endpoint,
        #                                    data_dest=self.dest_endpoint,
        #                                    data_path=self.data_prefetch_path,
        #                                    max_gb=500)  # TODO: bounce this out.

    def fetch_all_endpoint_configs(self, endpoints):
        """ Fetch all endpoint configurations via funcX.

            This enables us to:
            (1) invoke idle endpoints (e.g., Midway2/Theta)
            (2) get the Globus EP associated with a given endpoint
        """
        # print(endpoints)
        for item in self.fx_eps_to_check:
            print(item)
            # TODO: populate fx_endpoints
            pass

    def schedule(self, tiebreaker='random'):

        if self.prefetch_remote:
            raise NotImplementedError("Need to move this into a proper scheduler.")
        else:
            # Drain priority queues in order (make sure same extractor near each other)
            while True:
                # Hold the value of the queue with the leading priority.
                ext_pri_mapping = dict()
                for ext_type in self.to_schedule_pqs:

                    # If file type is unknown, then we skip. # TODO: think more about how to handle 'unknowns'.
                    if ext_type in ['unknown']:
                        # print(f"Unknown continue for ext_type {ext_type}")
                        continue

                    # If there is nothing in a given priority queue, just skip it.
                    if self.to_schedule_pqs[ext_type].qsize() == 0:
                        # print(ext_type)
                        # print(f"Q Size continue for ext_type {ext_type}")
                        continue

                    ext_pri_mapping[ext_type] = self.to_schedule_pqs[ext_type].queue[0].priority

                # If there is nothing to order, then we should break. # TODO: when in loop, will want a wait/retry.
                # print(f"Number of extractors in queue: {len(ext_pri_mapping)}")
                if len(ext_pri_mapping) == 0:
                    print("Nothing left to schedule! Breaking...")
                    break

                # print(f"Ext Pri Mapping: {ext_pri_mapping}")
                # List of all 'maximum priority' extractor (queue) names.
                max_value = max(ext_pri_mapping.values())
                max_value_items = [key for key, val in ext_pri_mapping.items() if val == max_value]
                # print(f"Here are our max_value_items: {max_value_items}")

                # If there's a tie.
                if len(max_value_items) >= 1:
                    # if our tiebreaker is 'random', then pop all leaders in random order.
                    # NOTE: pop all at once to avoid unnecessary iterations + checks.
                    if tiebreaker == 'random':
                        #  print(f"IN RANDOM")
                        max_value_items = list(max_value_items)  # cast to list so we can shuffle
                        # print(f"Max value items: {max_value_items}")

                        # "Tuple" object does not support item assignment
                        # print(type(max_value_items))
                        shuffle(max_value_items)

                        # DRAIN the leaders into to_xtract_q.
                        for extractor_name in max_value_items:
                            full_pq = self.to_schedule_pqs[extractor_name]
                            packed_pri_obj = full_pq.get()
                            fam = packed_pri_obj.data

                            # print(f"Fetched: {fam}")
                            fam['first_extractor'] = extractor_name
                            self.to_xtract_q.put(fam)
                            self.counters['cumu_scheduled'] += 1

        self.tasks_to_sched_flag = False
        self.cur_status = "SCHEDULED"

        orch = self.orch_thread(headers=self.headers)

    def orch_thread(self, headers):
        to_terminate = False

        # TYLER: Getting rid of imported function_ids
        # from scheddy.maps.function_ids import functions
        # TODO: GET ALL OF THE FUNCTIONS RIGHT HERE FROM DB.
        print(f"ENDPOINTS TO CHECK: {self.fx_eps_to_check}")
        all_extractors = get_all_extractors(self.fx_eps_to_check)
        print(f"Fetched all extractors... {all_extractors}")

        fxc = get_fx_client(headers)

        while True:

            # If our accounting is complete
            # NOTE: when concurrent, will also need to check if scheduling is DONE.
            # print(f"Checking done-ness...")
            if self.counters['fx']['success'] + \
                    self.counters['fx']['failed'] + \
                    self.counters['flagged_unknown'] == self.counters['cumu_scheduled'] \
                    and self.cur_status == 'SCHEDULED':
                to_terminate = True

            if to_terminate:
                print("[ORCH] Terminating!")
                print(f"Final counters: {self.counters}")
                self.cur_status = 'COMPLETED'  # TODO: Need to push this status to DB.
                break

            print(f"[ORCH] WQ length: {self.to_xtract_q.qsize()}")

            if self.to_xtract_q.empty() and self.funcx_current_tasks.empty():
                print(f"[ORCH] Empty work thread. Sleeping...")
                time.sleep(5)

            else:

                batch = fxc.create_batch()

                batch_len = 0
                while not self.to_xtract_q.empty():  # TODO: also need max batch size here.
                    family = self.to_xtract_q.get()
                    self.counters['cumu_orch_enter'] += 1

                    extractor_id = family['first_extractor']

                    if extractor_id in extractor_map:
                        extractor = extractor_map[extractor_id]
                    else:
                        self.counters['flagged_unknown'] += 1
                        continue

                    # *** Spaghetti code zone ***
                    # We should not need to repack and add an empty base_url
                    fam_batch = FamilyBatch()
                    packed_family = Family()
                    family['base_url'] = None
                    packed_family.from_dict(family)

                    fam_batch.add_family(packed_family)

                    event = extractor.create_event(
                        family_batch=fam_batch,
                        ep_name='foobar',
                        xtract_dir="/home/tskluzac/.xtract",
                        sys_path_add="/",
                        module_path=f"xtract_{extractor_id}_main",
                        metadata_write_path='/home/tskluzac/mdata'
                    )

                    fx_ep_id = self.fx_eps_to_check[0]  # TODO: Should not be fixed to first fx_ep.

                    batch.add(event,
                              endpoint_id=fx_ep_id,
                              function_id=all_extractors[f"xtract-{extractor_id}"][fx_ep_id])
                    batch_len += 1

                # Only want to send tasks if we retrieved tasks.
                if batch_len > 0:
                    batch_res = fxc.batch_run(batch)
                    time.sleep(1.1)
                    for item in batch_res:
                        self.funcx_current_tasks.put(item)

                poll_batch = []

                # print("Entering task loop")
                for i in range(0, 20):  # TODO: hardcode
                    if not self.funcx_current_tasks.empty():
                        tid = self.funcx_current_tasks.get()
                        poll_batch.append(tid)
                # print(f"Current length of poll_batch: {len(poll_batch)}")

                if len(poll_batch) > 0:
                    x = fxc.get_batch_result(poll_batch)
                    time.sleep(1.1)
                    # print(f"Poll result: {x}")
                    for item in x:
                        result = x[item]

                        if result['status'] == 'success':
                            # print(f"Success result: {result}")
                            self.counters['fx']['success'] += 1

                        elif result['status'] == 'failed':
                            result['exception'].reraise()
                            self.counters['fx']['failures'] += 1

                        elif result['pending']:
                            self.funcx_current_tasks.put(item)
                        else:
                            # If we haven't figured it out until here, we need some dev...
                            raise ValueError("[ORCH] CRITICAL Unrecognized funcX status...")
                    print(self.counters)

    def task_pulldown_thread(self):
        """

        task_pulldown_thread() will keep polling SQS queue containing all of the crawled data. This should generally be
        read as its own thread, and terminate when the orchestrator's POLLER is completed
        (because, well, that means everything is finished)

        If prefetching is turned OFF, then families will be placed directly into families_to_process queue.

        If prefetching is turned ON, then families will be placed directly into families_to_prefetch queue.

        NOTE: *n* current threads of this loop!

        """

        final_kill_check = False

        while True:
            # Getting this var to avoid flooding ourselves with SQS messages we can't process
            # num_pulled_but_not_pfing = self.prefetcher.next_prefetch_queue.qsize()

            # TODO: TYLER BRING BACK. 06/29
            # if self.num_families_fetched >= self.task_cap_until_termination:
            #     print(f"HIT CAP ON NUMBER OF TASKS. Terminating...")
            #     # Here we also tell the prefetcher that it's time to start terminating.
            #     self.prefetcher.last_batch = True
            #     return

            # Do some queue pause checks (if too many current uncompleted tasks)
            # TODO: TYLER BRING BACK 06/29
            # First, let's see how many uncompleted tasks are in XOrch.
            # if self.xorch.num_extracting_tasks > self.xorch.max_extracting_tasks:
            #     self.pause_q_consume = True
            #     print(f"[GET] Num. active tasks ({self.xorch.num_extracting_tasks}) "
            #           f"above threshold. Pausing SQS consumption...")

            if self.pause_q_consume:
                print("[GET] Pausing pull of new families. Sleeping for 20 seconds...")
                time.sleep(20)
                continue

            # Step 1. Get ceiling(batch_size/10) messages down from queue.
            # Otherwise, we can just pluck straight from the sqs crawl queue
            sqs_response = self.client.receive_message(  # TODO: properly try/except this block.
                QueueUrl=self.crawl_queue,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=1)

            del_list = []
            found_messages = False
            deleted_messages = False

            if ("Messages" in sqs_response) and (len(sqs_response["Messages"]) > 0):
                self.get_families_status = "ACTIVE"
                for message in sqs_response["Messages"]:
                    self.n_families_pull_from_sqs += 1
                    self.counters['cumu_pulled'] += 1
                    message_body = message["Body"]

                    # TODO: moved the prefetch logic.
                    # IF WE ARE NOT PREFETCHING, place directly into processing queue.
                    # if not self.prefetch_remote:
                    #     self.to_xtract_q = Queue()

                    # OTHERWISE, place into prefetching queue.
                    # else:
                    #     self.to_prefetch_q.put(message_body)

                    del_list.append({'ReceiptHandle': message["ReceiptHandle"],
                                     'Id': message["MessageId"]})

                    if self.n_families_pull_from_sqs % 1000 == 0:
                        print(f"SQS Pull Rate: "
                              f"{self.n_families_pull_from_sqs/(time.time()-self.tasks_pulled_start_time)} families/s")

                    message_as_dict = json.loads(message_body)
                    pri_extractor_pairs = self.extractor_scheduler.assign_files_to_extractors([message_as_dict])

                    for item in pri_extractor_pairs:
                        priority, extractor = item

                        if extractor not in self.to_schedule_pqs:
                            self.to_schedule_pqs[extractor] = PriorityQueue()

                        # print(f"Priority: {priority}")
                        # print(f"Extractor: {extractor}")
                        pri_entry_obj = PriorityEntry(priority, message_as_dict)
                        self.to_schedule_pqs[extractor].put(pri_entry_obj)
                        self.counters['cumu_to_schedule'] += 1

                found_messages = True

            # Step 2. Delete the messages from SQS.
            if len(del_list) > 0:
                self.client.delete_message_batch(
                    QueueUrl=self.crawl_queue,
                    Entries=del_list)

            if not deleted_messages and not found_messages:
                print("[GET] FOUND NO NEW MESSAGES. SLEEPING THEN DOWNGRADING TO IDLE...")
                self.get_families_status = "IDLE"

            # If we manage to pull nothing from SQS...
            if "Messages" not in sqs_response or len(sqs_response["Messages"]) == 0:

                # Quick check to see if 'status is 'complete'/'failed'
                status_dict = get_crawl_status(self.crawl_id)

                # if the crawl is completed (either by success or failure reasons...)
                if status_dict['crawl_status'] in ['complete', 'failed']:
                    # flag the class as 'no_new_tasks'
                    self.new_tasks_flag = False
                    print(f"[SCHEDULER] Terminating task pull-down thread!")

                    self.schedule()

                    break
