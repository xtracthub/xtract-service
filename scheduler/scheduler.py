
import os
import time
import boto3
import threading

from queue import Queue

from status_checks import get_crawl_status
from prefetcher.prefetcher import GlobusPrefetcher
from endpoint_strategies.rand_n_families import RandNFamiliesStrategy
from endpoint_strategies.nothing_moves import NothingMovesStrategy
# from orchestrator.extractor_orchestrator import ExtractorOrchestrator
from tests.test_utils.native_app_login import globus_native_auth_login

#####
# TODO pool.
# - add a funcX function that has the sole job of fetching (and returning) configuration info from eps.
# - finish the application of the fetch_all_endpoint_configs() function.
# - function to read data off of the crawl_queue --> load in internal queue.
# - create a strategy base class that includes a 'schedule()' function.


# TODO 1: need list of all available endpoints
class FamilyLocationScheduler:
    def __init__(self, fx_eps: list, crawl_id: str, headers: dict, load_type: str = "from_queue",
                 max_pull_threads: int = 20, endpoint_strategy: str = 'rand_n_families'):

        # Testing variables (do not run unless in debug)
        self.task_cap_until_termination = 50002  # Only want to run 50k files? Set to 50000.
        self.prefetch_remote = True  # In theory this should always be true...
        self.fx_ep_timeout_s = 60

        # General variables
        self.headers = headers
        self.crawl_id = crawl_id
        # self.xorch = ExtractorOrchestrator()
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

        # All sorts of internal queues.
        self.to_schedule_q = Queue()
        self.to_prefetch_q = Queue()
        self.to_xtract_q = Queue()

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
        print(endpoints)
        for item in self.fx_eps_to_check:
            print(item)
            # TODO: populate fx_endpoints
            pass

    def schedule(self):
        pass

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
                    message_body = message["Body"]

                    # IF WE ARE NOT PREFETCHING, place directly into processing queue.
                    if not self.prefetch_remote:
                        self.to_xtract_q = Queue()

                    # OTHERWISE, place into prefetching queue.
                    else:
                        self.to_prefetch_q.put(message_body)

                    del_list.append({'ReceiptHandle': message["ReceiptHandle"],
                                     'Id': message["MessageId"]})

                    if self.n_families_pull_from_sqs % 1000 == 0:
                        print(f"SQS Pull Rate: "
                              f"{self.n_families_pull_from_sqs/(time.time()-self.tasks_pulled_start_time)} families/s")
                found_messages = True

            # If we didn't get messages last time around, and the crawl is over.
            # TODO: TYLER BRING BACK: 06/29
            # if final_kill_check:
            #     # Make sure no final messages squeaked in...
            #     if "Messages" not in sqs_response or len(sqs_response["Messages"]) == 0:
            #
            #         # If GET about to die, then the next step is that prefetcher should die.
            #         # TODO: *Technically* a race condition here since there are n concurrent threads.
            #         # TODO: Should wait until this is the LAST such thread.
            #         self.prefetcher.kill_prefetcher = True
            #
            #         print(f"[GET] Crawl successful and no messages in queue to get...")
            #         print("[GET] Terminating...")
            #         return
            #     else:
            #         final_kill_check = False

            # Step 2. Delete the messages from SQS.
            if len(del_list) > 0:
                self.client.delete_message_batch(
                    QueueUrl=self.crawl_queue,
                    Entries=del_list)

            if not deleted_messages and not found_messages:
                print("[GET] FOUND NO NEW MESSAGES. SLEEPING THEN DOWNGRADING TO IDLE...")
                self.get_families_status = "IDLE"

            if "Messages" not in sqs_response or len(sqs_response["Messages"]) == 0:
                status_dict = get_crawl_status(self.crawl_id)

                if status_dict['crawl_status'] in ["SUCCEEDED", "FAILED"]:
                    final_kill_check = True


# TODO: below is the funcX config code. We should get this working too!
# from tests.test_utils.native_app_login import globus_native_auth_login
headers = globus_native_auth_login()
FamilyLocationScheduler(fx_eps=[],
                        crawl_id='c2788750-b7fc-4e00-85c8-f8c0963bdd23',
                        headers=headers)


# config_load = FamilyLocationScheduler.fetch_all_endpoint_configs()
# from funcx import FuncXClient

# from utils import config_func_info

# fxc = FuncXClient()
# func_uuid = fxc.register_function(function=config_func_info['function'], function_name='xtract_config')
# print(func_uuid)
# task_id = fxc.run('/home/skluzacek/.xtract', 'tyler-midway', function_id=func_uuid, endpoint_id="6b10014b-7dd3-482e-8996-b16f818e2921")
# print(task_id)
# while True:
#     res = fxc.get_task(task_id=task_id)
#     print(res)
#
#     if "exception" in res:
#         res["exception"].reraise()
#     import time
#     time.sleep(5)
