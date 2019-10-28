
from queue import Queue
from utils.pg_utils import pg_conn
import funcx
import json


# TODO: Inherit orchestrator class.
class MatioOrchestrator:
    def __init__(self, source_globus_ep_id, dest_globus_ep_id, crawl_id):
        self.source_globus_ep_id = source_globus_ep_id
        self.dest_globus_ep_id = dest_globus_ep_id
        self.crawl_id = crawl_id
        self.conn = pg_conn()

        # TODO: Store size of group as sort of resource tracking.
        # TODO: We'll later want to be extensible to multiple container types.
        self.matio_queue = Queue()
        self.conn = pg_conn()

        # Automatically populate the queue.
        self.populate_groups_queue()
        self.max_tasks_in_flight = 10  # TODO: this will need to be bigger.
        self.tasks_in_flight = 0

    def populate_groups_queue(self):
        cur = self.conn.cursor()
        update_cur = self.conn.cursor()
        query = f"SELECT group_id from groups where crawl_id='{self.crawl_id}' and status='crawled';"
        cur.execute(query)

        groups = cur.fetchall()

        count = 0
        for group in groups:
            print(group)
            update_query = f"UPDATE groups SET status='extract_pending' WHERE group_id='{group[0]}';"
            update_cur.execute(update_query)
            self.matio_queue.put(group[0])

            count += 1
            if count % 50 == 0:  # Commit every once in a while.
                self.conn.commit()
                break # TODO: Remove this break

    def extract_metadata(self):
        print("Pulling from queue...")
        print(self.matio_queue.qsize())
        current_group = self.matio_queue.get()

        print("Opening metadata from file system")
        # TODO: Update current group to 'extracting'.
        with open(f'../xtract_metadata/{current_group}.mdata', 'r') as f:
            physical_mdata = json.load(f)
            file_list = physical_mdata["files"]

            print(physical_mdata)
            # TODO: edit matio function to process groups.


# m = MatioOrchestrator('potato', 'tomato', '31598e28-c4e9-43c0-8c21-76db895b239c')
#
# m.populate_groups_queue()
# m.extract_metadata()

# print(m.matio_queue.qsize())
