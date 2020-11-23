

from queue import Queue
import globus_sdk
import json
import time
import threading


class GlobusPrefetcher:

    def __init__(self, transfer_token, crawl_id, data_source, data_dest, data_path, max_gb):

        self.transfer_token = transfer_token
        self.crawl_id = crawl_id

        self.transfer_check_queue = Queue()

        self.client_id = "83cd643f-8fef-4d4b-8bcf-7d146c288d81"
        self.data_source = data_source
        self.data_dest = data_dest
        self.data_path = data_path

        self.max_gb = max_gb
        self.max_files_in_batch = 10000

        self.last_batch = False
        bytes_in_gb = 1024 * 1024 * 1024

        self.max_bytes = bytes_in_gb * max_gb
        self.block_size = self.max_bytes / 20

        self.client = None
        self.tc = None

        self.family_map = dict()
        self.fam_to_size_map = dict()
        self.local_transfer_queue = Queue()
        self.transfer_map = dict()

        self.num_transfers = 0
        self.num_families_transferred = 0

        self.num_families_mid_transfer = 0

        self.current_batch = []
        self.current_batch_bytes = 0
        self.current_batch_num_families = 0

        self.bytes_pf_in_flight = 0
        self.bytes_pf_completed = 0  # This goes before they become 'bytes_orch_unextracted'

        self.get_globus_tc(self.transfer_token)

        self.orch_reader_q = Queue()

        # Puppet variables. The orchestrator will edit these.
        self.orch_unextracted_bytes = 0
        self.next_prefetch_queue = Queue()

        # The orchestrator class will kill any loops here by changing this variable.
        self.kill_prefetcher = False

        self.pf_msgs_pulled_since_last_batch = 0

    def get_globus_tc(self, TRANSFER_TOKEN):

        authorizer = globus_sdk.AccessTokenAuthorizer(TRANSFER_TOKEN)
        self.tc = globus_sdk.TransferClient(authorizer=authorizer)

    def get_family_size(self, family):
        tot_fam_size = 0
        for file_obj in family["files"]:
            file_size = file_obj["metadata"]["physical"]["size"]
            tot_fam_size += file_size
        return tot_fam_size

    def transfer_queue_thread(self):
        # TODO: this is also a thread that needs to be killed!

        gl_task_tmp_ls = []

        while True:
            gl_tid = self.transfer_check_queue.get()
            res = self.tc.get_task(gl_tid)

            if res['status'] != "SUCCEEDED":
                gl_task_tmp_ls.append(gl_tid)
            else:

                fids = self.transfer_map[gl_tid]

                for fid in fids:

                    family = self.family_map[fid]
                    fam_size = self.get_family_size(family)
                    family['metadata']["pf_transfer_completed"] = time.time()

                    self.bytes_pf_in_flight -= fam_size
                    self.bytes_pf_completed += fam_size

                    self.orch_reader_q.put(json.dumps(family))
                    self.num_families_mid_transfer -= 1

                # Sleep is here to avoid pressure on the Globus service in case of 'not done'.
                time.sleep(0.5)

            if self.transfer_check_queue.empty():

                for gl_tid in gl_task_tmp_ls:
                    self.transfer_check_queue.put(gl_tid)
                gl_task_tmp_ls = []

                print(f"[PF TRANSFER QUEUE] Queue empty. Sleeping for 5 seconds! ")
                time.sleep(5)

    def get_new_families(self):

        # Grab n tasks at a timefrom the internal 'to-prefetch' queue.
        i = 0
        families_to_pf = []

        while i < 1000 and not self.next_prefetch_queue.empty():
            i += 1
            pf_family = self.next_prefetch_queue.get()
            families_to_pf.append(pf_family)
            self.pf_msgs_pulled_since_last_batch += 1

        size_of_fams = 0
        if len(families_to_pf) > 0:

            for fam_json in families_to_pf:
                family = json.loads(fam_json)

                fam_id = family["family_id"]

                tot_fam_size = self.get_family_size(family)

                self.family_map[fam_id] = family
                self.local_transfer_queue.put(fam_id)
                self.fam_to_size_map[fam_id] = tot_fam_size

                size_of_fams += tot_fam_size
            self.bytes_pf_in_flight += size_of_fams

        return size_of_fams

    def main_poller_loop(self):

        transfer_thr = threading.Thread(target=self.transfer_queue_thread, args=())
        transfer_thr.start()

        need_more_families = True

        while True:

            if self.kill_prefetcher and self.last_batch:
                print("[PF] Killing main PF loop...")
                return

            # However in the case where it's NOT last_batch.
            elif self.kill_prefetcher:
                print("Kill_Prefetcher message received... Checking if last batch...")
                if self.next_prefetch_queue.empty():
                    print("Prefetch queue empty! Signifying last batch...")
                    self.last_batch = True

            if need_more_families:
                total_size = self.get_new_families()

                # If nothing to prefetch, BUT not ready to kill...
                if total_size is None:
                    print("No new prefetchable families, but not ready to kill. CONTINUING!")
                    continue

            # Dir size is added via the following...
            eff_dir_size = self.orch_unextracted_bytes + self.bytes_pf_in_flight + self.bytes_pf_completed

            # If we are under the folder size.
            if eff_dir_size < self.max_bytes:

                # As long as our local queue is not empty...
                while not self.local_transfer_queue.empty():

                    need_more_families = True

                    cur_fam_id = self.local_transfer_queue.get()
                    cur_fam_size = self.fam_to_size_map[cur_fam_id]

                    self.current_batch.append(self.family_map[cur_fam_id])
                    self.current_batch_bytes += cur_fam_size
                    self.current_batch_num_families += 1

                    # If we filled our block to the brim, close the batch!
                    if self.current_batch_bytes >= self.block_size or \
                            len(self.current_batch) >= self.max_files_in_batch:
                        print("Batch is full! Breaking loop!!!")
                        break

            else:
                print(f"[PF] Directory full with {eff_dir_size / 1024 / 1024 / 1024} GB. Sleeping for 5s...")
                print(self.orch_unextracted_bytes)
                print(self.bytes_pf_in_flight)  # TODO: in-flight.
                print(self.bytes_pf_completed)
                time.sleep(5)

            # The following are the conditions in which we send more work:
            # 1. batch_size_full: We have filled up our block and it it now time to send more data.
            batch_size_full = self.current_batch_bytes >= self.block_size
            # 2. last_batch: Means we have a too-small batch, but no new work is coming.
            last_batch = self.last_batch and len(self.current_batch) > 0
            # 3. batch_n_files_full: We have filled up the maximum number of files for 1 batch.
            batch_n_files_full = len(self.current_batch) > self.max_files_in_batch

            if batch_size_full or last_batch or batch_n_files_full:
                print("[PF] Generating a batch transfer object...")
                self.pf_msgs_pulled_since_last_batch = 0

                tdata = globus_sdk.TransferData(transfer_client=self.tc,
                                                source_endpoint=self.data_source,
                                                destination_endpoint=self.data_dest)

                fid_list = []
                for family_to_trans in self.current_batch:

                    # Create a directory (named by family_id) into which we want to place our families.
                    # TODO: hardcoding.
                    fam_dir = '/project2/chard/skluzacek/data_to_process/{}'.format(family_to_trans['family_id'])

                    for file_obj in family_to_trans['files']:
                        file_path = file_obj['path']
                        file_name = file_obj['path'].split('/')[-1]

                        file_obj['path'] = f"{fam_dir}/{file_name}"

                        tdata.add_item(file_path, f"{fam_dir}/{file_name}")
                    fid_list.append(family_to_trans['family_id'])

                    # Now we do the same for groups... # TODO: Move this upstream. Crawler?
                    for group_obj in family_to_trans['groups']:
                        for file_obj in group_obj['files']:
                            file_path = file_obj['path']
                            file_name = file_path.split('/')[-1]

                            file_obj['path'] = f"{fam_dir}/{file_name}"
                    self.num_families_transferred += 1
                    self.num_families_mid_transfer += 1

                transfer_result = self.tc.submit_transfer(tdata)
                self.num_transfers += 1
                print(f"Transfer result: {transfer_result}")
                gl_task_id = transfer_result['task_id']
                self.transfer_check_queue.put(gl_task_id)

                self.transfer_map[gl_task_id] = fid_list

                self.current_batch = []
                self.current_batch_bytes = 0
                self.current_batch_num_families = 0

            else:
                need_more_families = True

            if self.last_batch and self.transfer_check_queue.empty():
                if self.kill_prefetcher:
                    print("[PF] Prefetcher kill message received and nothing else to transfer...")
                    print("[PF] Terminating...")
                    return "SUCCESS"
                else:
                    print("Last current batch, but still waiting on more data from orchestrator. Sleeping for 5s...")
                    time.sleep(5)
