
from prefetcher.prefetcher import GlobusPrefetcher
from tests.test_utils.native_app_login import globus_native_auth_login


headers = globus_native_auth_login()
crawl_id = 'b45da65b-25d0-4b83-82f5-06d9823927ca'

pf = GlobusPrefetcher(transfer_token=headers['Transfer'],
                      crawl_id=crawl_id,
                      data_source="45a53408-c797-11e6-9c33-22000a1e3b52",  # Xtract's Petrel
                      data_dest="af7bda53-6d04-11e5-ba46-22000b92c6ec",  # Midway2
                      data_path="/project2/chard/skluzacek/test_prefetcher",
                      max_gb=1)

pf.start()
