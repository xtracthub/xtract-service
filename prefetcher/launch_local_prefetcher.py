from prefetcher import globus_poller_funcx
import argparse



def launch_local(crawl_id): 
    transfer_token = "AgPvlE6vjNoGq51Q6mj0g2MQxyQYYxQe9wGQbz6QPrdwgeWJ6bCmComJ4kqnlG6eka6OMlpeJ5v0qPcqDvlBks16eo"

    data_source = "e38ee745-6d04-11e5-ba46-22000b92c6ec"
    data_dest = "af7bda53-6d04-11e5-ba46-22000b92c6ec"
    data_dest_path = "/project2/chard/skluzacek/data-to-process/"


    event = {'transfer_token': transfer_token,
             'crawl_id': crawl_id,
             'data_source': data_source,
             'data_dest': data_dest,
             'data_path': data_dest_path,
             'max_gb': 5}

    globus_poller_funcx(event)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Initialize Local Globus Prefetch') 
    parser.add_argument('--crawl_id', '-C',  metavar='C', type=str, nargs=1)

    args = parser.parse_args()
    crawl_id = args.crawl_id[0]

    launch_local(crawl_id)
