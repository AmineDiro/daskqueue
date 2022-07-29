import time
import os
from distributed import Client, LocalCluster, Actor
from daskqueue import QueuePool, ConsumerBaseClass, ConsumerPool
from daskqueue.utils import logger


def process_item():
    return sum(i * i for i in range(10**4))


if __name__ == "__main__":
    client = Client(
        n_workers=3,
        threads_per_worker=1,
        dashboard_address=":8787",
        direct_to_workers=True,
    )

    n_consumers = 1

    queue_pool = client.submit(QueuePool, n_queues=1, actor=True).result()
    # TODO : QueuePool interface, .submit()

    dashboard_port = client.dashboard_link.split(":")[-1]

    logger.info(f" Dashboard link : http://localhost:{dashboard_port}")

    consumer_pool = ConsumerPool(client, queue_pool, n_consumers=n_consumers)
    consumer_pool.start()

    for i in range(10):
        queue_pool.submit(process_item)

    consumer_pool.join()
