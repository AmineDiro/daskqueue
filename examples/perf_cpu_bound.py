import time
import os
from distributed import Client, LocalCluster, Actor
from daskqueue import QueuePool, ConsumerBaseClass, ConsumerPool
from daskqueue.utils import logger


class CPUConsumer(ConsumerBaseClass):
    def process_item(self, item):
        return sum(i * i for i in range(10**8))


if __name__ == "__main__":
    client = Client(
        n_workers=3,
        threads_per_worker=1,
        worker_dashboard_address=":8787",
        direct_to_workers=True,
    )

    queue_pool = client.submit(QueuePool, 1, actor=True).result()
    dashboard_port = client.dashboard_link.split(":")[-1]

    logger.info(f" Dashboard link : http://IP:{dashboard_port}")
    n_consumers = 2

    consumer_pool = ConsumerPool(client, CPUConsumer, n_consumers, queue_pool)
    consumer_pool.start()

    for i in range(10):
        queue_pool.put_many(list(range(10)))

    consumer_pool.join()
