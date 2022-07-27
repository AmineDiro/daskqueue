import queue
import time
import pytest
from distributed import Client, LocalCluster, Actor
from daskqueue import QueuePool, ConsumerBaseClass, ConsumerPool
import logging


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,%(msecs)d %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)


class IOConsumer(ConsumerBaseClass):
    def process_item(self, item):
        logging.info(f"[{self.id}] : Processing {item}")
        with open("/dev/urandom", "rb") as f:
            return f.read(100)


if __name__ == "__main__":
    client = Client(n_workers=3, threads_per_worker=1)

    queue_pool = client.submit(QueuePool, 1, actor=True).result()

    ## DEBUG
    q = queue_pool.get_max_queue().result()

    n_consumers = 5
    consumer_pool = ConsumerPool(client, IOConsumer, n_consumers, queue_pool)
    consumer_pool.start()

    for i in range(10):
        queue_pool.put_many(list(range(10)))

    consumer_pool.join()
