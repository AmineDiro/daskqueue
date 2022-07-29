import queue
import time
import pytest
from distributed import Client
from daskqueue import QueuePool, ConsumerBaseClass, ConsumerPool

from daskqueue.utils import logger


def process_item():
    time.sleep(1)
    logger.debug("[Executor] processing ")
    with open("/dev/urandom", "rb") as f:
        return f.read(100)


if __name__ == "__main__":
    client = Client(n_workers=3, threads_per_worker=1)

    queue_pool = client.submit(QueuePool, 1, actor=True).result()

    ## DEBUG
    q = queue_pool.get_max_queue().result()

    n_consumers = 1
    consumer_pool = ConsumerPool(client, queue_pool, n_consumers=1)
    consumer_pool.start()

    for i in range(10):
        queue_pool.submit(process_item)

    consumer_pool.join()
