import queue
import time
import pytest
from distributed import Client
from daskqueue import QueuePool, ConsumerBaseClass, ConsumerPool

from daskqueue.utils import logger


def open_file():
    time.sleep(1)
    with open("/dev/urandom", "rb") as f:
        return f.read(100)


if __name__ == "__main__":
    client = Client(
        n_workers=5,
        threads_per_worker=1,
        dashboard_address=":3338",
        direct_to_workers=True,
    )

    ## Params
    n_queues = 1
    n_consumers = 2

    queue_pool = QueuePool(client, n_queues)

    consumer_pool = ConsumerPool(client, queue_pool, n_consumers=n_consumers)

    print(queue_pool)
    print(consumer_pool)

    consumer_pool.start()

    for i in range(5):
        queue_pool.submit(open_file)

    consumer_pool.join()

    print(queue_pool)
    print(consumer_pool)
