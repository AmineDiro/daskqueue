import time
import os
from distributed import Client
from daskqueue import QueuePool, ConsumerPool
from daskqueue.utils import logger


def process_item():
    return sum(i * i for i in range(10**8))


if __name__ == "__main__":

    client = Client(
        n_workers=5,
        threads_per_worker=1,
        dashboard_address=":3338",
        direct_to_workers=True,
    )

    ## Params
    n_queues = 2
    n_consumers = 2

    queue_pool = QueuePool(client, n_queues)

    consumer_pool = ConsumerPool(client, queue_pool, n_consumers=n_consumers)

    consumer_pool.start()

    for i in range(5):
        queue_pool.submit(process_item)

    consumer_pool.join(timestep=0.01, print_timestep=2, progress=True)
