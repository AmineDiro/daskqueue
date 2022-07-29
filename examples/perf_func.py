from collections import defaultdict
import time
from typing import Any, List, Tuple, Union
import uuid
import random

import numpy as np
from numpy.typing import NDArray
from distributed import Client, Queue

from daskqueue import ConsumerPool, QueuePool


def general_add(
    x: Union[int, float, NDArray], y: Union[int, float, NDArray]
) -> Union[int, float, NDArray]:
    time.sleep(random.random())
    return x + y


# add = lambda x, y : x + y


def main():
    client = Client(
        n_workers=3,
        threads_per_worker=1,
        worker_dashboard_address=":8787",
        direct_to_workers=True,
    )

    ## Params
    n_queues = 1
    n_consumers = 1

    # Queue Pool  with basic load balancing
    queue_pool = client.submit(QueuePool, n_queues, actor=True).result()

    # Start Consummers
    consumer_pool = ConsumerPool(client, queue_pool, n_consumers=n_consumers)
    consumer_pool.start()

    for i in range(10):
        queue_pool.submit(1, i, i + 1)

    # x = np.random.randn(10, 10)
    # y = np.random.randn(10, 10)
    ## Join to stop work
    consumer_pool.join()


if __name__ == "__main__":
    # main()

    client = Client(
        n_workers=3,
        threads_per_worker=1,
        dashboard_address=":3338",
        direct_to_workers=True,
    )

    ## Params
    n_queues = 1
    n_consumers = 1

    # Queue Pool  with basic load balancing
    queue_pool = client.submit(QueuePool, n_queues, actor=True).result()

    # Start Consummers
    consumer_pool = ConsumerPool(client, queue_pool, n_consumers=n_consumers)

    consumer_pool.start()
    for i in range(10):
        queue_pool.submit(general_add, i, i + 1).result()

    # x = np.random.randn(10, 10)
    # y = np.random.randn(10, 10)
    ## Join to stop work
    # consumer_pool.join()
