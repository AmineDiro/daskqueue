"""
Benchmarking 1_000_000 tasks using :
    - 2 nodes
    - 1 thread per process
    - 4 queues
    - 8 consumers
The tasks were chunked using  into 1000 calls of 1000 tasks per batch
The client submits to the QueuePool manager using
The function is 'empty' : just passes and doesn't use CPU or IO

Processing 1_000_000 empty tasks took 338s = 5min36s
"""

import time
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Callable, Tuple

from daskqueue import ConsumerPool, QueuePool
from daskqueue.Protocol import Message
from daskqueue.utils import logger
from distributed import Client


def void_func():
    pass


def process_item():
    return sum(i * i for i in range(10**5))


def batch_submit(queue_pool, func: Callable):
    # queue_pool.submit(func, *args)
    queue_pool.batch_submit([(func,) for _ in range(100)])


if __name__ == "__main__":

    client = Client(
        address="tcp://192.168.1.92:8786"
        # n_workers=6,
        # threads_per_worker=1,
        # dashboard_address=":3338",
        # direct_to_workers=True,
        # n_workers=6,
        # threads_per_worker=1,
        # dashboard_address=":3338",
        # direct_to_workers=True,
    )

    client.restart()
    logger.info("Cluster restarted.")

    ## Params
    n_queues = 4
    n_consumers = 8
    n_calls = 1_000
    chunk = 1000

    queue_pool = QueuePool(client, n_queues)

    consumer_pool = ConsumerPool(client, queue_pool, n_consumers=n_consumers)

    tic = time.perf_counter()
    consumer_pool.start()

    submit_func = partial(batch_submit, queue_pool)

    with ThreadPoolExecutor(max_workers=20) as executor:
        _ = executor.map(submit_func, [void_func for _ in range(n_calls)])

    consumer_pool.join(timestep=1, progress=True)
    toc = time.perf_counter()

    print(f"Processed all {n_calls*chunk} in  {toc - tic:0.4f} seconds")
