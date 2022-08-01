"""
Benchmarking 1_000_000 tasks using :
    - 2 nodes
    - 1 thread per process
    - 4 queues
    - 8 consumers
You can't submit 1_000_000 tasks using client.submit.
We use launching  tasks from tasks : submit 1000 tasks each spawned 1000 task.
The function is 'empty' : just passes and doesn't use CPU or IO

The scheduler came to a halt !
"""

import time
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Callable, Tuple

from daskqueue import ConsumerPool, QueuePool
from daskqueue.Protocol import Message
from daskqueue.utils import logger
from distributed import Client, worker_client


def void_func():
    pass


def process_item():
    return sum(i * i for i in range(10**5))


def batch_submit(chunk):
    res = []
    with worker_client() as client:
        for _ in range(chunk):
            res.append(client.submit(void_func, pure=False))

        return client.gather(res)


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

    tic = time.perf_counter()
    futures = []

    for _ in range(n_calls):
        futures.append(client.submit(batch_submit, chunk, pure=False))

    res = client.gather(futures)
    toc = time.perf_counter()

    print(f"Processed all {n_calls*chunk} in  {toc - tic:0.4f} seconds")
