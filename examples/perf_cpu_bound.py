import queue
import time
import os
from distributed import Client
from daskqueue import QueuePool, ConsumerPool
from daskqueue.utils import logger


def inc(x):
    return x + 1


def process_item():
    return sum(i * i for i in range(10**8))


if __name__ == "__main__":

    client = Client(
        n_workers=10,
        threads_per_worker=1,
        dashboard_address=":3338",
        direct_to_workers=True,
    )

    ## Params
    n_queues = 10
    n_consumers = 20

    queue_pool = QueuePool(client, n_queues)

    consumer_pool = ConsumerPool(client, queue_pool, n_consumers=n_consumers)

    consumer_pool.start()

    ## Perf count
    s = time.perf_counter()

    queue_pool.batch_submit([(inc, 1) for _ in range(10000)])

    print(queue_pool)

    e = time.perf_counter()

    print("===============================================")
    logger.info(f"Submitted all items in {e-s:.2f}s\n")
    print("===============================================")

    consumer_pool.join(timestep=0.01, print_timestep=2, progress=True)

    e = time.perf_counter()

    print("===============================================")
    logger.info(f"\n\nProcessed all items in {e-s:.2f}s")
    print("===============================================")
    print(queue_pool)
    print(consumer_pool)
