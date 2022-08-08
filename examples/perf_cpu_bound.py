import time
import os
from distributed import Client
from daskqueue import QueuePool, ConsumerPool
from daskqueue.utils import logger


def process_item():
    return sum(i * i for i in range(10**8))


def slowinc(x, delay):
    time.sleep(delay)
    return x + 1
    # return sum(i * i for i in range(10**8))


if __name__ == "__main__":

    n_queues = 10
    n_consumers = 100
    n_calls = 10000
    delay = 0.01

    client = Client(
        n_workers=10,
        threads_per_worker=1,
        dashboard_address=":3338",
        # address="tcp://192.168.1.92:8786",
        direct_to_workers=True,
    )

    queue_pool = QueuePool(client, n_queues=n_queues)

    consumer_pool = ConsumerPool(
        client,
        queue_pool,
        n_consumers=n_consumers,
        max_concurrency=10000,
    )

    tic = time.perf_counter()
    queue_pool.batch_submit([(slowinc, 1, delay) for _ in range(n_calls)])

    toc = time.perf_counter()

    print(f"Submit all items in  {toc - tic:0.4f} seconds")
    consumer_pool.start(timeout=1)
    consumer_pool.join(timestep=0.001, print_timestep=1, progress=True)

    toc = time.perf_counter()

    print(queue_pool)
    print(consumer_pool)
    print(f"Processed all items in  {toc - tic:0.4f} seconds")

    cqueue = [c.get_current_queue().result().key for c in consumer_pool]
    from collections import Counter

    count = Counter(cqueue)
