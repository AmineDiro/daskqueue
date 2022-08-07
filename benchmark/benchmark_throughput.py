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

import itertools
import os
import time
from typing import Callable

import pandas as pd
from daskqueue import ConsumerPool, QueuePool
import daskqueue
from daskqueue.utils import logger
from distributed import Client, LocalCluster
from datetime import datetime


def save_results(params):
    result_path = "/home/amine/Documents/programming/dask-queue/benchmark/results.csv"
    if os.path.exists(result_path):
        df = pd.read_csv(result_path)
        df = df.append(params, ignore_index=True)
        df.to_csv(result_path, index=False)
    else:
        df = pd.DataFrame([params])
        df.to_csv(result_path, index=False)


def batch_submit(queue_pool, func: Callable):
    # queue_pool.submit(func, *args)
    queue_pool.batch_submit([(func,) for _ in range(100)])


def slowinc(x, delay=0.1):
    time.sleep(delay)
    return x + 1


def run(params):
    cluster = LocalCluster(
        n_workers=4,
        threads_per_worker=params["thread_per_consumer"],
        dashboard_address=":3338",
    )
    client = Client(
        cluster,
        direct_to_workers=True,
    )

    queue_pool = QueuePool(client, params["n_queues"])

    consumer_pool = ConsumerPool(
        client,
        queue_pool,
        n_consumers=params["n_consumers"],
        max_concurrency=params["max_concurrency"],
    )

    tic = time.perf_counter()
    consumer_pool.start(timeout=1)

    for _ in range(params["n_calls"]):
        queue_pool.submit(slowinc, 1, params["task_duration"])

    consumer_pool.join(timestep=0.001)
    toc = time.perf_counter()

    print(f"Processed all items in  {toc - tic:0.4f} seconds")

    params["job_time"] = toc - tic
    params["version"] = str(daskqueue.__version__)
    params["timestamp"] = datetime.now()

    save_results(params)
    client.close()
    cluster.close()


if __name__ == "__main__":
    ## Params
    n_queues = [1, 2]
    n_consumers = [10]
    thread_per_consumer = [1]
    max_concurrency = [1]
    task_duration = [0.1]
    task_release_GIL = [False]
    n_calls = [10000, 20000]

    list = [
        n_queues,
        n_consumers,
        thread_per_consumer,
        max_concurrency,
        task_duration,
        task_release_GIL,
        n_calls,
    ]

    combinations = [p for p in itertools.product(*list)]
    combination = pd.DataFrame(
        combinations,
        columns=(
            "n_queues",
            "n_consumers",
            "thread_per_consumer",
            "max_concurrency",
            "task_duration",
            "task_release_GIL",
            "n_calls",
        ),
    )
    df = combination[(combination.n_queues <= combination.n_consumers)]
    df = df[((df.n_calls >= 1000) & (df.n_consumers > 2)) | ((df.n_calls < 1000))]

    for _, params in df.iterrows():
        logger.info(f"Run params :{params}")
        run(params=params)
