import os
import argparse
import asyncio
import shutil
from typing import List, Tuple
import uuid

import numpy as np
from distributed import Client, Queue

from daskqueue import ConsumerPool, QueuePool, ConsumerBaseClass


def get_random_msg(
    start_dir: str, dst_dir: str, l_files: List[str], size: int
) -> List[Tuple[str, str]]:
    """Generate a random copy list.
    Will randomly choose a file in `start_dir` and create a copy.

    Args:
        start_dir (str): start directory
        dst_dir(str): destination directory
        l_files (List[str]): list of paths in directory
        size (int): output size

    Returns:
        List[Tuple[str, str]]: _description_
    """
    msg = []
    for _ in range(size):
        idx = np.random.randint(len(l_files))
        src = os.path.join(start_dir, l_files[idx])
        dst = os.path.join(dst_dir, str(uuid.uuid1()))
        msg.append((src, dst))
    return msg


class CopyWorker(ConsumerBaseClass):
    ## You should always implement a concrete `process_item` where you define your processing code.
    # Take a look at the Implementation Section
    def process_item(self, item):
        src, dst = item
        shutil.copy(src, dst)


def main():
    parser = argparse.ArgumentParser(description="")
    parser.add_argument(
        "-s",
        "--scheduler_address",
        type=str,
        help="LSF Scheduler address, if None we will use Local Cluster",
    )
    parser.add_argument(
        "-w",
        "--n_workers",
        type=str,
        help="LSF Scheduler address, if None we will use Local Cluster",
    )
    parser.add_argument(
        "-q",
        "--n_queues",
        type=str,
        help="LSF Scheduler address, if None we will use Local Cluster",
    )
    args = parser.parse_args()

    client = Client(address=args.scheduler_address)

    ## Params
    start_dir = ""
    dst_dir = ""
    n_queues = args.n_queues  # +1 queue pool thread
    n_consumers = args.n_workers

    # Queue Pool  with basic load balancing
    queue_pool = client.submit(QueuePool, n_queues, actor=True).result()

    # Start Consummers
    consumer_pool = ConsumerPool(client, CopyWorker, n_consumers, queue_pool)
    consumer_pool.start()

    # Put copy Msg
    l_files = os.listdir(start_dir)

    for _ in range(10):
        msg = get_random_msg(start_dir, dst_dir, l_files, size=1000)
        queue_pool.put_many(msg)

    ## Join to stop work
    consumer_pool.join()


if __name__ == "__main__":
    main()
