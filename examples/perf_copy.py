import os
import argparse
import asyncio
import shutil
from typing import List, Tuple
import uuid

import numpy as np
from distributed import Client, Queue

from daskqueue import ConsumerPool, QueuePool, ConsumerBaseClass
from daskqueue.utils import logger


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
        logger.info(item)
        # shutil.copy(src, dst)


def main():

    client = Client(
        n_workers=5,
        threads_per_worker=1,
        dashboard_address=":3338",
        direct_to_workers=True,
    )

    ## Params
    start_dir = "/home/amine/Documents"
    dst_dir = "/home/amine/Documents"
    n_queues = 1
    n_consumers = 2

    # Queue Pool  with basic load balancing
    queue_pool = QueuePool(client, n_queues)

    # Start Consummers
    consumer_pool = ConsumerPool(client, queue_pool, CopyWorker, n_consumers)
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
