import os
import argparse
import asyncio
import shutil
from typing import List, Tuple
import uuid

import numpy as np
from distributed import Client, Queue

from daskqueue import QueueActor, Consumer, QueuePool


def get_random_msg(
    start_dir: str, l_files: List[str], size: int
) -> List[Tuple[str, str]]:
    msg = []
    for _ in range(size):
        idx = np.random.randint(len(l_files))
        src = os.path.join(start_dir, l_files[idx])
        dst = os.path.join(dst_dir, str(uuid.uuid1()))
        msg.append((src, dst))
    return msg


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="")
    parser.add_argument(
        "-s",
        "--scheduler_address",
        type=str,
        help="LSF Scheduler address, if None we will use Local Cluster",
    )
    args = parser.parse_args()
    client = Client(address=args.scheduler_address)

    start_dir = ""
    dst_dir = ""
    n_queues = 5  # +1 queue pool thread
    n_consummers = 20

    # Queue Pool  with basic load balancing
    queue_pool = client.submit(QueuePool, n_queues, actor=True).result()

    # Consummers
    consummers = []
    for _ in range(n_consummers):
        f_consummer = client.submit(Consumer, queue_pool, actor=True)
        consummer = f_consummer.result()
        consummers.append(consummer)
        consummer.start()

    # Parallel file copy
    l_files = os.listdir(start_dir)
    for _ in range(10):
        msg = get_random_msg(start_dir, l_files, size=1000)
        queue_pool.put_many(msg)
