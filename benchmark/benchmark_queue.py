import tempfile
from time import perf_counter

import click
import numpy as np
from tqdm import tqdm

from daskqueue.Protocol import Message
from daskqueue.queue import BaseQueue, DurableQueue, TransientQueue

N = 10_000
N_TEST = 10
MAX_BYTES = 100 * int(1e6)  # 100 MB


cprint = click.echo
gprint = lambda s: click.style(s, fg="green")


def func(x):
    return x + 2


def rdx_msg():
    msg = Message(func, 12)
    return msg


def read_write_queue(N: int, queue: BaseQueue):
    s = perf_counter()
    for _ in range(N):
        msg = rdx_msg()
        queue.put_sync(msg)
    e = perf_counter()
    wps = N / (e - s)  # op/s

    assert queue.qsize() == N

    s = perf_counter()
    for _ in range(N):
        _ = queue.get_sync()
    e = perf_counter()
    rps = N / (e - s)  # op/s

    return wps, rps


@click.command()
@click.option("--durable/--transient", default=False)
def bench(durable):
    w_ops = []
    r_ops = []

    for i in tqdm(range(N_TEST)):
        with tempfile.TemporaryDirectory() as tmpdirname:

            if durable:
                queue = DurableQueue(name="queue-0", dirpath=str(tmpdirname))
            else:
                queue = TransientQueue()

            t_wops, t_rops = read_write_queue(N, queue)

            w_ops.append(t_wops)
            r_ops.append(t_rops)

    w_ops = np.array(w_ops)
    r_ops = np.array(r_ops)

    cprint(f"Mean write ops [{N_TEST}tests] " + gprint(f"{w_ops.mean():.2f} wop/s"))
    cprint(f"Mean read ops [{N_TEST}tests] " + gprint(f"{r_ops.mean():.2f} rop/s"))


if __name__ == "__main__":
    bench()
