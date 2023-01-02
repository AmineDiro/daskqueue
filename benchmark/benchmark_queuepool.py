import tempfile
from time import perf_counter

import click
import numpy as np
from distributed import Client, LocalCluster

from daskqueue import ConsumerPool, Durability, QueuePool

N = 200_000
N_TEST = 1
MAX_BYTES = 100 * int(1e6)  # 100 MB
n_queues = 1
n_consumers = 1

cprint = click.echo
gprint = lambda s: click.style(s, fg="green")

func = lambda x: x + 2

func_no_return = lambda: None


def read_write_benchmark(
    client: Client, dirpath: str, N: int, durability: bool, progress: bool
):

    if durability:
        queue_pool = QueuePool(
            client, n_queues, durability=Durability.DURABLE, dirpath=str(dirpath)
        )
    else:

        queue_pool = QueuePool(client, n_queues)

    consumer_pool = ConsumerPool(client, queue_pool, n_consumers=n_consumers)

    s = perf_counter()
    _ = queue_pool.batch_submit([(func_no_return,) for _ in range(N)])
    # for _ in range(N):
    #     queue_pool.submit(func_no_return)
    e = perf_counter()

    wops = N / (e - s)

    s = perf_counter()
    consumer_pool.start()
    consumer_pool.join(timestep=0.1, progress=progress)
    consumer_pool.results()
    e = perf_counter()
    rps = N / (e - s)  # op/s

    return wops, rps


@click.command()
@click.option("--durable/--transient", default=False)
@click.option("--progress/--no-progress", default=False)
def bench(durable, progress):
    w_ops = []
    r_ops = []

    cluster = LocalCluster(
        n_workers=10,
        threads_per_worker=1,
        dashboard_address=":3338",
        worker_dashboard_address=":8787",
    )
    client = Client(cluster, direct_to_workers=True)

    with tempfile.TemporaryDirectory() as tmpdirname:
        t_wops, t_rops = read_write_benchmark(client, tmpdirname, N, durable, progress)
        w_ops.append(t_wops)
        r_ops.append(t_rops)

    w_ops = np.array(w_ops)
    r_ops = np.array(r_ops)

    cprint(f"Mean write ops [{N_TEST}tests] " + gprint(f"{w_ops.mean():.2f} wop/s"))
    cprint(f"Mean read ops [{N_TEST}tests] " + gprint(f"{r_ops.mean():.2f} rop/s"))

    client.close()
    cluster.close()


if __name__ == "__main__":
    bench()
