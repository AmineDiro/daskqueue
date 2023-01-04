import tempfile
from time import perf_counter

import click
import numpy as np
from distributed import Client, LocalCluster

from conftest import func
from daskqueue import ConsumerPool, Durability, QueuePool

N_TEST = 1
MAX_BYTES = 100 * int(1e6)  # 100 MB


cprint = click.echo
gprint = lambda s: click.style(s, fg="green")


def read_write_benchmark(
    client: Client,
    dirpath: str,
    N: int,
    n_queues: int,
    n_consumers: int,
    durability: bool,
    progress: bool,
    asynchronous: bool,
):

    if durability:
        queue_pool = QueuePool(
            client, n_queues, durability=Durability.DURABLE, dirpath=str(dirpath)
        )
    else:

        queue_pool = QueuePool(client, n_queues)

    consumer_pool = ConsumerPool(client, queue_pool, n_consumers=n_consumers)

    s = perf_counter()
    queue_pool.batch_submit([(func, 3) for _ in range(N)], async_mode=asynchronous)
    # for _ in range(N):
    #     queue_pool.submit(func_no_return)
    e = perf_counter()

    assert sum(list(queue_pool.get_queue_size().values())) == N
    wops = N / (e - s)

    s = perf_counter()
    consumer_pool.start()
    consumer_pool.join(timestep=0.01, progress=progress)
    consumer_pool.results()
    e = perf_counter()
    rps = N / (e - s)  # op/s

    return wops, rps


@click.command("cli", context_settings={"show_default": True})
@click.option("--durable/--transient", default=False)
@click.option("-v/-q", "--verbose", default=False, help="show queue/consumer progress")
@click.option("-N", "--ntasks", default=10000, help="Number of tasks to send.")
@click.option("--nqueues", default=1, help="Number of queue actors ")
@click.option("--nconsumers", default=1, help="Number of consumer actors.")
@click.option(
    "--sync/--async",
    default=True,
    help="Batch submission is asynchronous or synchrounous.",
)
def bench(durable, verbose, ntasks, nqueues, nconsumers, sync):
    w_ops = []
    r_ops = []

    cluster = LocalCluster(
        n_workers=10,
        threads_per_worker=1,
        dashboard_address=":3338",
        worker_dashboard_address=":0",
    )
    client = Client(cluster, direct_to_workers=True)

    with tempfile.TemporaryDirectory() as tmpdirname:
        t_wops, t_rops = read_write_benchmark(
            client, tmpdirname, ntasks, nqueues, nconsumers, durable, verbose, not sync
        )
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
