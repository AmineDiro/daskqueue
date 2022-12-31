import tempfile
from time import perf_counter

import click
from distributed import Client

from daskqueue import ConsumerPool, Durability, QueuePool

# from tqdm import tqdm


cprint = click.echo


func_no_return = lambda: None


def read_write_benchmark(client: Client, dirpath: str, N: int):
    n_queues = 1
    n_consumers = 2

    queue_pool = QueuePool(
        client, n_queues, durability=Durability.DURABLE, dirpath=str(dirpath)
    )

    consumer_pool = ConsumerPool(client, queue_pool, n_consumers=n_consumers)
    s = perf_counter()
    # _ = queue_pool.batch_submit([(func_no_return,) for _ in range(N)])
    for _ in range(N):
        queue_pool.submit(func_no_return)
    e = perf_counter()

    bs_ops = N / (e - s)

    cprint(
        "Batch submitted throughput : "
        + click.style(f"{bs_ops:.4f} seconds", fg="green")
    )

    consumer_pool.start()
    consumer_pool.join(timestep=0.2, progress=True)
    consumer_pool.results()
    return bs_ops


if __name__ == "__main__":

    # w_ops = []
    # w_mbps = []

    # for i in tqdm(range(N_TEST)):
    #         if i == 0:
    #             print("Temp dir :", tmpdirname)
    #         log = log_segment(tmpdirname)
    #         idx = index_segment(tmpdirname)
    #         t_wmbps, t_wops = write_log(N, idx, log)
    #         w_ops.append(t_wops)
    #         w_mbps.append(t_wmbps)

    # w_ops = np.array(w_ops)
    # w_mbps = np.array(w_mbps)

    # print(f"Mean write ops [{N_TEST}tests] {w_ops.mean():.2f} wop/s")
    # print(f"Mean write mps [{N_TEST}tests] {w_mbps.mean():.2f} mb/s")

    N = 100
    N_TEST = 10
    MAX_BYTES = 100 * int(1e6)  # 100 MB

    client = Client(
        n_workers=6,
        threads_per_worker=1,
        dashboard_address=":3338",
    )

    with tempfile.TemporaryDirectory() as tmpdirname:
        read_write_benchmark(client, tmpdirname, N)

    client.close()
    client.shutdown()
