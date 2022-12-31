from time import perf_counter

import click

from daskqueue import ConsumerPool
from daskqueue.Protocol import Message
from daskqueue.queue.base_queue import Durability
from daskqueue.queue.durable_queue import DurableQueue
from daskqueue.QueuePool import QueuePool

N = 1_000

cprint = click.echo


func_no_return = lambda: None


def process_item():
    return sum(i * i for i in range(10**2))


def rdx_msg():
    func = lambda x: x + 2
    msg = Message(func, 12)
    return msg


def test_durable_queue(durable_queue: DurableQueue):
    put_msgs = []
    get_msgs = []
    s = perf_counter()
    for _ in range(N):
        msg = rdx_msg()
        durable_queue.put_sync(msg)
        put_msgs.append(msg.id)

    e = perf_counter()

    w_ops = N / (e - s)  # op/s
    cprint("\n\t Mean write ops: " + click.style(f"{w_ops:.2f} wop/s", fg="green"))

    s = perf_counter()
    for _ in range(N):
        msg = durable_queue.get_sync()
        get_msgs.append(msg.id)
    e = perf_counter()
    r_ops = N / (e - s)  # op/s
    cprint("\n\t Mean read ops: " + click.style(f"{r_ops:.2f} rop/s", fg="green"))

    assert put_msgs == get_msgs


def test_durable_queuepool(client, tmp_path):
    n_queues = 1
    n_consumers = 2
    n_calls = 20

    queue_pool = QueuePool(
        client, n_queues, durability=Durability.DURABLE, dirpath=str(tmp_path)
    )
    consumer_pool = ConsumerPool(client, queue_pool, n_consumers=n_consumers)

    _ = queue_pool.batch_submit([(func_no_return,) for _ in range(n_calls)])

    assert n_calls == sum(list(queue_pool.get_queue_size().values()))

    tic = perf_counter()
    consumer_pool.start()
    consumer_pool.join(timestep=0.2, progress=True)
    toc = perf_counter()

    cprint(
        f"\n\tProcessed all {n_calls} in: "
        + click.style(f"{toc - tic:0.4f} seconds", fg="green")
    )
    res = consumer_pool.results()
    assert n_calls * [None] == [val for k in res for val in res[k].values()]
    assert False
