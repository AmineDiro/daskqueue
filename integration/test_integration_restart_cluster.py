import click

from daskqueue.ConsumerPool import ConsumerPool
from daskqueue.queue.base_queue import Durability
from daskqueue.QueuePool import QueuePool

cprint = click.echo


def func_no_return():
    return None


def process_item():
    return sum(i * i for i in range(10**2))


def test_durable_restart_cluster(tmp_path, client):
    n_queues = 1
    n_consumers = 1

    queue_pool = QueuePool(
        client, n_queues, durability=Durability.DURABLE, dirpath=str(tmp_path)
    )

    for _ in range(10):
        queue_pool.submit(func_no_return)

    # Other queue pool
    queue_pool = QueuePool(
        client, n_queues, durability=Durability.DURABLE, dirpath=str(tmp_path)
    )

    consumer_pool = ConsumerPool(
        client,
        queue_pool=queue_pool,
        n_consumers=n_consumers,
        batch_size=10,
    )
    consumer_pool.start()
    consumer_pool.join(0.1, progress=True)
    res = consumer_pool.results()

    assert 10 * [None] == [val for k in res for val in res[k].values()]
