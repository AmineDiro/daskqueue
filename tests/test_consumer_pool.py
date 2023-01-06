from distributed.utils_test import cleanup, client, cluster_fixture, gen_cluster, loop

from daskqueue.ConsumerPool import ConsumerPool
from daskqueue.QueuePool import QueuePool

add = lambda x, y: x + y
func_no_return = lambda: None


def test_consumer_pool_create(client):
    n_queues = 1
    queue_pool = QueuePool(client, n_queues)
    n_consumers = 2
    consumer_pool = ConsumerPool(client, queue_pool=queue_pool, n_consumers=n_consumers)

    assert hasattr(consumer_pool, "start")
    assert hasattr(consumer_pool, "join")
    assert hasattr(consumer_pool, "cancel")
    assert hasattr(consumer_pool, "results")
    assert hasattr(consumer_pool, "nb_consumed")

    assert 2 == len(consumer_pool)


def test_consumer_pool_submit_pure(client):
    n_queues = 1
    queue_pool = QueuePool(client, n_queues)

    n_consumers = 2
    consumer_pool = ConsumerPool(client, queue_pool=queue_pool, n_consumers=n_consumers)

    for _ in range(10):
        queue_pool.submit(add, 1, 1)

    consumer_pool.start()
    consumer_pool.join()
    res = consumer_pool.results()
    assert 10 * [2] == [val for k in res for val in res[k].values()]
    assert sum(queue_pool.get_queue_size().values()) <= 0


def test_consumer_pool_submit_noreturn(client):
    n_queues = 1
    queue_pool = QueuePool(client, n_queues)
    n_consumers = 10

    consumer_pool = ConsumerPool(
        client, queue_pool=queue_pool, n_consumers=n_consumers, batch_size=1
    )
    for _ in range(10):
        queue_pool.submit(func_no_return)

    consumer_pool.start()
    consumer_pool.join()
    res = consumer_pool.results()
    assert 10 * [None] == [val for k in res for val in res[k].values()]


def test_consumer_pool_ack_late(client):
    n_queues = 1
    n_consumers = 1
    queue_pool = QueuePool(client, n_queues)

    consumer_pool = ConsumerPool(
        client,
        queue_pool=queue_pool,
        n_consumers=n_consumers,
        batch_size=1,
        early_ack=False,
    )
    for _ in range(10):
        queue_pool.submit(func_no_return)

    consumer_pool.start()
    consumer_pool.join(0.1, progress=True)
    res = consumer_pool.results()

    assert 10 * [None] == [val for k in res for val in res[k].values()]
