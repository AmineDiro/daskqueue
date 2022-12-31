import queue
from multiprocessing import dummy

import pytest
from distributed import Client
from distributed.utils_test import cleanup, client, cluster_fixture, gen_cluster, loop

from daskqueue.Consumer import ConsumerBaseClass
from daskqueue.QueuePool import QueuePool, QueuePoolActor


@gen_cluster(client=True, cluster_dump_directory=False)
async def test_putmany_queuepool(c, s, a, b):
    n_queues = 2
    queue_pool = await c.submit(QueuePoolActor, n_queues, actor=True)
    res = await queue_pool.put_many([12, "skdfjs", 1213])
    assert None == res


def test_queue_pool_inteface_create(client):
    n_queues = 2
    queue_pool = QueuePool(client, n_queues)
    assert n_queues == len(queue_pool)
    assert 0 == sum(queue_pool.get_queue_size().values())
    assert queue_pool[0].qsize().result() == 0
    assert queue_pool[1].qsize().result() == 0
    with pytest.raises(IndexError) as e_info:
        _ = queue_pool[3]


def test_queuepool_inteface_put(client):
    n_queues = 1
    queue_pool = QueuePool(client, n_queues)
    _ = queue_pool.put(1)
    assert 1 == sum(queue_pool.get_queue_size().values())
    _ = queue_pool.put(1)
    _ = queue_pool.put(1)
    assert 3 == sum(queue_pool.get_queue_size().values())


def test_queuepool_inteface_put_many(client):
    n_queues = 1
    queue_pool = QueuePool(client, n_queues)
    _ = queue_pool.put_many([1, 2, 3])
    assert 3 == sum(queue_pool.get_queue_size().values())


def test_queuepool_inteface_submit(client):
    n_queues = 1
    queue_pool = QueuePool(client, n_queues)

    def dummy_func():
        pass

    _ = queue_pool.submit(dummy_func)
    assert 1 == sum(list(queue_pool.get_queue_size().values()))

    for _ in range(9):
        _ = queue_pool.submit(dummy_func)
    assert 10 == sum(list(queue_pool.get_queue_size().values()))


def test_queuepool_inteface_batch_submit(client):
    n_queues = 1
    queue_pool = QueuePool(client, n_queues)

    def dummy_func():
        pass

    _ = queue_pool.batch_submit([(dummy_func,) for _ in range(10)])
    assert 10 == sum(list(queue_pool.get_queue_size().values()))


def test_queuepool_inteface_submit_error(client):
    n_queues = 1
    queue_pool = QueuePool(client, n_queues)

    def dummy_func():
        pass

    class Worker(ConsumerBaseClass):
        pass

    with pytest.raises(RuntimeError) as e_info:
        _ = queue_pool.submit(dummy_func, worker_class=Worker)
