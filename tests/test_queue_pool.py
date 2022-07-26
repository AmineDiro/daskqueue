import pytest
from distributed import Client
from daskqueue import QueuePool

from distributed.utils_test import gen_cluster


@gen_cluster(client=True, cluster_dump_directory=False)
async def test_getnowait_from_empty_queue(c, s, a, b):
    n_queues = 2
    queue_pool = await c.submit(QueuePool, n_queues, actor=True)
    res = await queue_pool.get_nowait()
    assert None == res


@gen_cluster(client=True, cluster_dump_directory=False)
async def test_get_from_empty_queue(c, s, a, b):
    n_queues = 2
    queue_pool = await c.submit(QueuePool, n_queues, actor=True)
    res = await queue_pool.get(timeout=1)
    assert None == res


@gen_cluster(client=True, cluster_dump_directory=False)
async def test_put_queuepool(c, s, a, b):
    n_queues = 2
    queue_pool = await c.submit(QueuePool, n_queues, actor=True)
    res = await queue_pool.put(12)
    assert None == res


@gen_cluster(client=True, cluster_dump_directory=False)
async def test_putmany_queuepool(c, s, a, b):
    n_queues = 2
    queue_pool = await c.submit(QueuePool, n_queues, actor=True)
    res = await queue_pool.put_many([12, "skdfjs", 1213])
    assert None == res
