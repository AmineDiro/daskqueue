import pytest
from distributed import Client
from distributed.utils_test import gen_cluster

from daskqueue import QueueActor, QueuePool
from daskqueue.queue.transient_queue import Full


@gen_cluster(client=True, cluster_dump_directory=False)
async def test_create_queue(c, s, a, b):
    queue = await c.submit(QueueActor, actor=True)
    assert hasattr(queue, "qsize")
    assert hasattr(queue, "empty")
    assert hasattr(queue, "full")
    assert hasattr(queue, "put_many")
    assert hasattr(queue, "put")
    assert hasattr(queue, "put_nowait")
    assert hasattr(queue, "put_nowait_batch")
    assert hasattr(queue, "get")
    assert hasattr(queue, "get_nowait")
    assert hasattr(queue, "get_nowait_batch")


@gen_cluster(client=True, cluster_dump_directory=False)
async def test_getnowait_from_empty_queue(c, s, a, b):
    queue = await c.submit(QueueActor, actor=True)
    res = await queue.get_nowait()
    assert None == res


@gen_cluster(client=True, cluster_dump_directory=False)
async def test_get_from_empty_queue(c, s, a, b):
    queue = await c.submit(QueueActor, actor=True)
    res = await queue.get(timeout=1)
    assert res == None


@gen_cluster(client=True, cluster_dump_directory=False)
async def test_put_in_queue(c, s, a, b):
    queue = await c.submit(QueueActor, actor=True)
    res = await queue.put(1)
    assert res == None
    assert await queue.qsize() == 1


@gen_cluster(client=True, cluster_dump_directory=False)
async def test_put_get_in_queue(c, s, a, b):
    queue = await c.submit(QueueActor, actor=True)
    await queue.put(1)
    res_get = await queue.get(timeout=1)
    assert res_get == 1


@gen_cluster(client=True, cluster_dump_directory=False)
async def test_getnowait_in_queue(c, s, a, b):
    queue = await c.submit(QueueActor, actor=True)
    await queue.put(1)
    res_get = await queue.get_nowait()
    assert res_get == 1


@gen_cluster(client=True, cluster_dump_directory=False)
async def test_put_limit_queue(c, s, a, b):
    queue = await c.submit(QueueActor, maxsize=1, actor=True)
    _ = await queue.put(1, timeout=1)
    with pytest.raises(Full) as e_info:
        _ = await queue.put(1, timeout=1)
        print(f"{e_info}")
