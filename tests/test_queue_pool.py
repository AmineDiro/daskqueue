import pytest
from distributed import Client
from daskqueue import QueuePool

# @pytest.mark.asyncio
# async def test_get_null_queue():
#     client = Client(address="tcp://127.0.0.1:42577")
#     client.restart()
#     n_queues = 2
#     queue_pool = client.submit(QueuePool,n_queues, actor=True).result()
#     # res = queue_pool.get(timeout=1).result()
#     res = queue_pool.get_nowait().result()
#     client.restart()
#     assert None == res

client = Client(address="tcp://127.0.0.1:42577")


def test_getnowait_from_empty_queue():
    client.restart()
    n_queues = 2
    queue_pool = client.submit(QueuePool, n_queues, actor=True).result()
    # res = queue_pool.get(timeout=1).result()
    res = queue_pool.get_nowait().result()
    assert None == res


def test_get_from_empty_queue():
    client.restart()
    n_queues = 2
    queue_pool = client.submit(QueuePool, n_queues, actor=True).result()
    # res = queue_pool.get(timeout=1).result()
    res = queue_pool.get(timeout=1).result()
    assert None == res