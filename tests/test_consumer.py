import logging
import time
from re import I

import pytest
from distributed import Client
from distributed.utils_test import gen_cluster

from daskqueue.Consumer import ConsumerBaseClass, DummyConsumer
from daskqueue.QueuePool import QueuePool, QueuePoolActor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,%(msecs)d %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)


@gen_cluster(cluster_dump_directory=False)
async def test_async_consumer_create(s, a, b):
    async with Client(s.address, asynchronous=True) as c:
        worker = c.submit(
            DummyConsumer, 1, "test-consumer", "test", workers=[a.address], actor=True
        )
        worker = await worker
        assert hasattr(worker, "get_items")
        assert hasattr(worker, "len_items")
        assert hasattr(worker, "start")
        assert hasattr(worker, "_consume")
        assert hasattr(worker, "cancel")
        assert hasattr(worker, "is_consumming")


def test_create_consumer_concrete():
    class Worker(ConsumerBaseClass):
        pass

    pool = "test"
    with pytest.raises(Exception) as e_info:
        worker = Worker(pool)
        print(f"{e_info}")


@gen_cluster(client=True, cluster_dump_directory=False)
async def test_consummer_get_item(c, s, a, b):
    async with Client(s.address, asynchronous=True) as c:
        queue_pool = await c.submit(QueuePoolActor, 1, actor=True)
        await queue_pool.put(1)
        consumer = await c.submit(
            DummyConsumer, 1, "test-consumer", queue_pool, actor=True
        )
        await consumer.start()
        res = await consumer.get_items()
        assert 1 == res[0]


@gen_cluster(client=True, cluster_dump_directory=False)
async def test_consummer_get_item(c, s, a, b):
    async with Client(s.address, asynchronous=True) as c:
        queue_pool = await c.submit(QueuePoolActor, 1, actor=True)
        await queue_pool.put(1)
        await queue_pool.put(1)
        consumer = await c.submit(
            DummyConsumer, 1, "test-consumer", queue_pool, actor=True
        )
        await consumer.start()
        assert await consumer.is_consumming() == True
        await consumer.cancel()
        assert await consumer.is_consumming() == False
