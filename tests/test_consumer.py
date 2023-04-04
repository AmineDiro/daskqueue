import logging
import time
from re import I

import pytest
from distributed import Client
from distributed.utils_test import gen_cluster

from daskqueue.Consumer import ConsumerBaseClass, DummyConsumer, GeneralConsumer
from daskqueue.queue.base_queue import Durability
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
            DummyConsumer,
            1,
            "test-consumer",
            "test",
            1,
            10000,
            1,
            True,
            workers=[a.address],
            actor=True,
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


@gen_cluster(
    client=True,
    cluster_dump_directory=False,
    clean_kwargs={"threads": False, "instances": True, "processes": False},
)
async def test_consummer_get_item(c, s, a, b):
    async with Client(s.address, asynchronous=True) as c:
        queue_pool = await c.submit(
            QueuePoolActor, 1, "transient", 1, False, actor=True
        )
        await queue_pool.put(1)
        consumer = await c.submit(
            DummyConsumer, 1, "test-consumer", queue_pool, 1, 1000, 1, False, actor=True
        )
        await consumer.start()
        assert await consumer.done() == False

        await consumer.cancel()
        assert await consumer.done() == True

        n_items = await consumer.len_items()
        assert n_items == 1


@gen_cluster(
    client=True,
    cluster_dump_directory=False,
    clean_kwargs={"threads": False, "instances": True, "processes": False},
)
async def test_consummer_get_items(c, s, a, b):
    async with Client(s.address, asynchronous=True) as c:
        queue_pool = await c.submit(QueuePoolActor, 1, actor=True)
        await queue_pool.put(1)
        await queue_pool.put(1)
        consumer = await c.submit(
            DummyConsumer, 1, "test-consumer", queue_pool, 1, 1000, 1, True, actor=True
        )
        await consumer.start()
        await consumer.cancel()
        assert await consumer.is_consumming() == False
        assert await consumer.done() == True
