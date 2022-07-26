import pytest
from distributed import Client
from daskqueue import QueuePool, QueueActor


class TestQueue:
    value = 0
    client = Client(address="tcp://127.0.0.1:43041")
    client.restart()

    def test_create_queue(self):
        queue = self.client.submit(QueueActor, actor=True).result()
        assert 1 == 1

    def test_getnowait_from_empty_queue(self):
        queue = self.client.submit(QueueActor, actor=True).result()
        # res = queue_pool.get(timeout=1).result()
        res = queue.get_nowait().result()
        assert None == res

    def test_get_from_empty_queue(self):
        queue = self.client.submit(QueueActor, actor=True).result()
        res = queue.get(timeout=1).result()
        assert res == None

    def test_put_in_queue(self):
        queue = self.client.submit(QueueActor, actor=True).result()
        res = queue.put(1).result()
        assert res == None

    def test_put_get_in_queue(self):
        queue = self.client.submit(QueueActor, actor=True).result()
        res_put = queue.put(1).result()
        res_get = queue.get(timeout=1).result()
        assert res_get == 1

    def test_put_getnowait_in_queue(self):
        queue = self.client.submit(QueueActor, actor=True).result()
        res_put = queue.put(1).result()
        res_get = queue.get_nowait().result()
        assert res_get == 1
