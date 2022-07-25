import time 
import pytest
from distributed import Client, LocalCluster, Actor
from daskqueue import QueuePool,  ConsumerBaseClass
import logging

from daskqueue.Consumer import DummyConsumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,%(msecs)d %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)


    
class TestConsummer:
    client = Client()
    client.restart()
    time.sleep(2)

    queue_pool = client.submit(QueuePool,1, actor=True).result()
 

    def test_create_concrete_process(self):
        class Worker(ConsumerBaseClass):
            pass 
        pool = 'test'
        with pytest.raises(Exception) as e_info:
            worker = Worker(pool)

    def test_consummer_get_item(self):
        self.queue_pool.put(1)
        consumer = self.client.submit(DummyConsumer, self.queue_pool, actor=True).result()
        consumer.start()
        # assert consumer.len_items().result() == 0
        time.sleep(2)
        res = consumer.get_items().result()[0]
        assert 1 == res


    # def test_cancel_consumer(self):
    #     consumer = self.client.submit(DummyConsumer, self.queue_pool, actor=True).result()
    #     consumer.start()
    #     consumer.cancel()


