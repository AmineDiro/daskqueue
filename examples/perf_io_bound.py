import queue
import time
import pytest
from distributed import Client, LocalCluster, Actor
from daskqueue import QueuePool, ConsumerBaseClass
import logging


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,%(msecs)d %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)

class IOConsumer(ConsumerBaseClass):
    def process_item(self,item):
        # File operations (such as logging) can block the
        # event loop: run them in a thread pool.
        logging.info(f"Processing {item}")
        with open('/dev/urandom', 'rb') as f:
            return f.read(100)

if __name__ == "__main__":
    client = Client(address="tcp://127.0.0.1:39581")
    client.restart()
    time.sleep(2)

    queue_pool = client.submit(QueuePool, 1, actor=True).result()

    n_consumers = 5
    consumers = [
        client.submit(IOConsumer, queue_pool, actor=True).result() for _ in range(5)
    ]

    [c.start() for c in consumers]

    for i in range(10):
        queue_pool.put_many(list(range(1000)))
