import asyncio
from queue import Empty, Full, Queue
from typing import List, Optional
from uuid import UUID

from distributed.worker import get_worker

from daskqueue.Protocol import Message

from .base_queue import BaseQueue, Durability


class TransientQueue(BaseQueue):
    def __init__(self, maxsize=-1):
        # If maxsize is less than or equal to zero, the queue size is infinite
        self.maxsize = maxsize
        # Get the IOLoop running on the worker

        self.queue = Queue(maxsize=maxsize)
        try:
            self.loop = self._io_loop.asyncio_loop
            asyncio.set_event_loop(self.loop)
        except ValueError:
            self.loop = None

        super().__init__(durability=Durability.TRANSIENT, maxsize=maxsize)

    @property
    def _worker(self):
        return get_worker()

    @property
    def _io_loop(self):
        if self._worker:
            return self._worker.io_loop

    def qsize(self):
        return self.queue.qsize()

    def empty(self):
        return self.queue.empty()

    def full(self):
        return self.queue.full()

    async def put_many(self, list_items):
        for item in list_items:
            self.queue.put(item, block=False)

    async def put(self, item, timeout=None):
        self.queue.put(item, timeout=timeout)

    def put_sync(self, item):
        return self.queue.put(item)

    def get_sync(self, timeout=None):
        try:
            return self.queue.get(block=False)
        except Empty:
            return None

    async def get(self, timeout=None):
        return self.get_sync(timeout)

    async def get_many(self, n: int, timeout=None) -> List[Optional[Message]]:
        return [self.get_sync(timeout) for _ in range(n)]

    def put_nowait(self, item):
        self.queue.put_nowait(item)

    def put_nowait_batch(self, items):
        # If maxsize is <=0, queue is unbounded, so no need to check size.
        if self.maxsize > 0 and len(items) + self.qsize() > self.maxsize:
            raise Full(
                f"Cannot add {len(items)} items to queue of size "
                f"{self.qsize()} and maxsize {self.maxsize}."
            )
        for item in items:
            self.queue.put_nowait(item)

    def get_nowait(self):
        try:
            return self.queue.get_nowait()
        except asyncio.QueueEmpty:
            return None

    def get_nowait_batch(self, num_items):
        if num_items > self.qsize():
            raise Empty(
                f"Cannot get {num_items} items from queue of size " f"{self.qsize()}."
            )
        return [self.queue.get_nowait() for _ in range(num_items)]

    async def ack(msg_id: UUID):
        pass
