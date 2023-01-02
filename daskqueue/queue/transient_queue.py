import asyncio
from queue import Queue

from distributed.worker import get_worker

from daskqueue.queue.queue_exceptions import Empty, Full

from .base_queue import BaseQueue, Durability


class TransientQueue(BaseQueue):
    def __init__(self, maxsize=-1):
        # If maxsize is less than or equal to zero, the queue size is infinite
        self.maxsize = maxsize
        # Get the IOLoop running on the worker

        try:
            self.loop = self._io_loop.asyncio_loop
            asyncio.set_event_loop(self.loop)
            self.queue = asyncio.Queue(self.maxsize)
        except ValueError:
            self.loop = None
            self.queue = Queue(maxsize=maxsize)

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
            await self.queue.put(item)

    async def put(self, item, timeout=None):
        try:
            await asyncio.wait_for(self.queue.put(item), timeout)
        except asyncio.TimeoutError:
            raise Full

    def put_sync(self, item):
        return self.queue.put(item)

    def get_sync(self):
        return self.queue.get()

    async def get(self, timeout=None):
        try:
            return await asyncio.wait_for(self.queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None

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
