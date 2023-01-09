import asyncio
import time
from queue import Empty, Full, Queue
from threading import Event, Thread
from typing import List, Optional, Tuple
from uuid import UUID

from distributed.worker import get_worker
from sortedcontainers import SortedDict

from daskqueue.Protocol import Message
from daskqueue.utils import logger

from .base_queue import BaseQueue, Durability


class TransientQueue(BaseQueue):
    def __init__(
        self,
        maxsize=-1,
        ack_timeout: int = 5,
        retry: bool = False,
    ):
        # If maxsize is less than or equal to zero, the queue size is infinite
        self.maxsize = maxsize
        # Get the IOLoop running on the worker
        self.queue = Queue(maxsize=maxsize)
        self.delivered = SortedDict()

        # Garbage collection tasks for delivered unacked message
        self.ack_timeout = ack_timeout
        self.retry = retry
        self.stop_gc_event = Event()
        self._gc_thread = Thread(target=self._background_gc, daemon=True)
        self._gc_thread.start()
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
        return self.queue.qsize() + len(self.delivered)

    def empty(self):
        return self.queue.empty()

    def full(self):
        return self.queue.full()

    async def put_many(self, list_items):
        for item in list_items:
            self.queue.put(item, block=False)

    async def put(self, item, timeout=None):
        logger.debug(f"[Queue] Put item: {item}")
        self.queue.put(item, timeout=timeout)

    def put_sync(self, item):
        return self.queue.put(item)

    def get_sync(self, timeout=None):
        try:
            item = self.queue.get(block=False)
            if isinstance(item, Message):
                item.delivered_timestamp = time.time()
                self.delivered[item.delivered_timestamp] = item
            return item
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

    async def ack(self, timestamp: float, msg_id: UUID):
        logger.debug(f"[Queue] Ack item {msg_id}")
        item = self.delivered.pop(timestamp, None)

        if item is None or item != msg_id:
            # raise ValueError("Msg doesnt exist in the delivered list")
            pass
        return True

    async def ack_many(self, items: List[Tuple[float, UUID]]):
        await asyncio.gather(*[self.ack(item[0], item[1]) for item in items])

    def _background_gc(self):
        while not self.stop_gc_event.is_set():
            now = time.time()
            cutoff = self.delivered.bisect_left(now)
            for record in self.delivered.keys()[:cutoff]:
                idx_record = self.delivered.pop(record)
                if self.retry:
                    self.ready[idx_record.msg_id] = idx_record
            time.sleep(self.ack_timeout)

    def stop_gc(self):
        self.stop_gc_event.set()
