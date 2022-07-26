from typing import Any, List
import numpy as np
from distributed.worker import get_client
import asyncio
from .Queue import QueueActor


class QueuePool:
    """Utility class to operate on a fixed pool of queues.

    Arguments:
        queues (list): List of DaskActors to use in this pool.
    """

    def __init__(self, n_queues: List[Any]):
        # actors to be used
        self._client = get_client()
        self._queues = self.create_queues(n_queues)
        self._index_queue = {q.key: q for q in self._queues}
        self._queue_size = {q.key: 0 for q in self._queues}

    def create_queues(self, n_queues):
        return [
            self._client.submit(QueueActor, actor=True).result()
            for _ in range(n_queues)
        ]

    def _get_random_queue(self) -> QueueActor:
        idx = np.random.randint(len(self._queues))
        return self._queues[idx]

    async def get_max_queue(self) -> QueueActor:
        key_queue = max(zip(self._queue_size.values(), self._queue_size.keys()))[1]
        self._queue_size[key_queue] -= 1
        return self._index_queue[key_queue]

    async def put(self, item, timeout=None):
        try:
            q = self._get_random_queue()
            self._queue_size[q.key] += 1
            await asyncio.wait_for(q.put(item), timeout)
        except asyncio.TimeoutError:
            pass

    def put_nowait(self, item: Any) -> None:
        q = self._get_random_queue()
        self._queue_size[q.key] += 1
        q.put_nowait(item)

    def put_nowait_batch(self, items):
        pass
        # # If maxsize is 0, queue is unbounded, so no need to check size.
        # if self.maxsize > 0 and len(items) + self.qsize() > self.maxsize:
        #     pass
        # for item in items:
        #     self.queue.put_nowait(item)

    async def put_many(self, list_items: List[Any]) -> None:
        q = self._get_random_queue()
        self._queue_size[q.key] += len(list_items)
        await q.put_many(list_items)

    async def get(self, timeout=None):
        try:
            q = await self.get_max_queue()
            return await q.get(timeout)
        except asyncio.TimeoutError:
            # TODO : Reincrement the q , no item retrieved
            return None

    async def get_nowait(self):
        try:
            q = await self.get_max_queue()
            return await q.get_nowait()
        except asyncio.QueueEmpty:
            return None

    def get_nowait_batch(self, num_items):
        pass
        # if num_items > self.qsize():
        #     # raise Empty(
        #     #     f"Cannot get {num_items} items from queue of size " f"{self.qsize()}."
        #     # )
        #     pass
        # return [self.queue.get_nowait() for _ in range(num_items)]
