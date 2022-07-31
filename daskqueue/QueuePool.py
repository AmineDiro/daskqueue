from ast import Call
import asyncio
import functools
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Tuple, TypeVar, Union

import numpy as np
from distributed import Client, worker_client
from distributed.worker import get_client

from daskqueue.utils import logger

from .Consumer import ConsumerBaseClass, GeneralConsumer
from .Protocol import Message
from .Queue import QueueActor

TConsumer = TypeVar("TConsumer", bound=ConsumerBaseClass)


class QueuePoolActor:
    """Utility class to operate on a fixed pool of queues.

    Arguments:
        queues (list): List of DaskActors to use in this pool.
    """

    def __init__(self, n_queues: List[Any]):
        # actors to be used
        self._client = get_client()
        try:
            self._queues = self.create_queues(n_queues)
        except Exception:
            raise RuntimeError("Couldn't create queues")
        self.n_queues = n_queues
        self._index_queue = {q.key: q for q in self._queues}
        self._queue_size = {q.key: 0 for q in self._queues}
        self.worker_class = GeneralConsumer

    def print(self) -> str:
        qsize = [
            f"\n\t{k}: {self._queue_size[k] if self._queue_size[k]>=0 else 0} pending items"
            for k in self._queue_size
        ]

        return f"QueuePool : \n\t{self.n_queues} queue(s)" + "".join(qsize)

    def get_queue(self, idx: int) -> QueueActor:
        return self._queues[idx]

    def get_len(self) -> int:
        return len(self._index_queue)

    def create_queues(self, n_queues):
        return [
            self._client.submit(QueueActor, actor=True).result()
            for _ in range(n_queues)
        ]

    def get_queue_size(self) -> Dict[str, int]:
        return self._queue_size

    def _get_random_queue(self) -> QueueActor:
        idx = np.random.randint(len(self._queues))
        return self._queues[idx]

    async def get_max_queue(self) -> QueueActor:
        key_queue = max(zip(self._queue_size.values(), self._queue_size.keys()))[1]
        self._queue_size[key_queue] -= 1
        return self._index_queue[key_queue]

    async def batch_submit(
        self,
        list_calls: List[Tuple[Callable, ...]],
        timeout=None,
        worker_class=GeneralConsumer,
        **kwargs,
    ):
        if not issubclass(worker_class, GeneralConsumer):
            raise RuntimeError(
                "Can't submit arbitrary tasks to arbitrary consumer. Please use the default GeneralConsumer class"
            )
        msgs = [Message(func, *args, **kwargs) for func, *args in list_calls]
        await self.put_many(msgs)

    async def submit(
        self,
        func: Callable,
        *args,
        timeout=None,
        worker_class=GeneralConsumer,
        **kwargs,
    ):
        if not issubclass(worker_class, GeneralConsumer):
            raise RuntimeError(
                "Can't submit arbitrary tasks to arbitrary consumer. Please use the default GeneralConsumer class"
            )
        msg = Message(func, *args, **kwargs)
        await self.put(msg, timeout=timeout)

    async def put(self, msg: Union[Message, Any], timeout=None) -> None:
        try:
            logger.debug(f"[QueuePool] Item put in queue: {msg}")
            q = self._get_random_queue()
            self._queue_size[q.key] += 1
            await asyncio.wait_for(q.put(msg), timeout)
        except asyncio.TimeoutError:
            pass

    def put_nowait(self, item: Any) -> None:
        q = self._get_random_queue()
        self._queue_size[q.key] += 1
        try:
            q.put_nowait(item).result()
        except asyncio.QueueFull:
            logger.debug("reRaise same error")
            raise asyncio.QueueFull

    async def put_many(self, list_items: List[Any]) -> None:
        q = self._get_random_queue()
        self._queue_size[q.key] += len(list_items)

        for item in list_items:
            logger.debug(f"Put in queue_pool : \n item {item}")

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

    def put_nowait_batch(self, items):
        pass
        # # If maxsize is 0, queue is unbounded, so no need to check size.
        # if self.maxsize > 0 and len(items) + self.qsize() > self.maxsize:
        #     pass
        # for item in items:
        #     self.queue.put_nowait(item)

    def get_nowait_batch(self, num_items):
        pass
        # if num_items > self.qsize():
        #     # raise Empty(
        #     #     f"Cannot get {num_items} items from queue of size " f"{self.qsize()}."
        #     # )
        #     pass
        # return [self.queue.get_nowait() for _ in range(num_items)]

        #     pass
        # return [self.queue.get_nowait() for _ in range(num_items)]


def decorator(cls):
    class Interface:
        """Interface class to communicate with the queue Pool actor spawned in the cluster."""

        def __init__(self, client, n_queues):
            self.actor = client.submit(cls, n_queues, actor=True).result()
            logger.info(f"Created {n_queues} queues in Cluster and one QueueManager.")

        def __getattr__(self, key):
            attr = getattr(self.actor, key)
            if callable(attr):

                @functools.wraps(attr)
                def func(*args, **kwargs):
                    return attr(*args, **kwargs).result()

                return func

        def __repr__(self) -> str:
            return self.actor.print().result()

        def __getitem__(self, idx: int) -> QueueActor:
            return self.actor.get_queue(idx).result()

        def __len__(self):
            return self.actor.get_len().result()

    return Interface


QueuePool = decorator(QueuePoolActor)
