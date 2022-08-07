import asyncio
import functools
import itertools
from ast import Call
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Tuple, TypeVar, Union

import numpy as np
from distributed import Client, worker_client
from distributed.worker import get_client

from daskqueue.utils import logger
from daskqueue.utils.funcs import msg_grouper

from .Consumer import ConsumerBaseClass, GeneralConsumer
from .Protocol import Message
from .Queue import Full, QueueActor

TConsumer = TypeVar("TConsumer", bound=ConsumerBaseClass)


class PutTimeout(Exception):
    pass


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
        self._cycle_queues_put = itertools.cycle(self._queues)
        self._cycle_queues_get = itertools.cycle(self._queues)

        self.n_queues = n_queues
        self._index_queue = {q.key: q for q in self._queues}
        self._queue_size = {q.key: 0 for q in self._queues}
        self.worker_class = GeneralConsumer

    def print(self) -> str:
        qsize = [
            f"\n\t{idx}: {q.qsize().result()} pending items"
            for idx, q in self._index_queue.items()
        ]

        return f"QueuePool : {self.n_queues} queue(s)" + "".join(qsize)

    def get_len(self) -> int:
        return len(self._index_queue)

    def create_queues(self, n_queues):
        return [
            self._client.submit(QueueActor, actor=True).result()
            for _ in range(n_queues)
        ]

    def get_next_queue(self) -> QueueActor:
        return next(self._cycle_queues_get)

    async def get_queues(self) -> List[QueueActor]:
        return self._queues

    async def get_queue(self, idx: int) -> QueueActor:
        return self._queues[idx]

    def get_queue_size(self) -> Dict[str, int]:
        return {q: q.qsize().result() for q in self._queues}

    def _get_random_queue(self) -> QueueActor:
        idx = np.random.randint(len(self._queues))
        return self._queues[idx]

    # TODO : Don't need this ??
    async def get_max_queue(self) -> QueueActor:
        queues_size = self.get_queue_size()
        logger.info(f"queues_size : {queues_size}")
        size_max, q_max = max(zip(queues_size.values(), queues_size.keys()))
        if size_max == 0:
            return None
        return q_max

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

    async def batch_submit(
        self,
        list_calls: List[Tuple[Callable, ...]],
        timeout=None,
        worker_class=GeneralConsumer,
        **kwargs,
    ):
        """Batch submits a list of messages to the next put queue in pool.

        Args:
            list_calls (List[Tuple[Callable, ...]]): List of tasks Tuple[func, args] to submit
            timeout (_type_, optional): Optional timeout. Defaults to None.
            worker_class (_type_, optional): Submit is only available is using a subclass of GeneralConsumer class. Defaults to GeneralConsumer.

        Raises:
            RuntimeError: Exception if worker_class is not a subclass of GeneralConsumer
        """
        if not issubclass(worker_class, GeneralConsumer):
            raise RuntimeError(
                "Can't submit arbitrary tasks to arbitrary consumer. Please use the default GeneralConsumer class"
            )
        # msgs = [Message(func, *args, **kwargs) for func, *args in list_calls]

        put_tasks = []
        for msgs in msg_grouper(len(list_calls) // self.n_queues, list_calls):
            put_tasks.append(asyncio.create_task(self.put_many(msgs, timeout)))

        responses = await asyncio.gather(*put_tasks, return_exceptions=True)
        logger.info(responses)

    async def put(self, msg: Union[Message, Any], timeout=None) -> None:
        try:
            logger.debug(f"[QueuePool] Item put in queue: {msg}")
            q = next(self._cycle_queues_put)
            # TODO : delete this !
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

    async def put_many(self, list_items: List[Any], timeout=None) -> None:
        q = next(self._cycle_queues_put)
        try:
            await asyncio.wait_for(q.put_many(list_items), timeout)
        except asyncio.TimeoutError:
            ## TODO : implement canceling tasks  in Queue and QueuePool
            raise PutTimeout(f"Couldn't put all element in queue : {q}")

    def put_nowait_batch(self, items):
        pass
        # # If maxsize is 0, queue is unbounded, so no need to check size.
        # if self.maxsize > 0 and len(items) + self.qsize() > self.maxsize:
        #     pass
        # for item in items:
        #     self.queue.put_nowait(item)


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

        # def submit(
        #     self,
        #     func: Callable,
        #     *args,
        #     timeout=None,
        #     worker_class=GeneralConsumer,
        #     **kwargs,
        # ):
        #     if not issubclass(worker_class, GeneralConsumer):
        #         raise RuntimeError(
        #             "Can't submit arbitrary tasks to arbitrary consumer. Please use the default GeneralConsumer class"
        #         )
        #     msg = Message(func, *args, **kwargs)
        #     q = self.actor._get_random_queue().result()
        #     try:
        #         q.put(msg, timeout=timeout).result()
        #     except Full:
        #         logger.error(f"QueueActor {q} if full.")

    return Interface


QueuePool = decorator(QueuePoolActor)
