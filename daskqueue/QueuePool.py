import asyncio
import functools
import itertools
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, List, Tuple, TypeVar, Union

import numpy as np
from distributed.worker import get_client, get_worker

from daskqueue.queue import BaseQueue, TransientQueue
from daskqueue.queue.base_queue import Durability
from daskqueue.queue.durable_queue import DurableQueue
from daskqueue.utils import logger
from daskqueue.utils.funcs import msg_grouper

from .Consumer import ConsumerBaseClass, GeneralConsumer
from .Protocol import Message

TConsumer = TypeVar("TConsumer", bound=ConsumerBaseClass)


class PutTimeout(Exception):
    pass


class QueuePoolActor:
    """Utility class to operate on a fixed pool of queues.

    Arguments:
        queues (list): List of DaskActors to use in this pool.
    """

    def __init__(
        self,
        n_queues: int,
        durability: Durability = Durability.TRANSIENT,
        ack_timeout: int = 5,
        retry: bool = False,
        **kwargs,
    ):
        self.ack_timeout = ack_timeout
        self.retry = retry
        # actors to be used
        self._client = get_client()
        self.n_queues = n_queues
        self._queues = self.create_queues(n_queues, durability, **kwargs)

        self._cycle_queues_put = itertools.cycle(self._queues)
        self._cycle_queues_get = itertools.cycle(self._queues)

        self.n_queues = n_queues
        self._index_queue = {q.key: q for q in self._queues}
        self._queue_size = {q.key: 0 for q in self._queues}
        self.worker_class = GeneralConsumer
        self._total_put = 0

    @property
    def _worker(self):
        return get_worker()

    @property
    def _io_loop(self) -> asyncio.BaseEventLoop:
        if self._worker:
            return self._worker.io_loop

    def print(self) -> str:
        if len(self._queues) < 5:
            qsize = [
                f"\n\t{idx}: {q.qsize().result()} pending items"
                for idx, q in self._index_queue.items()
            ]

            return f"QueuePool : {self.n_queues} queue(s)" + "".join(qsize)
        else:
            sum_qsize = sum([q.qsize().result() for q in self._queues])
            return f"QueuePool : \n\t{self.n_queues} queue(s) \n\t{self._total_put} received \n\t{sum_qsize} pending"

    def get_len(self) -> int:
        return len(self._index_queue)

    def create_queues(self, n_queues: int, durability: Durability, **kwargs):
        if durability == Durability.TRANSIENT:
            return [
                self._client.submit(
                    TransientQueue,
                    maxsize=-1,
                    ack_timeout=self.ack_timeout,
                    retry=self.retry,
                    actor=True,
                ).result()
                for _ in range(n_queues)
            ]
        if durability == Durability.DURABLE:
            dirpath = kwargs["dirpath"] if "dirpath" in kwargs else "/tmp/"
            return [
                self._client.submit(
                    DurableQueue,
                    f"queue-{i}",
                    dirpath,
                    ack_timeout=self.ack_timeout,
                    retry=self.retry,
                    actor=True,
                ).result()
                for i in range(n_queues)
            ]
        raise ValueError("Please provide a correct durability type.")

    async def get_queues(self) -> List[BaseQueue]:
        return self._queues

    def get_next_queue(self) -> BaseQueue:
        return next(self._cycle_queues_get)

    async def get_queue(self, idx: int) -> BaseQueue:
        return self._queues[idx]

    def get_queue_size(self) -> Dict[str, int]:
        return {q: q.qsize().result() for q in self._queues}

    def _get_random_queue(self) -> BaseQueue:
        idx = np.random.randint(len(self._queues))
        return self._queues[idx]

    # TODO : Don't need this ??
    async def get_max_queue(self) -> BaseQueue:
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

    def put_many_sync(self, list_items: List[Any]) -> None:
        q = next(self._cycle_queues_put)

        logger.debug(f"Sending {len(list_items)} item to  queue : {q}")
        q.put_many(list_items).result()
        self._total_put += len(list_items)

    async def put(self, msg: Union[Message, Any], timeout=None) -> None:
        try:
            logger.debug(f"[QueuePool] Item put in queue: {msg}")
            q = next(self._cycle_queues_put)
            # TODO : delete this !
            await asyncio.wait_for(q.put(msg), timeout)
            self._total_put += 1
        except asyncio.TimeoutError:
            pass

    async def put_many(self, list_items: List[Any], timeout=None) -> None:
        q = next(self._cycle_queues_put)

        try:
            q.put_many(list_items)
            self._total_put += len(list_items)
        except asyncio.TimeoutError:
            ## TODO : implement canceling tasks  in Queue and QueuePool
            raise PutTimeout(f"Couldn't put all element in queue : {q}")

    def put_nowait(self, item: Any) -> None:
        q = self._get_random_queue()
        self._queue_size[q.key] += 1
        try:
            q.put_nowait(item).result()
        except asyncio.QueueFull:
            logger.debug("reRaise same error")
            raise asyncio.QueueFull

    def stop_gc(self):
        [q.stop_gc() for q in self._queues]


def decorator(cls):
    class Interface:
        """Interface class to communicate with the queue Pool actor spawned in the cluster."""

        def __init__(self, client, n_queues: int, **kwargs):
            self.actor = client.submit(cls, n_queues, actor=True, **kwargs).result()
            self.n_queues = n_queues
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

        def __getitem__(self, idx: int) -> TransientQueue:
            return self.actor.get_queue(idx).result()

        def __len__(self):
            return self.actor.get_len().result()

        def batch_submit(
            self,
            list_calls: List[Tuple[Callable, ...]],
            sync_mode: bool = True,
            worker_class: ConsumerBaseClass = GeneralConsumer,
            batch_size: int = 1000,
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

            futures = []

            with ThreadPoolExecutor(min(os.cpu_count(), self.n_queues)) as e:
                # TODO : figure out a heuristic for the batch size
                for msgs in msg_grouper(
                    min(len(list_calls) // self.n_queues + 1, batch_size), list_calls
                ):
                    if sync_mode:
                        f = e.submit(self.put_many_sync, msgs)
                    else:
                        f = e.submit(self.actor.put_many_sync, msgs)
                    futures.append(f)
            return [f.result() for f in futures]

    return Interface


QueuePool = decorator(QueuePoolActor)
