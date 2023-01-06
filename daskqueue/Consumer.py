import asyncio
import logging
import os
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Any, Dict, List

from distributed import get_worker

from daskqueue.queue.base_queue import BaseQueue
from daskqueue.utils import logger

from .backend import Backend
from .Protocol import Message


class ConsumerBaseClass(ABC):
    def __init__(
        self,
        id: int,
        name: str,
        pool,
        batch_size: int,
        max_concurrency: int,
        retries: int,
        early_ack: bool,
    ) -> None:
        self.id = id
        self.name = name + f"-{os.getpid()}"
        self.pool = pool
        self.future = None
        self.items = []
        self._worker = get_worker()
        self._executor = self._worker.executor
        self._running_tasks = []
        self._logger = logger
        self.max_concurrency = max_concurrency
        self.batch_size = batch_size
        self.n_retries = retries
        self.early_ack = early_ack

        logger.debug(
            f"Consumer specs : batch : {batch_size}, retry : {self.n_retries}, max_concrrency: {max_concurrency}"
        )

    async def len_items(self) -> int:
        return len(self.items)

    async def len_pending_items(self) -> int:
        return len(self._running_tasks)

    async def get_current_queue(self) -> BaseQueue:
        return self._current_

    async def get_items(self) -> List[Any]:
        return self.items

    async def done(self) -> bool:
        """Checks if the consumer is done. A done consumers has a closed fetch_loop
        and no running tasks.

        Returns:
            done (bool): True if the consumer is done consumming.
        """
        await self.update_state()
        try:
            done = (
                len(self._running_tasks) == 0
                and self.fetch_loop.done()
                # and len(self.items) > 0
            )
            return done
        except AttributeError:
            # Hasn't started the loop yet
            return False

    async def is_consumming(self) -> bool:
        return not (self.fetch_loop.cancelled() or self.fetch_loop.done())

    async def start(self, timeout: int = 1) -> None:
        """Starts the consumming loop, runs on Dask Worker's Tornado event loop."""
        self._current_q = await self.pool.get_next_queue()
        self.fetch_loop = asyncio.create_task(self._consume(timeout))

    async def update_state(self) -> None:
        _done_tasks = [task for task in self._running_tasks if task.done()]
        for task in _done_tasks:
            logger.debug(f"[{self.name}]: Cleaning done tasks")
            self._running_tasks.remove(task)

    async def _ack_items(self, items: List[Message]):
        logger.debug(f"[Consumer-{self.name}- Ackings items {items}")
        await self._current_q.ack_many(
            [(item.delivered_timestamp, item.id) for item in items]
        )

    async def _ack_late_item(self, task: asyncio.Future, item: Message):
        await task
        await self._current_q.ack(item.delivered_timestamp, item.id)

    async def _consume(self, timeout: float = 0.1) -> None:
        """Runs an async loop to fetch item from a queue determined by the QueuePool and processes it in place"""
        loop = asyncio.get_event_loop()

        while True:
            await self.update_state()
            items = await self._current_q.get_many(self.batch_size, timeout=timeout)
            items = [item for item in items if item]

            if len(items) > 0:
                if self.early_ack:
                    await self._ack_items(items)

                for item in items:
                    logger.debug(f"[{self.name}]: Received item : {item}")
                    if item is None:
                        continue

                    self.items.append(item)

                    task = asyncio.ensure_future(
                        loop.run_in_executor(self._executor, self.process_item, item),
                    )

                    if not self.early_ack:
                        # NOTE: Ack each message individualy
                        ack_task = asyncio.create_task(self._ack_late_item(task, item))
                        self._running_tasks.append(ack_task)

                    self._running_tasks.append(task)
                    if len(self._running_tasks) > self.max_concurrency:
                        done, pending = await asyncio.wait(
                            self._running_tasks, return_when=asyncio.FIRST_COMPLETED
                        )

    async def cancel(self) -> bool:
        """Cancels the running _consume task"""
        logging.info(
            f"[{self.name}]: Cancelling {len(self._running_tasks)} outstanding tasks"
        )
        done = True
        # Waits for all pending tasks to finish before killing
        await self.update_state()
        # TODO : Check that everything is shutdown
        try:
            self.fetch_loop.cancel()
            if len(self._running_tasks) > 0:
                done, _ = await asyncio.wait(
                    self._running_tasks, return_when=asyncio.ALL_COMPLETED
                )
        finally:
            return True

    @abstractmethod
    def process_item(self, item: Any):
        """Process items from the queue."""
        raise NotImplementedError


class DummyConsumer(ConsumerBaseClass):
    def process_item(self, item):
        logger.info(f"[{self.name}]: Processing {item}")


class GeneralConsumer(ConsumerBaseClass):
    def __init__(
        self,
        id: int,
        name,
        pool,
        batch_size,
        max_concurrency: int,
        retries: int,
        early_ack: bool,
        backend: Backend = None,
    ) -> None:
        self.backend = backend
        self._results = defaultdict(lambda: None)
        super().__init__(
            id, name, pool, batch_size, max_concurrency, retries, early_ack
        )

    def get_results(self) -> Dict[Message, Any]:
        return self._results

    def save(self, msg, result: Any) -> None:
        logger.debug(f"[{self.name}] Saving result for item : {msg}")
        self._results[hash(msg)] = result
        if self.backend:
            self.backend.save(result)

    def process_item(self, msg: Message) -> None:
        logger.debug(f"[{self.name}] Processing item : {msg.data}")
        result = msg.data()
        self.save(msg, result)
