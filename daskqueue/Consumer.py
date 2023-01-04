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
        name,
        pool,
        max_concurrency: int = 10000,
        batch: bool = True,
        batch_size: int = 1000,
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
        self.batch = batch
        self.batch_size = batch_size

    async def len_items(self) -> int:
        return len(self.items)

    async def len_pending_items(self) -> int:
        return len(self._running_tasks)

    async def get_current_queue(self) -> BaseQueue:
        return self._current_q

    async def get_items(self) -> List[Any]:
        return self.items

    async def done(self) -> bool:
        await self.update_state()

        try:
            done = (
                len(self._running_tasks) == 0
                and self.fetch_loop.done()
                and len(self.items) > 0
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

    async def _consume(self, timeout: float = 0.1) -> None:
        """Runs an async loop to fetch item from a queue determined by the QueuePool and processes it in place"""
        loop = asyncio.get_event_loop()
        while True:
            try:
                await self.update_state()

                if not self.batch:
                    items = [await self._current_q.get(timeout=timeout)]

                else:
                    items = await self._current_q.get_many(
                        self.batch_size, timeout=timeout
                    )

                for item in items:
                    logger.debug(f"[{self.name}]: Received item : {item}")
                    if item is None:
                        if len(self.items) == 0:
                            continue
                        else:
                            raise ValueError("Received None.")

                    self.items.append(item)

                    task = asyncio.ensure_future(
                        loop.run_in_executor(self._executor, self.process_item, item),
                    )

                    self._running_tasks.append(task)
                    if len(self._running_tasks) > self.max_concurrency:
                        done, pending = await asyncio.wait(
                            self._running_tasks, return_when=asyncio.FIRST_COMPLETED
                        )
            except ValueError:
                break

    async def cancel(self) -> None:
        """Cancels the running _consume task"""
        logger.debug(f"[{self.name}]: Canceling ...")
        logging.info(
            f"[{self.name}]: Cancelling {len(self._running_tasks)} outstanding tasks"
        )
        [t.cancel() for t in self._running_tasks]
        self.fetch_loop.cancel()

    @abstractmethod
    def process_item(self, item: Any):
        """Process items from the queue."""
        raise NotImplementedError


class DummyConsumer(ConsumerBaseClass):
    def process_item(self, item):
        logger.info(f"[{self.name}]: Processing {item}")


class GeneralConsumer(ConsumerBaseClass):
    def __init__(
        self, id: int, name, pool, max_concurrency: int = 10000, backend: Backend = None
    ) -> None:
        self.backend = backend
        self._results = defaultdict(lambda: None)
        super().__init__(id, name, pool, max_concurrency)

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
