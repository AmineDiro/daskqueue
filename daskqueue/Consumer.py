import argparse
import asyncio
import logging
from socketserver import TCPServer
from turtle import update
import uuid
from abc import ABC, abstractmethod
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from typing import Any, Dict, List, Tuple

from distributed import get_worker

from daskqueue.utils import logger
from .Protocol import Message


class ConsumerBaseClass(ABC):
    def __init__(self, pool) -> None:
        self.id = str(uuid.uuid4())
        self.pool = pool
        self.future = None
        self.items = []
        self._worker = get_worker()
        self._executor = self._worker.executor
        self._running_tasks = []
        self._logger = logger
        self.max_concurrency = 1000

    async def len_items(self) -> int:
        return len(self.items)

    async def get_items(self) -> List[Any]:
        return self.items

    async def done(self) -> bool:
        await self.update_state()
        done = len(self._running_tasks) == 0 and self.fetch_loop.done()
        logger.debug(f"[Consumer {self.id}]: fetch_loop {self.fetch_loop.done()}")
        logger.debug(
            f"[Consumer {self.id}]: running {len(self._running_tasks)}, {self._running_tasks}"
        )
        return done

    async def consume_status(self) -> bool:
        return self.fetch_loop.cancelled()

    async def start(self) -> None:
        """Starts the consumming loop, runs on Dask Worker's Tornado event loop."""
        self.fetch_loop = asyncio.create_task(self._consume())

    async def update_state(self) -> None:
        _done_tasks = [task for task in self._running_tasks if task.done()]
        for task in _done_tasks:
            logger.debug("[Consumer {self.id}]: Cleaning done tasks")
            self._running_tasks.remove(task)

    async def _consume(self, timeout: int = 1) -> None:
        """Runs an async loop to fetch item from a queue determined by the QueuePool and processes it in place"""
        loop = asyncio.get_event_loop()
        while True:
            await self.update_state()

            q = await self.pool.get_max_queue()
            item = await q.get(timeout=timeout)

            if item is None:
                break

            logger.debug(f"[Consumer {self.id}]: Received item : {item}")
            self.items.append(item)

            task = asyncio.ensure_future(
                loop.run_in_executor(self._executor, self.process_item, item),
            )

            self._running_tasks.append(task)

            if len(self._running_tasks) > self.max_concurrency:
                await asyncio.gather(*self._running_tasks, return_exceptions=True)

    async def cancel(self) -> None:
        """Cancels the running _consume task"""
        logger.debug(f"[Consumer {self.id}]: Canceling consumer ...")
        logging.info(
            f"[Consumer {self.id}]: Cancelling {len(self._running_tasks)} outstanding tasks"
        )
        [t.cancel() for t in self._running_tasks]

        self.fetch_loop.cancel()

    @abstractmethod
    def process_item(self, item: Any):
        """Process items from the queue."""
        raise NotImplementedError


class DummyConsumer(ConsumerBaseClass):
    def process_item(self, item):
        logger.info(f"[Consumer {self.id}]: Processing {item}")


class Backend:
    pass


class GeneralConsumer(ConsumerBaseClass):
    def __init__(self, pool, backend=None) -> None:
        self.backend = backend
        self._results = defaultdict(lambda: None)
        super().__init__(pool)

    def get_results(self) -> Dict[Message, Any]:
        return self._results

    def save(self, msg, result: Any) -> None:
        self._results[hash(msg)] = result
        if self.backend:
            self.backend.save(result)

    def process_item(self, msg: Message) -> None:
        logger.debug(f"[Executor] processing item {msg}")
        args, kwargs = msg._data
        result = msg.func(*args, **kwargs)
        self.save(msg, result)
