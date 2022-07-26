import argparse
import asyncio
import logging
import uuid
from abc import ABC, abstractmethod
from concurrent.futures import ProcessPoolExecutor
from typing import Any, List, Tuple

from distributed import get_worker

from .QueuePool import QueuePool

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,%(msecs)d %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)


class ConsumerBaseClass(ABC):
    def __init__(self, pool: QueuePool) -> None:
        self.id = str(uuid.uuid4())
        self.pool = pool
        self.future = None
        self.items = []
        self._worker = get_worker()
        self._executor = self._worker.executor
        self.tasks = []

    async def len_items(self) -> int:
        return len(self.items)

    async def get_items(self) -> List[Any]:
        return self.items

    async def consume_status(self) -> List[Any]:
        return self.fetch_loop.cancelled()

    async def start(self) -> None:
        """Starts the consumming loop, runs on Dask Worker's Tornado event loop."""
        self.fetch_loop = asyncio.create_task(self._consume())

    async def _consume(self) -> None:
        """Runs an async loop to fetch item from a queue determined by the QueuePool and processes it in place"""
        loop = asyncio.get_event_loop()
        while True:
            q = await self.pool.get_max_queue()
            item = await q.get()
            if item:
                self.items.append(item)
            future = asyncio.ensure_future(
                loop.run_in_executor(self._executor, self.process_item, item),
            )
            self.tasks.append(future)

            if item is None:
                break

    async def cancel(self) -> None:
        """Cancels the running _consume task"""
        logging.info(f"[Consumer {self.id}]:  Canceling consumer ...")
        self.fetch_loop.cancel()
        [t.cancel() for t in self.tasks]

    @abstractmethod
    def process_item(self, item: Any):
        """Process items from the queue."""
        raise NotImplementedError


class DummyConsumer(ConsumerBaseClass):
    def process_item(self, item):
        logging.info(f"[Consumer {self.id}]: Processing {item}")
