import argparse
import asyncio
import os
import queue
import random
import shutil
import time
from typing import List, Tuple
import uuid
from concurrent.futures import ProcessPoolExecutor

import numpy as np
from abc import ABC, abstractmethod

from distributed import Client, get_worker

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,%(msecs)d %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)


class ConsumerBaseClass(ABC):
    def __init__(self, pool) -> None:
        self.pool = pool
        self.future = None
        self.items = []
        self._worker = get_worker()
        self._executor = self._worker.executor
        self.tasks = []

    async def len_items(self) -> int:
        return len(self.items)

    async def get_items(self):
        return self.items

    async def start(self):
        """Starts the consumming loop, runs on Dask Worker's Tornado event loop."""
        self.fetch_loop = asyncio.create_task(self._consume())

    async def _consume(self):
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

    async def cancel(self):
        """Cancels the running _consume task"""

        logging.info("Canceling consumer ...")
        self.fetch_loop.cancel()

    @abstractmethod
    def process_item(self, item):
        """Process items from the queue."""
        raise NotImplementedError


class DummyConsumer(ConsumerBaseClass):
    def process_item(self, item):
        logging.info(f"Processing {item}")

