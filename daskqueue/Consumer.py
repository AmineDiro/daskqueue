import argparse
import asyncio
import os
import queue
import random
import shutil
import time
from typing import List, Tuple
import uuid

import numpy as np
from abc import ABC, abstractmethod

from distributed import Client


class ConsumerBaseClass(ABC):
    def __init__(self, pool) -> None:
        self.pool = pool
        self.future = None
        self.items = []

    async def len_items(self) -> int:
        return len(self.items)

    async def get_items(self):
        return self.items

    async def start(self):
        """Starts the consumming loop, runs on Dask Worker's Tornado event loop."""
        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

        self.task = asyncio.create_task(self._consume())

    async def _consume(self):
        """Runs an async loop to fetch item from a queue determined by the QueuePool and processes it in place"""
        while True:
            q = await self.pool.get_max_queue()
            item = await q.get_nowait()
            ################################
            ### IMPORTANT : CHANGE THIS BLOCK
            if isinstance(item, Tuple):
                self.items.append(item)
                src, dst = item
                shutil.copy(src, dst)
            else:
                continue
            ################################

    async def cancel(self):
        """Cancels the running _consume task"""
        print("Canceling consumer ...")
        self.task.cancel()

    @abstractmethod
    def process_item(self, item):
        """Process items from the queue."""
        raise NotImplementedError


class DummyConsumer(ConsumerBaseClass):
    def process_item(self, item):
        print(f"Processing {item}")
        pass
