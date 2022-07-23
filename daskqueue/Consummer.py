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

from distributed import Client


class Consumer:
    def __init__(self, pool) -> None:
        self.pool = pool
        self.future = None
        self.items = []

    async def len_items(self) -> int:
        return len(self.items)

    async def get_items(self):
        return self.items

    async def start(self):
        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

        self.task = asyncio.create_task(self._consume())

    async def _consume(self):
        while True:
            q = await self.pool.get_max_queue()
            item = await q.get_nowait()
            if isinstance(item, int):
                self.items.append(item)
            ## TODO change this to function
            if isinstance(item, Tuple):
                self.items.append(item)
                src, dst = item
                shutil.copy(src, dst)

    async def cancel(self):
        print("Canceling consumer ...")
        self.task.cancel()

