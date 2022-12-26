import asyncio
import glob
import os
from typing import Any, List, Optional, Tuple

from distributed.worker import get_worker

from daskqueue.Protocol import Message
from daskqueue.segment.index_segment import IndexSegment
from daskqueue.segment.log_segment import LogAccess, LogSegment

from .base_queue import BaseQueue, Durability


class DurableQueue(BaseQueue):
    def __init__(
        self,
        name: str,
        dirpath: str,
        exchange: str = "default",
        maxsize: Optional[int] = None,
        max_bytes: int = 1024,
    ):
        self.name = name
        self.dirpath = dirpath
        # If maxsize is less than or equal to zero, the queue size is infinite
        self.maxsize = maxsize
        self.max_bytes = max_bytes

        # NOTE: A queue handles message from one exchange
        self.exchange = exchange

        # Get the IOLoop running on the worker
        self.worker_loop = asyncio.new_event_loop()  # self._io_loop
        asyncio.set_event_loop(self.worker_loop)

        self.ro_segments: List[LogSegment] = []
        self.active_segment: LogSegment = None
        self.segment_index: IndexSegment = None

        # TODO(@Amine) : Parse the storage
        self.setup_storage()

        super().__init__(durability=Durability.DURABLE, maxsize=self.maxsize)

    @property
    def _worker(self):
        return get_worker()

    @property
    def _io_loop(self) -> asyncio.BaseEventLoop:
        if self._worker:
            return self._worker.io_loop

    def setup_storage(self) -> Any:
        queue_dir = os.path.join(self.dirpath, f"{self.exchange}-{self.exchange}")
        if not os.path.exists(queue_dir):
            os.makedirs(queue_dir)

        self.ro_segments, self.active_segment = self._load_segments(queue_dir)
        self.segment_index = self._load_index(queue_dir)

    def _load_index(self, path: str) -> IndexSegment:
        name = str(self.name).rjust(10, "0") + ".index"
        index_path = os.path.join(path, name)
        return IndexSegment(index_path)

    def _load_segments(path: str) -> Tuple[List[LogSegment], LogSegment]:
        segments = glob.glob(path + "/*.log")
        if segments:
            segments.sort()
            active_seg_path = segments.pop()
            return [LogSegment(sp, LogAccess.RO) for sp in segments], LogSegment(
                active_seg_path, LogAccess.RW
            )
        else:
            # TODO : Determine this length
            seg_name = str(0).rjust(20, "0") + ".log"
            seg_path = os.join(path, seg_name)
            return [], LogSegment(seg_path, LogAccess.RW)

    def put_sync(self, item: Message, timeout=None):
        # Append to Active Segment
        # Add to Log Index
        # Append to the dequeue
        pass

    def get_sync(self, timeout=None):
        # Pop item from dequeue
        #

        pass

    async def put(self, item: Message, timeout=None):
        pass

    async def put_many(self, list_items):
        pass

    async def get(self, timeout=None):
        pass

    def qsize(self):
        pass
