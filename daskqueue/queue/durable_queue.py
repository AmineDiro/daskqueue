import asyncio
import glob
import mmap
import os
from enum import Enum, auto
from typing import Any, List, Optional, Tuple

from distributed.worker import get_worker

from daskqueue.Protocol import Message

from .base_queue import BaseQueue, Durability


class SegmentStatus(Enum):
    RO: auto()  # closed segment
    RW: auto()  # closed


class Segment:
    def __init__(self, path: str, status: SegmentStatus, max_bytes: int):
        self.path = path
        self.name = self.parse_name(path)
        self.status = status
        self.max_bytes = max_bytes
        self.cursor = 0
        self.offset_range = ()
        self._mm_obj = self.init_segment(status)

    def init_segment(self, status):
        if status == SegmentStatus.RW:
            if not os.path.exists():
                with open(self.path, "wb") as f:
                    f.write(self.max_bytes * b"\0")
                f = open(self.path, "r+b", 0)
                return mmap.mmap(f.fileno())
            else:
                f = open(self.path, "r+b", 0)
                return mmap.mmap(f.fileno())

    def append(self):
        pass

    def parse_name(path):
        filename = os.path.basename(path)
        return os.path.splitext(filename)[0]

    def parse_offset():
        pass


class PersistentQueue(BaseQueue):
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

        self.segments: List[int] = []
        self.active_segment: int = None

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

        self.ro_seg, self.active_seg = self.get_segments(queue_dir)

    def get_segments(path: str) -> Tuple[List[Segment], Segment]:
        segments = glob.glob(path + "/*.log")
        if segments:
            segments.sort()
            active_seg_path = segments.pop()
            return [Segment(sp, SegmentStatus.RO) for sp in segments], Segment(
                active_seg_path, SegmentStatus.RW
            )
        else:
            # TODO : Determine this length
            seg_name = str(0).rjust(20, "0") + ".log"
            seg_path = os.join(path, seg_name)
            return [], Segment(seg_path, SegmentStatus.RW)

    async def put(self, item: Message, timeout=None):
        pass

    async def put_many(self, list_items):
        pass

    async def get(self, timeout=None):
        pass

    def qsize(self):
        pass
