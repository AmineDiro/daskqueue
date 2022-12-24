import asyncio
import glob
import mmap
import os
from enum import Enum, auto
from typing import Any, List, Optional, Tuple

from distributed.worker import get_worker

from daskqueue.Protocol import Message

from .base_queue import BaseQueue, Durability


class LogAccess(Enum):
    RO = auto()  # closed segment
    RW = auto()  # closed


class LogSegment:
    def __init__(self, path: str, status: LogAccess, max_bytes: int):
        self.path = path
        self.status = status
        self.max_bytes = max_bytes
        self.cursor = 0
        self.offset_range = ()

        self.name = self.parse_name(path)
        self.file = self.create_or_open(path)
        self._mm_obj = self.mmap_segment(status)

    def create_or_open(self, path):
        if not os.path.exists(self.path):
            with open(self.path, "wb") as f:
                f.write(self.max_bytes * b"\0")
            f = open(self.path, "r+b", 0)
        else:
            f = open(self.path, "r+b", 0)
        return f

    def mmap_segment(self, status):
        if status == LogAccess.RW:
            mm_obj = mmap.mmap(self.file.fileno(), 0)
            # TODO: Read about this
            mm_obj.madvise(mmap.MADV_SEQUENTIAL)
            return mm_obj

    def append(self, buffer: bytes) -> int:
        if self.status != LogAccess.RW:
            raise Exception("Can't append to a closed segment")
        nbytes = self._mm_obj.write(buffer)
        old_cursor = self.cursor
        self.cursor += nbytes
        return (old_cursor, self.cursor)

    @property
    def closed(self) -> bool:
        if self.status == LogAccess.RO:
            return True
        return self._mm_obj.closed

    def close(self) -> int:
        self._mm_obj.close()
        self.file.close()
        self.status = LogAccess.RO

        self.path = self.rename_segment(self)

        return self.cursor

    def rename_segment(self):
        self.name = str(self.cursor).rjust(20, "0") + ".log"
        dirpath = os.path.dirname(self.path)
        offset_path = os.path.join(dirpath, self.name)
        os.rename(self.path, offset_path)
        return offset_path

    def parse_name(self, path):
        filename = os.path.basename(path)
        return os.path.splitext(filename)[0]

    def parse_offset():
        pass


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

        self.segments: List[LogSegment] = []
        self.active_segment: LogSegment = None

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

    def get_segments(path: str) -> Tuple[List[LogSegment], LogSegment]:
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

    async def put(self, item: Message, timeout=None):
        pass

    async def put_many(self, list_items):
        pass

    async def get(self, timeout=None):
        pass

    def qsize(self):
        pass
