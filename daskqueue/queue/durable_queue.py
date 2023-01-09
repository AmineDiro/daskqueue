import asyncio
import glob
import os
from typing import Any, Dict, List, Optional, Tuple
from uuid import UUID

from distributed.worker import get_worker

from daskqueue.Protocol import Message
from daskqueue.segment import INDEX_MAX_BYTES, MAX_BYTES
from daskqueue.segment.index_record import IdxRecord
from daskqueue.segment.index_segment import IndexSegment
from daskqueue.segment.log_segment import FullSegment, LogAccess, LogSegment
from daskqueue.utils import logger

from .base_queue import BaseQueue, Durability


class DurableQueue(BaseQueue):
    def __init__(
        self,
        name: str,
        dirpath: str,
        durability: Durability = Durability.DURABLE,
        exchange: str = "default",
        maxsize: Optional[int] = None,
        log_max_bytes: int = MAX_BYTES,
        index_max_bytes: int = INDEX_MAX_BYTES,
        ack_timeout: int = 5,
        retry: bool = False,
    ):
        self.name = name
        self.dirpath = dirpath

        # NOTE: A queue handles message from one exchange
        self.exchange = exchange
        self.queue_dir = os.path.join(self.dirpath, f"{self.exchange}-{self.name}")

        # If maxsize is None, the queue size is infinite
        self.log_max_bytes = log_max_bytes
        self.index_max_bytes = index_max_bytes
        self.maxsize = maxsize
        self.ack_timeout = ack_timeout
        self.retry = retry

        # Get the IOLoop running on the worker
        self.worker_loop = asyncio.new_event_loop()  # self._io_loop
        asyncio.set_event_loop(self.worker_loop)

        self.ro_segments: Dict[int, LogSegment] = {}
        self.active_segment: LogSegment = None
        self.index_segment: IndexSegment = None

        # TODO(@Amine) : Parse the storage
        self.setup_storage()

        super().__init__(durability=durability, maxsize=self.maxsize)

    @property
    def _worker(self):
        return get_worker()

    @property
    def _io_loop(self) -> asyncio.BaseEventLoop:
        if self._worker:
            return self._worker.io_loop

    def setup_storage(self) -> Any:
        if not os.path.exists(self.queue_dir):
            os.makedirs(self.queue_dir)

        self.ro_segments, self.active_segment = self._load_segments(self.queue_dir)
        self.index_segment = self._load_index(self.queue_dir)

    def _load_index(self, path: str) -> IndexSegment:
        name = f"{self.exchange}-{self.name}.index"
        index_path = os.path.join(path, name)
        return IndexSegment(
            index_path,
            max_bytes=self.index_max_bytes,
            retry=self.retry,
            ack_timeout=self.ack_timeout,
        )

    def _load_segments(self, path: str) -> Tuple[List[LogSegment], LogSegment]:
        segments = glob.glob(path + "/*.log")
        if segments:
            segments.sort()
            active_seg_path = segments.pop()
            list_ro_segment = [
                LogSegment(sp, LogAccess.RO, max_bytes=self.log_max_bytes)
                for sp in segments
            ]
            return {int(seg.name): seg for seg in list_ro_segment}, LogSegment(
                active_seg_path, LogAccess.RW, max_bytes=self.log_max_bytes
            )
        else:
            # TODO : Determine this length
            seg_name = str(0).rjust(20, "0") + ".log"
            seg_path = os.path.join(path, seg_name)
            return {}, LogSegment(seg_path, LogAccess.RW, max_bytes=self.log_max_bytes)

    def new_active_segment(self):
        self.active_segment.read_only()
        # Appending the closed segment to the list of read-only segments
        self.ro_segments[int(self.active_segment.name)] = self.active_segment
        # TODO : Should probably add the archival info in the file footer ?
        file_no = int(self.active_segment.name) + 1
        seg_name = str(file_no).rjust(20, "0") + ".log"
        seg_path = os.path.join(self.queue_dir, seg_name)
        return LogSegment(seg_path, LogAccess.RW, max_bytes=self.log_max_bytes)

    def put_sync(self, item: Message) -> IdxRecord:
        # Append to Active Segment
        # TODO : Can't retry forever, I should add a wrapper to retrie a number of times
        if len(item.serialize()) > (self.log_max_bytes - 8):
            raise ValueError(
                "Cannot append message bigger than the max log semgent size"
            )
        try:
            offset = self.active_segment.append(item)
        except FullSegment:
            self.active_segment = self.new_active_segment()
            offset = self.active_segment.append(item)

        # Add to Log Index
        return self.index_segment.push(item.id, offset)

    def get_sync(self) -> Optional[Message]:
        index_record = self.index_segment.pop()
        if index_record is None:
            return None
        file_no = index_record.offset.file_no

        # TODO : Could probably keep an ordered set of segments (RO + Active)
        if file_no in self.ro_segments:
            record = self.ro_segments[file_no].read(index_record.offset)
        else:
            record = self.active_segment.read(index_record.offset)

        # Update the msg delivered timestamp to match record
        record.msg.delivered_timestamp = index_record.timestamp
        return record.msg

    async def put(self, item: Message, timeout=None):
        return self.put_sync(item)

    async def put_many(self, list_items: List[Message]):
        for item in list_items:
            self.put_sync(item)

    async def get(self, timeout=None) -> Optional[Message]:
        return self.get_sync()

    async def get_many(self, n: int, timeout=None) -> List[Optional[Message]]:
        return [self.get_sync() for _ in range(n)]

    def qsize(self):
        return len(self.index_segment)

    def ack_sync(self, timestamp: float, msg_id: UUID):
        logger.debug(f"[Queue-{self.name}] Ack item {msg_id}")
        return self.index_segment.ack(timestamp, msg_id)

    async def ack(self, timestamp: float, msg_id: UUID):
        return self.ack_sync(timestamp, msg_id)

    async def ack_many(self, items: List[Tuple[float, UUID]]):
        await asyncio.gather(*[self.ack(item[0], item[1]) for item in items])

    def close(self):
        [log.close_file() for log in self.ro_segments.values()]
        self.active_segment.close_file()
        self.index_segment.close()

    def stop_gc(self):
        self.index_segment.stop_gc_event.set()
