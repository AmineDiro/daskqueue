import asyncio
import glob
import os
from typing import Any, Dict, List, Optional, Tuple
from uuid import UUID

from distributed.worker import get_worker

from daskqueue.Protocol import Message
from daskqueue.segment.index_record import IdxRecord
from daskqueue.segment.index_segment import IndexSegment
from daskqueue.segment.log_segment import FullSegment, LogAccess, LogSegment

from .base_queue import BaseQueue, Durability


class DurableQueue(BaseQueue):
    def __init__(
        self,
        name: str,
        dirpath: str,
        durability: Durability = Durability.DURABLE,
        exchange: str = "default",
        maxsize: Optional[int] = None,
    ):
        self.name = name
        self.dirpath = dirpath
        # NOTE: A queue handles message from one exchange
        self.exchange = exchange
        self.queue_dir = os.path.join(self.dirpath, f"{self.exchange}-{self.name}")

        # If maxsize is None, the queue size is infinite
        self.maxsize = maxsize

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
        return IndexSegment(index_path)

    def _load_segments(self, path: str) -> Tuple[List[LogSegment], LogSegment]:
        segments = glob.glob(path + "/*.log")
        if segments:
            segments.sort()
            active_seg_path = segments.pop()
            list_ro_segment = [LogSegment(sp, LogAccess.RO) for sp in segments]
            return {int(seg.name): seg for seg in list_ro_segment}, LogSegment(
                active_seg_path, LogAccess.RW
            )
        else:
            # TODO : Determine this length
            seg_name = str(0).rjust(20, "0") + ".log"
            seg_path = os.path.join(path, seg_name)
            return {}, LogSegment(seg_path, LogAccess.RW)

    def new_active_segment(self):
        self.active_segment.close()
        # Appending the closed segment to the list of read-only segments
        self.ro_segments[int(self.active_segment.name)] = self.active_segment
        # TODO : Should probably add the archival info in the file footer ?
        file_no = int(self.active_segment.name) + 1
        seg_name = str(file_no).rjust(20, "0") + ".log"
        seg_path = os.path.join(self.queue_dir, seg_name)
        return LogSegment(seg_path, LogAccess.RW)

    def put_sync(self, item: Message) -> IdxRecord:
        # Append to Active Segment
        # TODO : Can't retry forever, I should add a wrapper to retrie a number of times
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
        # TODO : Could probably keep an ordered set of segments
        if file_no in self.ro_segments:
            return self.ro_segments[file_no].read(index_record.offset)

        record = self.active_segment.read(index_record.offset)
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

    def ack_sync(self, timestamp: float, msg_id: UUID):
        return self.index_segment.ack(timestamp, msg_id)

    async def ack(self, timestamp: float, msg_id: UUID):
        return self.ack_sync(timestamp, msg_id)

    def qsize(self):
        return len(self.index_segment)
