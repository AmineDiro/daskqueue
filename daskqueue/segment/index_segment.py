import mmap
import os
import struct
import time
from collections import OrderedDict
from io import FileIO
from threading import Event, Thread
from typing import Dict, Optional, Tuple
from uuid import UUID

from sortedcontainers import SortedDict

from daskqueue.Protocol import Message
from daskqueue.segment import (
    FORMAT_VERSION,
    HEADER_SIZE,
    INDEX_FILE_IDENTIFIER,
    INDEX_MAX_BYTES,
)

from .index_record import IdxRecord, IdxRecordProcessor, MessageStatus
from .log_record import RecordOffset


class IndexSegment:
    def __init__(
        self,
        path: str,
        max_bytes: int = INDEX_MAX_BYTES,
        ack_timeout: int = 5,
        retry: bool = False,
    ):
        self.path = path
        self.max_bytes = max_bytes
        self.name = self.parse_name(path)

        # In-memory datastructures
        self.delivered = SortedDict()
        self.ready = OrderedDict()
        self.processor = IdxRecordProcessor()
        self.retry = retry

        # Garbage collection tasks for delivered unacked message
        self.stop_gc_event = Event()
        self.ack_timeout = ack_timeout
        self._gc_thread = Thread(target=self._background_gc, daemon=True)
        self._gc_thread.start()

        # Setup
        self.created, self.file = self.create_or_open(path)
        self._mm_obj = self.mmap_index_segment()
        self.load_index()

    @property
    def closed(self) -> bool:
        return self._mm_obj.closed

    def __len__(self) -> int:
        """Return the length of both pending and delivered and unacked items ."""
        return len(self.ready) + len(self.delivered)

    def create_or_open(self, path) -> Tuple[bool, FileIO]:
        if not os.path.exists(path):
            with open(self.path, "wb") as f:
                off = self._write_header(f)
                f.write((self.max_bytes - off) * b"\0")
            return True, open(self.path, "r+b", 0)

        f = open(self.path, "r+b", 0)
        self.check_file(f)
        return False, f

    def _write_header(self, file):
        version_byte = struct.pack("!HH", *FORMAT_VERSION)
        return file.write(version_byte + INDEX_FILE_IDENTIFIER)

    def check_file(self, file):
        header = file.read(HEADER_SIZE)
        version = struct.unpack("!HH", header[:4])
        fid = header[4:]
        if fid != INDEX_FILE_IDENTIFIER or version != FORMAT_VERSION:
            file.close()
            raise Exception("The file is not a daskqueue index_segment.")

    def mmap_index_segment(self):
        mm_obj = mmap.mmap(self.file.fileno(), 0)
        mm_obj.seek(HEADER_SIZE)
        return mm_obj

    def load_index(self) -> Dict[UUID, IdxRecord]:
        assert self._mm_obj.tell() == HEADER_SIZE
        cur = HEADER_SIZE
        while cur < self.max_bytes:
            try:
                buffer = self._mm_obj[cur : cur + self.processor.RECORD_SIZE]
                if buffer == self.processor.RECORD_SIZE * b"\x00":
                    raise ValueError("End of file")

                idx_record = self.processor.parse_bytes(buffer)
                cur += self.processor.RECORD_SIZE
                self.update_index(idx_record)
                self._mm_obj.seek(cur)
            except ValueError:
                break
            except AssertionError:
                break

    def update_index(
        self,
        idx_record: IdxRecord,
    ):
        if idx_record.status == MessageStatus.READY:
            self.ready[idx_record.msg_id] = idx_record

        elif idx_record.status == MessageStatus.DELIVERED:
            self.ready.pop(idx_record.msg_id, None)
            self.delivered[idx_record.timestamp] = idx_record

        elif idx_record.status == MessageStatus.ACKED:
            self.delivered.pop(idx_record.timestamp, None)

    def append(
        self,
        msg_id: UUID,
        status: MessageStatus,
        offset: RecordOffset,
        delivered_timestamp: float = None,
    ) -> IdxRecord:
        # Write to disk .index file
        tmstmp = time.time()
        if status == MessageStatus.ACKED:
            idx_record = IdxRecord(msg_id, status, offset, delivered_timestamp)
        else:
            idx_record = IdxRecord(msg_id, status, offset, tmstmp)
        idx_record_bytes = self.processor.serialize_idx_record(idx_record)
        _ = self._mm_obj.write(idx_record_bytes)
        # Update internal mem index
        self.update_index(idx_record)
        return idx_record

    def push(self, msg_id: UUID, offset: RecordOffset) -> IdxRecord:
        return self.append(msg_id, MessageStatus.READY, offset)

    def pop(self) -> Optional[IdxRecord]:
        try:
            idx_record: IdxRecord = self.ready.popitem(last=False)[1]
            return self.append(
                idx_record.msg_id, MessageStatus.DELIVERED, idx_record.offset
            )
        except KeyError:
            return None

    def drop(msg: Message):
        pass

    def ack(self, timestamp: float, msg_id: Message):
        # Pop the delivered messages from the queue
        idx_record = self.delivered.pop(timestamp, default=None)
        if idx_record is None or idx_record.msg_id != msg_id:
            raise ValueError("Msg doesnt exist in the delivered list")

        # Update
        return self.append(
            msg_id=msg_id,
            status=MessageStatus.ACKED,
            offset=idx_record.offset,
            delivered_timestamp=timestamp,
        )

    # TODO: Compat
    def _compact(self):
        pass

    def _background_gc(self):
        while not self.stop_gc_event.is_set():
            now = time.time()
            cutoff = self.delivered.bisect_left(now)
            for t in self.delivered.keys()[:cutoff]:
                idx_record = self.delivered.pop(t)
                # TODO : Will retry indefinetly
                if self.retry:
                    self.ready[idx_record.msg_id] = idx_record
                    self.ready.move_to_end(idx_record.msg_id, last=False)
                else:
                    # TODO: For now, we ack the failed garbage collected messages
                    self.append(
                        msg_id=idx_record.msg_id,
                        status=MessageStatus.ACKED,
                        offset=idx_record.offset,
                        delivered_timestamp=now,
                    )
            time.sleep(self.ack_timeout)

    def parse_name(self, path):
        filename = os.path.basename(path)
        return os.path.splitext(filename)[0]

    def close(self) -> bool:
        self.stop_gc_event.set()
        self._mm_obj.flush()
        self.file.flush()
        self._mm_obj.close()
        self.file.close()
        return self.closed
