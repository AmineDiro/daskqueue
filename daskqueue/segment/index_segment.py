import mmap
import os
import struct
import time
from collections import OrderedDict
from typing import Dict
from uuid import UUID

from sortedcontainers import SortedDict

from daskqueue.Protocol import Message
from daskqueue.segment import (
    _FORMAT_VERSION,
    _INDEX_FILE_IDENTIFIER,
    HEADER_SIZE,
    MAX_BYTES,
)

from .index_record import IdxRecord, IdxRecordProcessor, MessageStatus
from .log_record import RecordOffset


class IndexSegment:
    def __init__(self, path: str, max_bytes: int = MAX_BYTES):
        self.path = path
        self.max_bytes = max_bytes

        self.processor = IdxRecordProcessor()
        self.created, self.file = self.create_or_open(path)
        self._mm_obj = self.mmap_index_segment()

        # In-memory Datastructure
        self.delivered_messages, self.ready_messages = self.load_index()

    @property
    def closed(self) -> bool:
        return self._mm_obj.closed

    def __len__(self):
        return len(self.ready_messages)

    def create_or_open(self, path):
        if not os.path.exists(path):
            with open(self.path, "wb") as f:
                off = self._write_header(f)
                f.write((self.max_bytes - off) * b"\0")
            return True, open(self.path, "r+b", 0)

        f = open(self.path, "r+b", 0)
        self.check_file(f)
        return False, f

    def close(self):
        self._mm_obj.close()
        self.file.close()
        return self.closed

    def _write_header(self, file):
        version_byte = struct.pack("!HH", *_FORMAT_VERSION)
        return file.write(version_byte + _INDEX_FILE_IDENTIFIER)

    def check_file(self, file):
        header = file.read(HEADER_SIZE)
        version = struct.unpack("!HH", header[:4])
        fid = header[4:]
        if fid != _INDEX_FILE_IDENTIFIER or version != _FORMAT_VERSION:
            file.close()
            raise Exception("The file is not a daskqueue index_segment.")

    def mmap_index_segment(self):
        mm_obj = mmap.mmap(self.file.fileno(), 0)
        mm_obj.seek(HEADER_SIZE)
        return mm_obj

    def load_index(self) -> Dict[UUID, IdxRecord]:
        delivered = SortedDict()
        ready = OrderedDict(lambda: None)
        assert self._mm_obj.tell() == 8
        cur = 8
        try:
            while cur < self.max_bytes:
                idx_record = self.processor.parse_bytes(
                    self._mm_obj[cur : cur + self.processor.RECORD_SIZE]
                )
                cur += self.processor.RECORD_SIZE
                self.update_index(ready, idx_record)
        finally:
            self._mm_obj.seek(HEADER_SIZE)
            return delivered, ready

    def update_index(
        self,
        delivered: SortedDict[int, IdxRecord],
        ready: OrderedDict[UUID, IdxRecord],
        idx_record: IdxRecord,
    ):
        if idx_record.status == MessageStatus.READY:
            ready[idx_record.msg_id] = idx_record

        elif idx_record.status == MessageStatus.DELIVERED:
            ready.pop(idx_record.msg_id, None)
            delivered[idx_record.timestamp] = idx_record

        elif idx_record.status == MessageStatus.ACKED:
            # Shouldn't have an acked message in ready queue
            delivered.pop(idx_record.timestamp)

    def set(self, msg_id: UUID, status: MessageStatus, offset: RecordOffset):
        # Write to disk
        tmstmp = time.time()
        idx_record = IdxRecord(msg_id, status, offset, tmstmp)
        idx_record_bytes = self.processor.create_idx_record(idx_record)
        _ = self._mm_obj.write(idx_record_bytes)

        # Update internal index
        self.msg_index[msg_id] = idx_record

    def get(self, msg_id: UUID) -> IdxRecord:
        # Finds the msg in the index
        return self.msg_index[msg_id]

    def pop() -> Message:
        pass

    def drop(msg: Message):
        pass

    def ack(msg: Message):
        pass
