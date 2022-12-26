import mmap
import os
import struct
import time
from typing import Dict
from uuid import UUID

from daskqueue.segment import _FORMAT_VERSION, _INDEX_FILE_IDENTIFIER, HEADER_SIZE

from .index_record import IdxRecord, IdxRecordProcessor, MessageStatus
from .log_record import RecordOffset


class IndexSegment:
    def __init__(self, path: str, max_bytes: int = 1024):
        self.path = path
        self.max_bytes = max_bytes

        self.processor = IdxRecordProcessor()
        self.file = self.create_or_open(path)
        self._mm_obj = self.mmap_index_segment()
        self.msg_index = self.read_index()

    @property
    def closed(self) -> bool:
        return self._mm_obj.closed

    def create_or_open(self, path):
        if not os.path.exists(path):
            with open(self.path, "wb") as f:
                off = self._write_header(f)
                f.write((self.max_bytes - off) * b"\0")
            f = open(self.path, "r+b", 0)
        else:
            f = open(self.path, "r+b", 0)
            self.check_file(f)
        return f

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
        mm_obj.seek(8)
        return mm_obj

    def read_index(self) -> Dict[UUID, IdxRecord]:
        return {}

    def set(self, msg_id: UUID, status: MessageStatus, offset: RecordOffset):
        # Write to disk
        # Update internal index
        tmstmp = time.time()
        idx_record = IdxRecord(msg_id, status, offset, tmstmp)
        idx_record_bytes = self.processor.create_idx_record(idx_record)
        _ = self._mm_obj.write(idx_record_bytes)

        self.msg_index[msg_id] = (status, offset, tmstmp)
