import mmap
import os
import struct
from enum import Enum, auto

from daskqueue.Protocol import Message
from daskqueue.segment import (
    FILE_IDENTIFIER,
    FOOTER,
    FORMAT_VERSION,
    HEADER_SIZE,
    MAX_BYTES,
)

from .log_record import RecordOffset, RecordProcessor


class FullSegment(Exception):
    pass


class LogAccess(Enum):
    RO = auto()  # closed segment
    RW = auto()  # closed


class LogSegment:
    # TODO : construct a header

    def __init__(self, path: str, status: LogAccess, max_bytes: int = MAX_BYTES):
        self.path = path
        self.status = status
        self.max_bytes = max_bytes
        self.w_cursor = 0
        self.offset_range = ()

        self.name = self.parse_name(path)

        self.file = self.create_or_open(path)
        self._mm_obj = self.mmap_segment(status)

        self.processor = RecordProcessor()

    def create_or_open(self, path):
        # File Structure :
        # Where each block has the following structure:
        if not os.path.exists(path):
            with open(self.path, "wb") as f:
                off = self._write_header(f)
                f.write((self.max_bytes - off) * b"\0")
            f = open(self.path, "r+b", 0)
        else:
            f = open(self.path, "r+b", 0)
            self.check_file(f)
        return f

    def _write_header(self, file):
        version_byte = struct.pack("!HH", *FORMAT_VERSION)
        return file.write(version_byte + FILE_IDENTIFIER)

    def check_file(self, file):
        header = file.read(HEADER_SIZE)
        version = struct.unpack("!HH", header[:4])
        fid = header[4:]
        if fid != FILE_IDENTIFIER or version != (0, 1):
            file.close()
            raise Exception("The file is not the compatible with daskqueue logsegment.")

    def mmap_segment(self, status):
        mm_obj = mmap.mmap(self.file.fileno(), 0)

        self.w_cursor = 8

        if status == LogAccess.RW:
            # Seek to the latest write positon
            last_write = mm_obj.rfind(FOOTER)
            if last_write > 0:
                self.w_cursor = last_write + len(FOOTER)
                mm_obj.seek(self.w_cursor)
            else:
                mm_obj.seek(8)

                self.w_cursor = 8
                mm_obj.seek(8)

        return mm_obj

    def append(self, msg: Message) -> RecordOffset:
        if self.status != LogAccess.RW:
            raise Exception("Can't append to a closed segment")

        try:
            offset = self._mm_obj.tell()
            record_bytes = self.processor.create_record(msg)
            n_bytes = self._mm_obj.write(record_bytes)

            # Update write cursor
            self.w_cursor += n_bytes
            return RecordOffset(file_no=int(self.name), offset=offset, size=n_bytes)

        except ValueError:
            raise FullSegment("The log segment is full")

    def read(self, offset: RecordOffset):
        return self.processor.parse_bytes(
            self._mm_obj[offset.offset : offset.offset + offset.size]
        )

    @property
    def closed(self) -> bool:
        if self.status == LogAccess.RO:
            return True
        return self._mm_obj.closed

    def archive(self):
        self.status = LogAccess.RO
        return self.w_cursor

    def read_only(self) -> int:
        self.status = LogAccess.RO
        return self.w_cursor

    def parse_name(self, path):
        filename = os.path.basename(path)
        return os.path.splitext(filename)[0]

    def parse_offset():
        pass

    def close_file(self):
        self.read_only()
        self._mm_obj.flush()
        self._mm_obj.close()
        self.file.close()
