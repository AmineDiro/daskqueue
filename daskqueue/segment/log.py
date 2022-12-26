import mmap
import os
import struct
from enum import Enum, auto
from typing import Tuple

from daskqueue.Protocol import Message
from daskqueue.segment import _FILE_IDENTIFIER, _FOOTER, _FORMAT_VERSION, HEADER_SIZE

from .record import Record, RecordProcessor


class LogAccess(Enum):
    RO = auto()  # closed segment
    RW = auto()  # closed


class LogSegment:
    # TODO : construct a header

    def __init__(self, path: str, status: LogAccess, max_bytes: int):
        self.path = path
        self.status = status
        self.max_bytes = max_bytes
        self.w_cursor = 0
        self.offset_range = ()

        self.name = self.parse_name(path)

        self.file = self.create_or_open(path)
        self._mm_obj = self.mmap_segment(status)

        self.rec_processor = RecordProcessor()

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
        version_byte = struct.pack("!HH", *_FORMAT_VERSION)
        return file.write(version_byte + _FILE_IDENTIFIER)

    def check_file(self, file):
        header = file.read(HEADER_SIZE)
        version = struct.unpack("!HH", header[:4])
        fid = header[4:]
        if fid != _FILE_IDENTIFIER or version != (0, 1):
            file.close()
            raise Exception("The file is not the compatible with daskqueue logsegment.")

    def mmap_segment(self, status):
        if status == LogAccess.RW:
            mm_obj = mmap.mmap(self.file.fileno(), 0)
            mm_obj.madvise(mmap.MADV_SEQUENTIAL)

            # Seek to the latest write positon
            last_write = mm_obj.rfind(_FOOTER)
            if last_write > 0:
                self.w_cursor = last_write
                mm_obj.seek(self.w_cursor)
            else:
                # Move the the header
                self.w_cursor = 8
                mm_obj.seek(8)
            return mm_obj

    def append(self, msg: Message) -> Tuple[int, int]:
        if self.status != LogAccess.RW:
            raise Exception("Can't append to a closed segment")

        offset = self._mm_obj.tell()
        record_bytes = self.rec_processor.create_record(msg)
        n_bytes = self._mm_obj.write(record_bytes)

        # Update write cursor
        self.w_cursor += n_bytes

        return (offset, n_bytes)

    @property
    def closed(self) -> bool:
        if self.status == LogAccess.RO:
            return True
        return self._mm_obj.closed

    def close(self) -> int:
        self._mm_obj.close()
        self.file.close()
        self.status = LogAccess.RO

        self.path = self.rename_segment()
        return self.w_cursor

    def rename_segment(self):
        self.name = str(self.w_cursor).rjust(20, "0") + ".log"
        dirpath = os.path.dirname(self.path)
        offset_path = os.path.join(dirpath, self.name)
        os.rename(self.path, offset_path)
        return offset_path

    def parse_name(self, path):
        filename = os.path.basename(path)
        return os.path.splitext(filename)[0]

    def parse_offset():
        pass
