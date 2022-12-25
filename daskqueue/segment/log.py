import mmap
import os
import struct
from enum import Enum, auto
from typing import Tuple


class LogAccess(Enum):
    RO = auto()  # closed segment
    RW = auto()  # closed


class LogSegment:
    # TODO : construct a header
    _FORMAT_VERSION = (0, 1)
    _FILE_IDENTIFIER = b"\x53\x34\x4e\x40"
    _FOOTER = b"\x52\x3f\x4a\x43"

    def __init__(self, path: str, status: LogAccess, max_bytes: int):
        self.path = path
        self.status = status
        self.max_bytes = max_bytes
        self.w_cursor = 0
        self.offset_range = ()

        self.name = self.parse_name(path)
        self.file = self.create_or_open(path)
        self._mm_obj = self.mmap_segment(status)

    def create_or_open(self, path):
        # File Structure :
        # <FILE_IDENTIFIER - 4 bytes ><Blocks>...
        # Where each block has the following structure:
        # <FORMAT_VERSION><N Bytes>
        if not os.path.exists(path):
            with open(self.path, "wb") as f:
                f.write(self._FILE_IDENTIFIER)
                f.write((self.max_bytes - 4) * b"\0")
            f = open(self.path, "r+b", 0)
        else:
            f = open(self.path, "r+b", 0)
            self.check_file(f)
        return f

    def check_file(self, file):
        header = file.read(len(self._FILE_IDENTIFIER))
        if header != self._FILE_IDENTIFIER:
            file.close()
            raise Exception("The file is not the compatible with daskqueue logsegment.")

    def mmap_segment(self, status):
        if status == LogAccess.RW:
            mm_obj = mmap.mmap(self.file.fileno(), 0)
            mm_obj.madvise(mmap.MADV_SEQUENTIAL)

            # Seek to the latest write positon
            last_write = mm_obj.rfind(self._FOOTER)
            if last_write > 0:
                self.w_cursor = last_write
                mm_obj.seek(self.w_cursor)
            else:
                # Move the the header
                self.w_cursor = 4
                mm_obj.seek(4)
            return mm_obj

    def _pack_buffer(self, buffer):
        header = struct.pack("!HH", *self._FORMAT_VERSION)
        return header + buffer + self._FOOTER

    def append(self, buffer: bytes) -> Tuple[int, int]:
        if self.status != LogAccess.RW:
            raise Exception("Can't append to a closed segment")

        offset = self._mm_obj.tell()
        packed_buffer = self._pack_buffer(buffer)
        n_bytes = self._mm_obj.write(packed_buffer)

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
