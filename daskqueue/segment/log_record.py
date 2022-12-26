import struct
from binascii import crc32
from dataclasses import dataclass

import cloudpickle

from daskqueue.Protocol import Message
from daskqueue.segment import _FOOTER


@dataclass(frozen=True, slots=True)
class RecordOffset:
    file_no: int
    offset: int
    size: int

    def pack(self):
        return struct.pack("!III", self.file_no, self.offset, self.size)


@dataclass(frozen=True)
class Record:
    checksum: int
    msg_size: int
    msg: Message
    footer: bytes


class RecordProcessor:
    def parse_bytes(self, buffer: bytes) -> Record:
        footer = buffer[-4:]
        checksum_data = buffer[4:-4]
        s = 0
        checksum = struct.unpack("!I", buffer[:4])[0]
        s += 4
        msg_size = struct.unpack("!i", buffer[s : s + 4])[0]
        s += 4
        msg = cloudpickle.loads(buffer[s : s + msg_size])

        record = Record(
            checksum=checksum,
            msg_size=msg_size,
            msg=msg,
            footer=footer,
        )

        if not (footer == _FOOTER) or not self._verify_checksum(
            checksum, checksum_data
        ):
            raise Exception("Corrupt data detected: invalid checksum")

        return record

    def _verify_checksum(self, retrieved_checksum: int, checksum_data: bytes):
        # key is the bytes of the key,
        return crc32(checksum_data) & 0xFFFFFFFF == retrieved_checksum

    def create_record(self, msg: Message):
        msg_bytes = msg.serialize()
        record_size = struct.pack("!i", len(msg_bytes))

        # CRC covers : checksum(<MSG_SIZE><MSG>)
        data = record_size + msg_bytes
        checksum = struct.pack("!I", crc32(data) & 0xFFFFFFFF)
        blob = checksum + data + _FOOTER
        return blob
