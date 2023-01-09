import struct
from binascii import crc32
from dataclasses import dataclass
from enum import IntEnum, auto
from uuid import UUID

from .log_record import RecordOffset


class MessageStatus(IntEnum):
    READY = auto()
    DELIVERED = auto()
    ACKED = auto()
    FAILED = auto()


@dataclass
class IdxRecord:
    msg_id: UUID
    status: MessageStatus
    offset: RecordOffset
    timestamp: float


class IdxRecordProcessor:
    RECORD_SIZE = 41

    def parse_bytes(self, buffer: bytes) -> IdxRecord:
        assert len(buffer) == self.RECORD_SIZE
        checksum_data = buffer[4:]
        s = 0
        checksum = struct.unpack("!I", buffer[s : s + 4])[0]
        s += 4
        if not self._verify_checksum(checksum, checksum_data):
            raise Exception("Corrupt data detected: invalid checksum")

        timestamp = struct.unpack("!d", buffer[s : s + 8])[0]
        s += 8
        msg_id = UUID(bytes=buffer[s : s + 16])
        s += 16
        status = MessageStatus(struct.unpack("!b", buffer[s : s + 1])[0])
        s += 1
        file_no, offset, size = struct.unpack("!III", buffer[s : s + 12])
        record = IdxRecord(
            msg_id=msg_id,
            status=status,
            offset=RecordOffset(file_no, offset, size),
            timestamp=timestamp,
        )

        return record

    def _verify_checksum(self, retrieved_checksum: int, checksum_data: bytes):
        # key is the bytes of the key,
        return crc32(checksum_data) & 0xFFFFFFFF == retrieved_checksum

    def serialize_idx_record(self, idx_record: IdxRecord):
        # <CRC-4><TIMESTAMP><MSG_ID-4><STATUS-1><POINTER>
        timestamp_bytes = struct.pack("!d", idx_record.timestamp)  # 8 bytes
        id = idx_record.msg_id.bytes  # 16 bytes
        status = struct.pack("!b", idx_record.status)  # 1 bytes

        data = timestamp_bytes + id + status + idx_record.offset.pack()  # 37 bytes
        checksum = struct.pack("!I", crc32(data) & 0xFFFFFFFF)  # 4 byes

        # TODO : Test to see if alignement improves performance??
        blob = checksum + data  # 41 bytes
        return blob
