import struct
from binascii import crc32
from dataclasses import dataclass
from typing import Tuple
from uuid import UUID

import cloudpickle

from daskqueue.Protocol import Message
from daskqueue.segment import _FOOTER


@dataclass(frozen=True)
class Record:
    checksum: int
    msg_id_size: int
    msg_size: int
    msg_id: UUID
    msg: Message
    footer: bytes


class RecordProcessor:
    def parse_bytes(self, buffer: bytes) -> Record:
        footer = buffer[-4:]
        checksum_data = buffer[4:-4]
        s = 0
        checksum = struct.unpack("!I", buffer[:4])[0]
        s += 4
        msgid_size, msg_size = struct.unpack("!ii", buffer[s : s + 8])
        s += 8
        msg_id = UUID(buffer[s : s + msgid_size].decode())
        s += msgid_size
        msg = cloudpickle.loads(buffer[s : s + msg_size])

        record = Record(
            checksum=checksum,
            msg_id_size=msgid_size,
            msg_size=msg_size,
            msg_id=msg_id,
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
        msg_uuid = str(msg.id).encode()
        msg_bytes = msg.serialize()
        record_size = struct.pack("!ii", len(msg_uuid), len(msg_bytes))

        # CRC covers : checksum(<MSGID_SIZE><MSG_SIZE><MSG_IG><MSG>)
        data = record_size + msg_uuid + msg_bytes
        checksum = struct.pack("!I", crc32(data) & 0xFFFFFFFF)
        blob = checksum + data + _FOOTER
        return blob
