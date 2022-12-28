import logging
import struct

import pytest

from daskqueue.Protocol import Message
from daskqueue.segment.log_record import RecordOffset

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,%(msecs)d %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)

from daskqueue.segment import FORMAT_VERSION, HEADER_SIZE, INDEX_FILE_IDENTIFIER
from daskqueue.segment.index_record import IdxRecord, MessageStatus
from daskqueue.segment.index_segment import IndexSegment


def test_index_segment(tmp_path):
    name = str(1).rjust(10, "0") + ".index"
    index_path = tmp_path / name

    seg = IndexSegment(index_path)
    assert seg._mm_obj.closed == False
    assert seg._mm_obj.tell() == 8


def test_check_index_file(tmpdir):
    p = tmpdir.join("bad.index")
    p.write(b"test")

    with pytest.raises(Exception) as e_info:
        seg = IndexSegment(p)

    p = tmpdir.join("good.index")

    p.write(struct.pack("!HH", *FORMAT_VERSION) + INDEX_FILE_IDENTIFIER)

    seg = IndexSegment(p)
    assert seg._mm_obj.tell() == 8


def test_index_segment_append(msg, log_segment, index_segment):
    offset: RecordOffset = log_segment.append(msg)

    assert log_segment.w_cursor == HEADER_SIZE + offset.size

    # Record the msg to index
    index_segment.push(msg.id, offset)
    # idx_record = index_segment.pop(msg.id)

    assert msg.id in index_segment.ready
    assert index_segment.ready[msg.id].offset.offset == offset.offset
    assert index_segment.ready[msg.id].offset.size == offset.size


def test_index_segment_close(index_segment, msg):
    index_segment.close()
    assert index_segment.closed


def test_index_segment_read(msg, index_segment, log_segment):
    N = 10

    func = lambda x: x + 2
    for _ in range(N):
        msg = Message(func, 1)
        offset = log_segment.append(msg)
        index_segment.set(msg.id, MessageStatus.READY, offset)

    index_segment.close()
    assert len(index_segment) == N
