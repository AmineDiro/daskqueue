import logging
import os
import struct
import tempfile
import time

import pytest

from conftest import func, index_segment
from daskqueue.Protocol import Message
from daskqueue.segment.log_record import RecordOffset
from daskqueue.segment.log_segment import LogSegment

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,%(msecs)d %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)

from daskqueue.segment import FORMAT_VERSION, HEADER_SIZE, INDEX_FILE_IDENTIFIER
from daskqueue.segment.index_record import IdxRecord, IdxRecordProcessor, MessageStatus
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
    from conftest import func

    N = 10

    for _ in range(N):
        msg = Message(func, 1)
        offset = log_segment.append(msg)
        index_segment.append(msg.id, MessageStatus.READY, offset)

    index_segment.close()
    assert len(index_segment) == N


def test_index_segment_pop(msg, index_segment: IndexSegment, log_segment):
    from conftest import func

    N = 10
    M = 3
    for _ in range(N):
        msg = Message(func, 1)
        offset = log_segment.append(msg)
        index_segment.append(msg.id, MessageStatus.READY, offset)

    for _ in range(M):
        rec = index_segment.pop()
    assert len(index_segment.delivered) == M
    assert len(index_segment.ready) == N - M


def test_index_segment_ack(msg, index_segment: IndexSegment, log_segment):

    N = 10
    for _ in range(N):
        msg = Message(func, 1)
        offset = log_segment.append(msg)
        index_segment.append(msg.id, MessageStatus.READY, offset)

    rec = index_segment.pop()
    assert len(index_segment.delivered) == 1

    assert index_segment.delivered.keys()[0] == rec.timestamp

    delivered_rec = index_segment.ack(rec.timestamp, rec.msg_id)
    assert len(index_segment.delivered) == 0
    assert len(index_segment.ready) == N - 1
    assert delivered_rec.timestamp == rec.timestamp
    assert delivered_rec.status == MessageStatus.ACKED


def test_index_processor(log_segment, index_segment, msg):
    offset = log_segment.append(msg)
    index_record = index_segment.push(msg.id, offset)
    processor = IdxRecordProcessor()

    buffer = processor.serialize_idx_record(index_record)
    index_record_bis = processor.parse_bytes(buffer)
    assert index_record == index_record_bis


def test_load_index(msg: Message, log_segment: LogSegment):
    with tempfile.TemporaryDirectory() as tmpdirname:
        name = f"default-queue-0.index"
        index_path = os.path.join(tmpdirname, name)
        index_segment = IndexSegment(index_path)

        for _ in range(10):
            msg = Message(func, 1)
            offset = log_segment.append(msg)
            index_segment.push(msg.id, offset)

        index_segment.close()
        assert index_segment.closed

        ready = index_segment.ready
        delivered = index_segment.delivered

        index_segment = IndexSegment(index_path)
        assert ready == index_segment.ready
        assert delivered == index_segment.delivered


def test_index_segment_gc(msg, tmp_path, log_segment: LogSegment):
    name = str(1).rjust(10, "0") + ".index"
    index_path = tmp_path / name

    index_segment = IndexSegment(index_path, ack_timeout=0.1, retry=False)
    N = 10
    M = 4

    for _ in range(N):
        msg = Message(func, 1)
        offset = log_segment.append(msg)
        index_segment.append(msg.id, MessageStatus.READY, offset)

    for _ in range(M):
        _ = index_segment.pop()
    assert len(index_segment.delivered) == M
    assert len(index_segment.ready) == N - M

    time.sleep(0.5)
    assert len(index_segment.delivered) == 0
    assert len(index_segment.ready) == N - M


def test_index_segment_gc_reschedule(msg, tmp_path, log_segment: LogSegment):
    name = str(1).rjust(10, "0") + ".index"
    index_path = tmp_path / name

    index_segment = IndexSegment(index_path, ack_timeout=0.1, retry=True)
    N = 2

    for _ in range(N):
        msg = Message(func, 1)
        offset = log_segment.append(msg)
        index_segment.append(msg.id, MessageStatus.READY, offset)

    for _ in range(N):
        _ = index_segment.pop()

    assert len(index_segment.delivered) == N
    assert len(index_segment.ready) == 0

    time.sleep(0.2)
    assert len(index_segment.ready) == N
    assert len(index_segment.delivered) == 0


def test_index_segment_stop_gc(tmp_path):
    name = str(1).rjust(10, "0") + ".index"
    index_path = tmp_path / name

    index_segment = IndexSegment(index_path, ack_timeout=0.1, retry=True)

    index_segment.stop_gc.set()

    time.sleep(0.1)

    assert index_segment._gc_thread.is_alive() == False
