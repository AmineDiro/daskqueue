import logging
import os
import struct
from re import I

import pytest

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,%(msecs)d %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)

from daskqueue.Protocol import Message
from daskqueue.segment import _FILE_IDENTIFIER, HEADER_SIZE
from daskqueue.segment.log import LogAccess, LogSegment


@pytest.fixture
def msg():
    func = lambda x: x + 2
    msg = Message(func, 12)
    return msg


@pytest.fixture
def log_segment(tmp_path):
    seg_name = str(0).rjust(20, "0") + ".log"
    seg_path = tmp_path / seg_name

    seg = LogSegment(seg_path, LogAccess.RW, 1024)
    return seg


def test_logsegment(tmp_path):
    seg_name = str(0).rjust(20, "0") + ".log"
    seg_path = tmp_path / seg_name

    seg = LogSegment(seg_path, LogAccess.RW, 1024)
    assert seg._mm_obj.closed == False
    assert seg.w_cursor == 8
    assert seg._mm_obj.tell() == 8

    seg_name = str(1).rjust(20, "0") + ".log"
    seg_path = tmp_path / seg_name

    seg = LogSegment(seg_path, LogAccess.RO, 1024)
    assert seg._mm_obj is None
    assert seg.w_cursor == 0


def test_check_segfile(tmpdir):
    p = tmpdir.join("bad.log")
    p.write(b"test")

    with pytest.raises(Exception) as e_info:
        seg = LogSegment(p, LogAccess.RW, 1024)

    p = tmpdir.join("good.log")

    _FORMAT_VERSION = (0, 1)
    p.write(struct.pack("!HH", *_FORMAT_VERSION) + _FILE_IDENTIFIER)

    seg = LogSegment(p, LogAccess.RW, 1024)
    assert seg._mm_obj.tell() == 8
    assert seg.w_cursor == 8


def test_logsegment_append(log_segment, msg):
    offset = log_segment.append(msg)

    assert log_segment.w_cursor == HEADER_SIZE + offset.size

    # Can't write
    with pytest.raises(ValueError) as e_info:
        [log_segment.append(msg) for _ in range(1000)]

    log_segment.close()

    with open(log_segment.path, "r+b") as f:
        f.seek(offset.offset)
        blob = f.read(offset.size)
        record = log_segment.rec_processor.parse_bytes(blob)
        assert msg.data() == record.msg.data()
        assert msg.timestamp == record.msg.timestamp


def test_logsegment_close(log_segment, msg):
    offset = log_segment.append(msg)
    log_segment.close()
    assert log_segment.closed
    assert (
        os.path.basename(log_segment.path)
        == str(offset.offset + offset.size).rjust(20, "0") + ".log"
    )


def test_logseg_reopen(tmpdir, msg):
    p = tmpdir.join("0000.log")

    log_segment = LogSegment(p, LogAccess.RW, 1024)
    offset = log_segment.append(msg)
    log_segment.close()

    assert log_segment.closed

    log_segment = LogSegment(p, LogAccess.RW, 1024)

    assert log_segment.w_cursor == offset.offset
