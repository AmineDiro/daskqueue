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
from daskqueue.segment import _FORMAT_VERSION, _INDEX_FILE_IDENTIFIER, HEADER_SIZE
from daskqueue.segment.index_record import MessageStatus
from daskqueue.segment.index_segment import IndexSegment
from daskqueue.segment.log_segment import LogAccess, LogSegment


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


@pytest.fixture
def index_segment(tmp_path):
    name = str(0).rjust(10, "0") + ".index"
    index_path = tmp_path / name
    idx = IndexSegment(index_path)
    return idx


def test_index_segment(tmp_path):
    name = str(0).rjust(20, "0") + ".index"
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

    p.write(struct.pack("!HH", *_FORMAT_VERSION) + _INDEX_FILE_IDENTIFIER)

    seg = IndexSegment(p)
    assert seg._mm_obj.tell() == 8


def test_index_segment_append(msg, log_segment, index_segment):
    offset = log_segment.append(msg)

    assert log_segment.w_cursor == HEADER_SIZE + offset.size

    # Can't write
    index_segment.set(msg.id, MessageStatus.READY, offset)


def test_index_segment_close(index_segment, msg):
    index_segment.close()
    assert index_segment.closed


@pytest.mark.skip
def test_index_segment(tmpdir, msg):
    p = tmpdir.join("0000.idx")

    log_segment = IndexSegment(p)
    offset = log_segment.append(msg)
    log_segment.close()

    assert log_segment.closed

    log_segment = IndexSegment(p)
