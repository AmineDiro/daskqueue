import logging
import os
import struct
import time
from re import I

import pytest

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,%(msecs)d %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)

from daskqueue.segment.log import LogAccess, LogSegment

# FIxture
# seg_name = str(0).rjust(20, "0") + ".log"
# seg_path = tmp_path / seg_name
# seg = LogSegment(seg_path, LogAccess.RW, 1024)


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
    assert seg.w_cursor == 4

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
    p.write(LogSegment._FILE_IDENTIFIER)

    seg = LogSegment(p, LogAccess.RW, 1024)
    assert seg._mm_obj.tell() == 4


def test_logsegment_append(log_segment):
    header = struct.pack("!HH", *log_segment._FORMAT_VERSION)
    offset1, size1 = log_segment.append(b"12")
    _, size2 = log_segment.append(b"12")
    assert log_segment.w_cursor == len(log_segment._FILE_IDENTIFIER) + size1 + size2

    with pytest.raises(ValueError) as e_info:
        log_segment.append(1024 * b"1")

    log_segment.close()

    with open(log_segment.path, "r+b") as f:
        f.seek(offset1)
        assert header == f.read(len(header))
        assert b"12" == f.read(2)
        assert log_segment._FOOTER == f.read(len(log_segment._FOOTER))


def test_logsegment_close(log_segment):
    offset, w_size = log_segment.append(b"12")
    log_segment.close()
    assert log_segment.closed
    assert (
        os.path.basename(log_segment.path)
        == str(offset + w_size).rjust(20, "0") + ".log"
    )


def test_logseg_reopen(tmpdir):
    p = tmpdir.join("0000.log")

    log_segment = LogSegment(p, LogAccess.RW, 1024)
    offset, size = log_segment.append(b"12")
    log_segment.close()

    assert log_segment.closed

    log_segment = LogSegment(p, LogAccess.RW, 1024)

    assert log_segment.w_cursor == offset
