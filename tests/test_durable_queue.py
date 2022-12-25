import logging
import os
import time
from re import I

import pytest

from daskqueue.Protocol import Message

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,%(msecs)d %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)

from daskqueue.queue.durable_queue import DurableQueue, LogAccess, LogSegment

# FIxture
# seg_name = str(0).rjust(20, "0") + ".log"
# seg_path = tmp_path / seg_name
# seg = LogSegment(seg_path, LogAccess.RW, 1024)


@pytest.fixture
def seg(tmp_path):
    seg_name = str(0).rjust(20, "0") + ".log"
    seg_path = tmp_path / seg_name

    seg = LogSegment(seg_path, LogAccess.RW, 1024)
    return seg


def test_logsegment(tmp_path):
    seg_name = str(0).rjust(20, "0") + ".log"
    seg_path = tmp_path / seg_name

    seg = LogSegment(seg_path, LogAccess.RW, 1024)
    assert seg._mm_obj.closed == False
    assert seg.w_cursor == 0

    seg_name = str(1).rjust(20, "0") + ".log"
    seg_path = tmp_path / seg_name

    seg = LogSegment(seg_path, LogAccess.RO, 1024)
    assert seg._mm_obj is None
    assert seg.w_cursor == 0


def test_logsegment_append(seg):

    _, size1 = seg.append(b"12")
    _, size2 = seg.append(b"12")
    assert seg.w_cursor == size1 + size2

    with pytest.raises(ValueError) as e_info:
        seg.append(1024 * b"1")


def test_logsegment_close(seg):
    _, w_size = seg.append(b"12")
    seg.close()
    assert seg.closed
    assert os.path.basename(seg.path) == str(w_size).rjust(20, "0") + ".log"
