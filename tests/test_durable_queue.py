import logging
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


def test_logsegment(tmp_path):
    seg_name = str(0).rjust(20, "0") + ".log"
    seg_path = tmp_path / seg_name

    seg = LogSegment(seg_path, LogAccess.RW, 1024)
    assert seg._mm_obj.closed == False
    assert seg.cursor == 0

    seg_name = str(1).rjust(20, "0") + ".log"
    seg_path = tmp_path / seg_name

    seg = LogSegment(seg_path, LogAccess.RO, 1024)
    assert seg._mm_obj is None
    assert seg.cursor == 0


def test_logsegment_append(tmp_path):
    seg_name = str(0).rjust(20, "0") + ".log"
    seg_path = tmp_path / seg_name
    seg = LogSegment(seg_path, LogAccess.RW, 1024)

    seg.append(b"12")
    seg.append(b"12")
    assert seg.cursor == 4

    seg.append(1024 * b"1")


def test_logsegment_close(tmp_path):
    seg_name = str(0).rjust(20, "0") + ".log"
    seg_path = tmp_path / seg_name

    seg = LogSegment(seg_path, LogAccess.RW, 1024)
    assert seg.closed
    assert seg.path == tmp_path / str(2).rjust(20, "0") + ".log"
