import pytest

from daskqueue.Protocol import Message
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
