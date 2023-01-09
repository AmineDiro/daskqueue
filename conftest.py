import time

import pytest
from distributed.utils_test import (  # noqa
    cleanup,
    client,
    cluster_fixture,
    gen_cluster,
    loop,
)

from daskqueue.Protocol import Message
from daskqueue.queue.durable_queue import DurableQueue
from daskqueue.segment.index_segment import IndexSegment
from daskqueue.segment.log_segment import LogAccess, LogSegment

MAX_BYTES = 100 * 1024  # 100KB


def sleep_func(t):
    time.sleep(t)
    return t


# def func(x):
#     return x + 2

func = lambda x: x + 2


@pytest.fixture
def msg():
    msg = Message(func, 12)
    return msg


@pytest.fixture
def log_segment(tmp_path):
    seg_name = str(0).rjust(20, "0") + ".log"
    seg_path = tmp_path / seg_name

    seg = LogSegment(seg_path, LogAccess.RW, MAX_BYTES)
    return seg


@pytest.fixture
def index_segment(tmp_path):
    name = str(0).rjust(10, "0") + ".index"
    index_path = tmp_path / name
    idx = IndexSegment(index_path)
    return idx


@pytest.fixture
def durable_queue(tmp_path):
    return DurableQueue(name="queue-0", dirpath=str(tmp_path))
