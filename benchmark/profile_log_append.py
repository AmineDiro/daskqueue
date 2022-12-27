import os
import tempfile

from daskqueue.Protocol import Message
from daskqueue.segment.index_segment import IndexSegment, MessageStatus
from daskqueue.segment.log_segment import LogAccess, LogSegment

N = 10_000
N_TEST = 10
MAX_BYTES = 100 * int(1e6)  # 100 MB


func = lambda x: x + 2


def rdx_msg():
    msg = Message(func, 12)
    return msg


def log_segment(tmpdir):
    seg_name = str(0).rjust(20, "0") + ".log"
    seg_path = os.path.join(tmpdir, seg_name)

    return LogSegment(seg_path, LogAccess.RW, MAX_BYTES)


def index_segment(tmpdir):
    name = str(0).rjust(10, "0") + ".index"
    seg_path = os.path.join(tmpdir, name)
    return IndexSegment(seg_path, MAX_BYTES)


def write_log(N: int, idx: IndexSegment, log: LogSegment):
    msg = rdx_msg()
    offset = log.append(msg)
    idx.set(msg.id, MessageStatus.READY, offset)

    log.close()
    idx.close()


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as tmpdirname:
        print("Temp dir :", tmpdirname)
        log = log_segment(tmpdirname)
        idx = index_segment(tmpdirname)
        write_log(N, idx, log)
