import os
import tempfile
import time

import numpy as np
from tqdm import tqdm

from daskqueue.Protocol import Message
from daskqueue.segment.index_segment import IndexSegment, MessageStatus
from daskqueue.segment.log_segment import LogAccess, LogSegment

N = 10_000
N_TEST = 10
MAX_BYTES = 100 * int(1e6)  # 100 MB


def rdx_msg():
    func = lambda x: x + 2
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
    s = time.perf_counter()
    size = 0
    for _ in range(N):
        msg = rdx_msg()
        offset = log.append(msg)
        size += offset.size
        idx.append(msg.id, MessageStatus.READY, offset)
    e = time.perf_counter()

    wps = N / (e - s)  # op/s
    w_mbps = size / (e - s) / (1e6)  # mbs

    log.read_only()
    idx.close()
    return w_mbps, wps


if __name__ == "__main__":
    w_ops = []
    w_mbps = []

    for i in tqdm(range(N_TEST)):
        with tempfile.TemporaryDirectory() as tmpdirname:
            if i == 0:
                print("Temp dir :", tmpdirname)
            log = log_segment(tmpdirname)
            idx = index_segment(tmpdirname)
            t_wmbps, t_wops = write_log(N, idx, log)
            w_ops.append(t_wops)
            w_mbps.append(t_wmbps)

    w_ops = np.array(w_ops)
    w_mbps = np.array(w_mbps)

    print(f"Mean write ops [{N_TEST}tests] {w_ops.mean():.2f} wop/s")
    print(f"Mean write mps [{N_TEST}tests] {w_mbps.mean():.2f} mb/s")
