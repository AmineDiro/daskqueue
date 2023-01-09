import click

from conftest import func
from daskqueue.Protocol import Message
from daskqueue.queue.durable_queue import DurableQueue

N = 1_000

cprint = click.echo


def func_no_return():
    return None


def process_item():
    return sum(i * i for i in range(10**2))


def rdx_msg():
    msg = Message(func, 12)
    return msg


def test_durable_multiple_logsegments(tmp_path):
    put_msgs = []
    get_msgs = []
    index_bytes = 2048
    log_bytes = 1024
    durable_queue = DurableQueue(
        name="queue-0",
        dirpath=str(tmp_path),
        index_max_bytes=index_bytes,
        log_max_bytes=log_bytes,
    )

    len_msg = len(rdx_msg().serialize())
    N = 3 * (log_bytes // len_msg)

    for _ in range(N):
        msg = rdx_msg()
        durable_queue.put_sync(msg)
        put_msgs.append(msg.id)

    assert len(durable_queue.ro_segments) == 2

    for _ in range(N):
        msg = durable_queue.get_sync()
        get_msgs.append(msg.id)

    assert len(durable_queue.index_segment.ready) == 0
    assert len(durable_queue.index_segment.delivered) == N
    assert put_msgs == get_msgs


def test_durable_multiple_logsegments_reopen(tmp_path):
    put_msgs = []
    get_msgs = []
    index_bytes = 2048
    log_bytes = 1024
    durable_queue = DurableQueue(
        name="queue-0",
        dirpath=str(tmp_path),
        index_max_bytes=index_bytes,
        log_max_bytes=log_bytes,
    )

    len_msg = len(rdx_msg().serialize())
    N = 3 * (log_bytes // len_msg)
    M = N // 2

    for _ in range(N):
        msg = rdx_msg()
        durable_queue.put_sync(msg)
        put_msgs.append(msg.id)

    assert durable_queue.qsize() == N
    assert len(durable_queue.ro_segments) == 2

    # Pop some elements
    for i in range(M):
        msg = durable_queue.get_sync()
        if i % 4 == 0:
            durable_queue.ack_sync(msg.delivered_timestamp, msg.id)
        get_msgs.append(msg.id)

    ready_prev = durable_queue.index_segment.ready
    delivered_prev = durable_queue.index_segment.delivered

    durable_queue.close()
    # Ropen the queue
    durable_queue = DurableQueue(
        name="queue-0",
        dirpath=str(tmp_path),
        index_max_bytes=index_bytes,
        log_max_bytes=log_bytes,
    )

    assert durable_queue.index_segment.ready == ready_prev
    assert durable_queue.index_segment.delivered == delivered_prev
