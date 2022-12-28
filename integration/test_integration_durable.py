from time import perf_counter

from daskqueue.Protocol import Message
from daskqueue.queue.durable_queue import DurableQueue

N = 1_000


def rdx_msg():
    func = lambda x: x + 2
    msg = Message(func, 12)
    return msg


def test_durable_queue(durable_queue: DurableQueue):
    put_msgs = []
    get_msgs = []
    s = perf_counter()
    for _ in range(N):
        msg = rdx_msg()
        durable_queue.put_sync(msg)
        put_msgs.append(msg.id)

    e = perf_counter()

    w_ops = N / (e - s)  # op/s
    print(f"Mean write ops [{N}tests] {w_ops:.2f} wop/s")

    for _ in range(N):
        msg = durable_queue.get_sync()
        get_msgs.append(msg.id)

    assert put_msgs == get_msgs
