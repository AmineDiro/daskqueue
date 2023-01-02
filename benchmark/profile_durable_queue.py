import cProfile
import tempfile

from daskqueue.Protocol import Message
from daskqueue.queue import DurableQueue

N = 10_000
N_TEST = 10
MAX_BYTES = 100 * int(1e6)  # 100 MB


func = lambda x: x + 2


def rdx_msg():
    msg = Message(func, 12)
    return msg


def read_write_queue(queue, msg):
    queue.put_sync(msg)
    pop_msg = queue.get_sync()
    return pop_msg


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as tmpdirname:
        queue = DurableQueue(name="queue-0", dirpath=str(tmpdirname))
        msg = rdx_msg()

        cProfile.run("read_write_queue(queue,msg)", filename="benchmark/queue.prof")
