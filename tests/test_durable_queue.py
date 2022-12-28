from daskqueue.Protocol import Message
from daskqueue.queue.durable_queue import DurableQueue


def test_init_durable_queue(tmp_path):
    queue = DurableQueue(name="queue-0", dirpath=str(tmp_path))

    # segment name
    assert int(queue.active_segment.name) == 0
    assert queue.index_segment.name == "default-queue-0"

    # Mem structure
    assert len(queue.ro_segments) == 0
    assert len(queue.index_segment) == 0


def test_queue_push_sync(durable_queue: DurableQueue, msg: Message):
    rec = durable_queue.put_sync(msg)

    assert rec.msg_id == msg.id
    assert len(durable_queue.index_segment) == 1
    assert durable_queue.active_segment.w_cursor == rec.offset.offset + rec.offset.size


def test_queue_get_sync(durable_queue: DurableQueue, msg: Message):
    [durable_queue.put_sync(msg) for _ in range(4)]
    pop_msg = durable_queue.get_sync()

    assert msg.id == pop_msg.id
    assert msg.data() == pop_msg.data()
