import cloudpickle
import pytest

from daskqueue.Protocol import Message


def test_msg_serialize():
    func = lambda x: x + 4
    msg = Message(func, 33)
    b = msg.serialize()
    msg_bis = cloudpickle.loads(b)

    assert msg_bis.data() == msg.data()
