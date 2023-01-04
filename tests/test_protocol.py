import pickle

from conftest import func
from daskqueue.Protocol import Message


def test_msg_serialize():
    msg = Message(func, 33)

    b = msg.serialize()
    msg_bis = pickle.loads(b)

    assert msg_bis.data() == msg.data()
