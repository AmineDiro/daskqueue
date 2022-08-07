import itertools
from typing import Generator, Iterable
from daskqueue.Protocol import Message


def msg_grouper(n, iterable: Iterable, **kwargs) -> Generator:
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, n))
        if not chunk:
            return
        yield [Message(func, *args, **kwargs) for func, *args in chunk]
