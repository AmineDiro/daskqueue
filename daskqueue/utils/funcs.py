import itertools
from typing import Generator, Iterable

from daskqueue.Protocol import Message


def msg_grouper(n: int, iterable: Iterable, **kwargs) -> Generator:
    it = iter(iterable)
    while True:
        chunk = list(itertools.islice(it, n))
        if chunk:
            yield [Message(func, *args, **kwargs) for func, *args in chunk]
        else:
            break
