import pickle
import time
import uuid
from functools import partial
from typing import Callable

import cloudpickle


class Message:
    """Message interface."""

    def __init__(self, func, *args, **kwargs) -> None:
        self.id = uuid.uuid4()
        self.timestamp = int(time.time())
        self.delivered_timestamp = None

        if not isinstance(func, Callable):
            raise RuntimeError
        self.data = partial(func, *args, **kwargs)

    def __hash__(self) -> int:
        return hash(self.id)

    def __str__(self) -> str:
        return f"Message {self.id.int}"  #: \n\t\tfunction = {self.func.__name__} \n\t\targs={self._data[0]} \n\t\tkwargs={self._data[1]} "

    def serialize(self) -> bytes:
        try:
            return pickle.dumps(self)
        except pickle.PicklingError:
            return cloudpickle.dumps(self)
