import uuid
from typing import Callable


class Message:
    """Message interface."""

    def __init__(self, func, *args, **kwargs) -> None:
        self.id = uuid.uuid4()
        if not isinstance(func, Callable):
            raise RuntimeError
        self.func = func
        self._data = (args, kwargs)

    def __hash__(self) -> int:
        return hash(self.id)

    def __str__(self) -> str:
        return f"Message {self.id.int}"  #: \n\t\tfunction = {self.func.__name__} \n\t\targs={self._data[0]} \n\t\tkwargs={self._data[1]} "
