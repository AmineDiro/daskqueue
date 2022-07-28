import uuid
from typing import Callable


class Message:
    """Message interface."""

    def __init__(self, *args, **kwargs) -> None:
        self.id = uuid.uuid4()
        self.func = args[0]
        self._data = (args[1:], kwargs)

    def is_callable(self) -> bool:
        return isinstance(self.func, Callable)

    def __str__(self) -> str:
        return f"Message {self.id} : function = {self.func}, args={self._data} "
