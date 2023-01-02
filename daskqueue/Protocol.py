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

        if not isinstance(func, Callable):
            raise RuntimeError
        self.data = partial(func, *args, **kwargs)

    def __hash__(self) -> int:
        return hash(self.id)

    def __str__(self) -> str:
        return f"Message {self.id.int}"  #: \n\t\tfunction = {self.func.__name__} \n\t\targs={self._data[0]} \n\t\tkwargs={self._data[1]} "

    def serialize(self) -> bytes:
        # return b"\x80\x05\x95f\x02\x00\x00\x00\x00\x00\x00\x8c\x12daskqueue.Protocol\x94\x8c\x07Message\x94\x93\x94)\x81\x94}\x94(\x8c\x02id\x94\x8c\x04uuid\x94\x8c\x04UUID\x94\x93\x94)\x81\x94}\x94\x8c\x03int\x94\x8a\x10}_\x9b\xcc\xc6b\x10\xac\xd6C6$;(\x0bKsb\x8c\ttimestamp\x94J\xfa\xad\xb2c\x8c\x04data\x94\x8c\tfunctools\x94\x8c\x07partial\x94\x93\x94\x8c\x17cloudpickle.cloudpickle\x94\x8c\x0e_make_function\x94\x93\x94(h\x11\x8c\r_builtin_type\x94\x93\x94\x8c\x08CodeType\x94\x85\x94R\x94(K\x01K\x00K\x00K\x01K\x02KCC\x08|\x00d\x01\x17\x00S\x00\x94NK\x02\x86\x94)\x8c\x01x\x94\x85\x94\x8c$/tmp/ipykernel_1512387/1906415462.py\x94\x8c\x08<lambda>\x94K\x04C\x02\x08\x00\x94))t\x94R\x94}\x94(\x8c\x0b__package__\x94N\x8c\x08__name__\x94\x8c\x08__main__\x94uNNNt\x94R\x94\x8c\x1ccloudpickle.cloudpickle_fast\x94\x8c\x12_function_setstate\x94\x93\x94h'}\x94}\x94(h$h\x1e\x8c\x0c__qualname__\x94h\x1e\x8c\x0f__annotations__\x94}\x94\x8c\x0e__kwdefaults__\x94N\x8c\x0c__defaults__\x94N\x8c\n__module__\x94h%\x8c\x07__doc__\x94N\x8c\x0b__closure__\x94N\x8c\x17_cloudpickle_submodules\x94]\x94\x8c\x0b__globals__\x94}\x94u\x86\x94\x86R0\x85\x94R\x94(h'K\x0c\x85\x94}\x94Nt\x94bub."
        return cloudpickle.dumps(self)
