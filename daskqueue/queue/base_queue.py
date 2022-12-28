from abc import ABC, abstractclassmethod
from enum import Enum, auto
from typing import Optional


class Durability(Enum):
    DURABLE = auto()
    TRANSIENT = auto()


class BaseQueue(ABC):
    def __init__(self, durability: Durability, maxsize: Optional[int] = None) -> None:
        self.durability = durability
        self.maxsize = maxsize

    @abstractclassmethod
    async def put(self):
        raise NotImplementedError("Need an async put method")

    @abstractclassmethod
    async def put_many(self):
        raise NotImplementedError("Need an async put method")

    @abstractclassmethod
    async def get(self):
        raise NotImplementedError("Needs an async get method")

    @abstractclassmethod
    def qsize(self):
        raise NotImplementedError("Needs a qsize method ")
