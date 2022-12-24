from abc import ABC, abstractclassmethod, abstractmethod
from enum import Enum
from typing import Optional


class Durability(Enum):
    DURABLE: 1
    TRANSIENT: 2


class BaseQueue(ABC):
    def __init__(self, durability: Durability, maxsize: Optional[int] = None) -> None:
        self.durability = durability
        self.maxsize = maxsize if maxsize else 1000000  # TODO: cap this limit

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
