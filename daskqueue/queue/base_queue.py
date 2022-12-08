from abc import ABC, abstractclassmethod, abstractmethod


class BaseQueue(ABC):
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
