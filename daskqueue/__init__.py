from . import _version
from .ActorPool import ActorPool
from .Consumer import ConsumerBaseClass, DummyConsumer, GeneralConsumer
from .ConsumerPool import ConsumerPool
from .Protocol import Message
from .queue.simple_queue import QueueActor
from .QueuePool import *

__version__ = _version.get_versions()["version"]
