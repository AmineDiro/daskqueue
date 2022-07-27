import time
from typing import TypeVar

from distributed import Actor, Client, LocalCluster

from daskqueue import ConsumerBaseClass, QueuePool
from daskqueue.utils import logger

TConsumer = TypeVar("TConsumer", bound=ConsumerBaseClass)


class ConsumerPool:
    def __init__(
        self,
        client: Client,
        ConsumerClass: TConsumer,
        n_consumers: int,
        queue_pool: QueuePool,
    ) -> None:
        if not issubclass(ConsumerClass, ConsumerBaseClass):
            raise Exception(
                "ConsumerClass passed should be a subclass of ConsumerBaseClasse"
            )
        self.n_consumers = n_consumers
        self.queue_pool = queue_pool
        self.consumers = [
            client.submit(ConsumerClass, queue_pool, actor=True).result()
            for _ in range(n_consumers)
        ]

    def start(self) -> None:
        """Start the consumme loop in each consumer"""
        logger.info(f"Starting {self.n_consumers} consumers")
        [c.start() for c in self.consumers]

    def nb_consumed(self) -> None:
        """Return the number of items consumed by our ConsumerPool"""
        return sum([c.len_items().result() for c in self.consumers])

    def join(self, timestep: int = 2) -> None:
        """Join ConsumerPool will wait until all consumer are done processing items.
        Basically have processed all the elements of the queue_pool.
        We then cancel consumer to make sure the while loop is closed

        Args:
            timestep (int, optional): time step (in seconds) to wait between each check. Defaults to 2.
        """
        logger.info(
            f"Waiting for the {self.n_consumers} consumers to process all items in queue_pool..."
        )
        while True:
            done_consumers = sum([c.done().result() for c in self.consumers])
            if done_consumers < len(self.consumers):
                logger.debug(
                    f"[{done_consumers}/{len(self.consumers)} done]. Still processing..."
                )
                time.sleep(timestep)
            else:
                logger.debug(
                    f"[{done_consumers}/{len(self.consumers)}]. All consumers are done !"
                )
                break
        self.cancel()

    def cancel(self) -> None:
        """Cancels the consume loop task in each consumer."""
        logger.info(f"Cancelling {self.n_consumers} consumers.")
        [c.cancel() for c in self.consumers]
