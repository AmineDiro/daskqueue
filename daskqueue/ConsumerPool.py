import os
import time
from typing import TypeVar
import itertools
from distributed import Actor, Client, LocalCluster

from daskqueue import ConsumerBaseClass, QueuePoolActor, GeneralConsumer
from daskqueue.QueuePool import QueuePool
from daskqueue.utils import logger

TConsumer = TypeVar("TConsumer", bound=ConsumerBaseClass)


class ConsumerPool:
    _counter = itertools.count()

    def __init__(
        self,
        client: Client,
        queue_pool: QueuePool,
        ConsumerClass: TConsumer = GeneralConsumer,
        n_consumers: int = 1,
        max_concurrency: int = 10000,
    ) -> None:
        if not issubclass(ConsumerClass, ConsumerBaseClass):
            raise Exception(
                "ConsumerClass passed should be a subclass of ConsumerBaseClasse"
            )
        self.n_consumers = n_consumers
        self.queue_pool = queue_pool.actor
        self.consumer_class = ConsumerClass
        self.consumers = {}
        for idx in range(n_consumers):
            name = f"{ConsumerClass.__name__}-{idx}"
            self.consumers[name] = client.submit(
                ConsumerClass,
                idx + 1,
                name,
                self.queue_pool,
                max_concurrency=max_concurrency,
                actor=True,
            ).result()

    # def __repr__(self) -> str:
    #     return f"ConsumerPool : \n\t{self.n_consumers} Consumer(s) \n\t{self.nb_consumed()} items consummed"

    def __repr__(self) -> str:
        consumer_info = [
            f"\n\t{c_name}: {consumer.len_items().result()} received, {consumer.len_pending_items().result()} pending tasks"
            for c_name, consumer in self.consumers.items()
        ]

        return f"Consumers : {self.n_consumers} Consumers(s)" + "".join(consumer_info)

    def __getitem__(self, idx: int) -> ConsumerBaseClass:
        return self.consumers.values[idx]

    def __len__(self) -> int:
        return len(self.consumers)

    def start(self, timeout: int = 1) -> None:
        """Start the consumme loop in each consumer"""
        logger.info(f"Starting {self.n_consumers} consumers")
        [c.start(timeout) for c in self.consumers.values()]

    def nb_consumed(self) -> None:
        """Return the total number of items consumed by our ConsumerPool"""
        return sum([c.len_items().result() for c in self.consumers.values()])

    def join(
        self, timestep: int = 2, print_timestep: int = 2, progress: bool = False
    ) -> None:
        """Join ConsumerPool will wait until all consumer are done processing items.
        Basically have processed all the elements of the queue_pool.
        We then cancel consumer to make sure the while loop is closed

        Args:
            timestep (int, optional): time step (in seconds) to wait between each check. Defaults to 2.
        """
        logger.info(
            f"Waiting for the {self.n_consumers} consumers to process all items in queue_pool..."
        )
        start_join = time.time()
        while True:
            done_consumers = all([c.done().result() for c in self.consumers.values()])
            if not done_consumers:
                if progress and (time.time() - start_join > print_timestep):
                    logger.debug("Still processing...")
                    logger.info(self.queue_pool.print().result())
                    logger.info(self)
                    start_join = time.time()
                time.sleep(timestep)
            else:
                logger.info(
                    f"All consumers are done ! {self.nb_consumed()} items processed. "
                )
                break
        self.cancel()

    def results(self) -> None:
        """Start the consumme loop in each consumer"""
        if not hasattr(self.consumer_class, "get_results"):
            raise NotImplementedError(
                "Please Implement a .get_results method in your ConsumerClass"
            )
        return {k: self.consumers[k].get_results().result() for k in self.consumers}

    def cancel(self) -> None:
        """Cancels the consume loop task in each consumer."""
        logger.info(f"Cancelling {self.n_consumers} consumers.")
        [c.cancel().result() for c in self.consumers.values()]
