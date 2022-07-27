"""THis class is heavily inspired from the Ray's ActorPool.
 It tries to mirror how ray reimplements the map interface to loadbalance across Actors.

NOTE : Under heavy development !
"""

from typing import Any, Callable, List
from distributed import wait

# from actors.dask_actors import QueueActor
import asyncio
from .Queue import QueueActor


class ActorPool:
    """Utility class to operate on a fixed pool of actors.

    Arguments:
        actors (list): List of DaskActors to use in this pool.
    """

    def __init__(self, actors: List[QueueActor]):
        # actors to be used
        self._idle_actors = list(actors)

        # get actor from future
        self._future_to_actor = {}

        self._future_key = 0

        # get future from index
        self._index_to_future = {}

        # next task to do
        self._next_task_index = 0

        # next task to return
        self._next_return_index = 0

        # next work depending when actors free
        self._pending_submits = []

    def map(self, fn: Callable, values: Any = None):
        """Apply the given function in parallel over the actors and values.

        This returns an ordered iterator that will return results of the map
        as they finish. Note that you must iterate over the iterator to force
        the computation to finish.

        Arguments:
            fn (func): Function that takes (actor, value) as argument and
                returns an ObjectRef computing the result over the value. The
                actor will be considered busy until the ObjectRef completes.
            values (list): List of values that fn(actor, value) should be
                applied to.

        Returns:
            Iterator over results from applying fn to the actors and values.

        """
        # Ignore/Cancel all the previous submissions
        # by calling `has_next` and `gen_next` repeteadly.
        while self.has_next():
            try:
                self.get_next(timeout=0)
            except TimeoutError:
                pass

        if values is not None:
            for v in values:
                self.submit(fn, v)
        else:
            self.submit(fn)

        result = []
        while self.has_next():
            result.append(self.get_next())

        return result

    def submit(self, fn, value=None):
        """Schedule a single task to run in the pool.

        This has the same argument semantics as map(), but takes on a single
        value instead of a list of values. The result can be retrieved using
        get_next() / get_next_unordered().

        Arguments:
            fn (func): Function that takes (actor, value) as argument and
                returns an ObjectRef computing the result over the value. The
                actor will be considered busy until the ObjectRef completes.
            value (object): Value to compute a result for.

        """
        if self._idle_actors:
            actor = self._idle_actors.pop()
            if value is not None:
                future = fn(actor, value)
            else:
                future = fn(actor)
            self.future_key = tuple(future) if isinstance(future, list) else future
            self._future_to_actor[self._future_key] = (self._next_task_index, actor)
            self._future_key += 1
            self._index_to_future[self._next_task_index] = future
            self._next_task_index += 1
            # print(
            #     f"future_key : {self._future_key}, future : {future}, result : {future.result()}"
            # )
            print("index to future: ", self._index_to_future)
            print("future to actor", self._future_to_actor)
        else:
            self._pending_submits.append((fn, value))

    def has_next(self):
        """Returns whether there are any pending results to return.

        Returns:
            True if there are any pending results not yet returned.

        """
        return bool(self._future_to_actor)

    def get_next(self, timeout=None):
        """Returns the next pending result in order.

        This returns the next result produced by submit(), blocking for up to
        the specified timeout until it is available.

        Returns:
            The next result.

        Raises:
            TimeoutError if the timeout is reached.

        """
        if not self.has_next():
            raise StopIteration("No more results to get")
        if self._next_return_index >= self._next_task_index:
            raise ValueError(
                "It is not allowed to call get_next() after get_next_unordered()."
            )
        future = self._index_to_future[self._next_return_index]
        if timeout is not None:
            res, _ = wait([future], timeout=timeout)
            if not res:
                raise TimeoutError("Timed out waiting for result")
        del self._index_to_future[self._next_return_index]
        self._next_return_index += 1

        future_key = tuple(future) if isinstance(future, list) else future
        i, a = self._future_to_actor.pop(future_key)

        self._return_actor(a)

        print("result returned : ", future.result())

        return future.result()

    def _return_actor(self, actor):
        self._idle_actors.append(actor)
        if self._pending_submits:
            self.submit(*self._pending_submits.pop(0))

    def has_free(self):
        """Returns whether there are any idle actors available.

        Returns:
            True if there are any idle actors and no pending submits.

        """
        return len(self._idle_actors) > 0 and len(self._pending_submits) == 0

    def pop_idle(self):
        """Removes an idle actor from the pool.

        Returns:
            An idle actor if one is available.
            None if no actor was free to be removed.

        """
        if self.has_free():
            return self._idle_actors.pop()
        return None

    def push(self, actor):
        """Pushes a new actor into the current list of idle actors."""
        busy_actors = []
        if self._future_to_actor.values():
            _, busy_actors = zip(*self._future_to_actor.values())
        if actor in self._idle_actors or actor in busy_actors:
            raise ValueError("Actor already belongs to current ActorPool")
        else:
            self._idle_actors.append(actor)
