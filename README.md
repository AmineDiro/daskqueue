daskqueue
===============

[![code style](https://img.shields.io/pypi/v/daskqueue)](https://pypi.org/project/daskqueue/)
[![licence](https://img.shields.io/github/license/AmineDiro/daskqueue)](https://github.com/AmineDiro/daskqueue/blob/main/LICENSE.md)
[![issues](https://img.shields.io/github/issues/AmineDiro/daskqueue)](https://github.com/AmineDiro/daskqueue/issues)
[![code style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

**daskqueue** is small python library built on top of Dask and Dask Distributed that implements a very lightweight Distributed Task queue.

Think of this library as a simpler version of Celery built entirely on Dask. Running Celery on HPC environnment (for instance) is usually very tricky whereas spawning a Dask Cluster is a lot easier to manage, debug and cleanup.

Motivation
-------

[Dask](https://docs.dask.org/en/stable/) is an amazing library for parallel computing written entirely in Python. It is easy to install and offer both a high level API wrapping common collections (arrays, bags, dataframes) and a low level API for written custom code (Task graph with Delayed and Futures).

For all its greatness, Dask implements a central scheduler (basically a simple tornado eventloop) involved in every decision, which can sometimes create a central bottleneck. This is a pretty serious limitation when trying use Dask in high throughput situation. A simple Task Queue is usually the best approach when trying to distribute millions of tasks.

The **daskqueue** python library leverages [Dask Actors](https://distributed.dask.org/en/stable/actors.html) to implement distributed queues with a simple load balancer `QueuePool` and a `Consummer` class to consumme message from these queues.

We used Actors because:

- Actors are stateful, they  can hold on to and mutate state. They are allowed to update their state in-place, which is very useful when spawning distributed queues !

- **NO CENTRAL SCHEDULING NEEDED :** Operations on actors do not inform the central scheduler, and so do not contribute to the 4000 task/second overhead. They also avoid an extra network hop and so have lower latencies. Actors can communicate between themselves in a P2P manner, which makes it pretty neat when having a huge number of queues and consummers.

> **Note** :  Dask provides a [Queue implementation](https://docs.dask.org/en/stable/futures.html?highlight=queue#queues) but they are mediated by the central scheduler, and so they are not ideal for sending large amounts of data (everything you send will be routed through a central point) and add additionnal overhead on the scheduler when trying to put millions of tasks.


Install
-------

daskqueue requires Python 3.6 or newer.
You can install manually by cloning the repository:

```bash
$ pip install daskqueue
```


Usage
-----


This simple example show how to copy files in parallel using Dask workers and a distributed queue:

```python
from distributed import Client
from daskqueue import QueuePool, ConsumerPool
from daskqueue.utils import logger

def process_item():
    return sum(i * i for i in range(10**8))

if __name__ == "__main__":
    client = Client(
        n_workers=5,
        # task function doesn't release the GIL
        threads_per_worker=1,
        direct_to_workers=True,
    )

    ## Params
    n_queues = 1
    n_consumers = 5

    queue_pool = QueuePool(client, n_queues=n_queues)

    consumer_pool = ConsumerPool(client, queue_pool, n_consumers=n_consumers)
    consumer_pool.start()

    for i in range(5):
        queue_pool.submit(process_item)

    # Wait for all work to be done
    consumer_pool.join()

    ## Get results
    result = consumer_pool.results()

```

Take a look at the `examples/` folder to get some usage.


Implementation
-------
You should think of daskqueue as a very simple distributed version of aiomultiprocessing. We have three basic classes:
- `QueueActor`: Wraps a simple AsyncIO Queue object in a Dask Actor, providing an interface for putting and getting item in a **distributed AND asynchronous** fashion. Each queue runs in a separate Dask Worker and can interface with different actors in the cluster.
- `QueuePoolActor`: Basic Pool actor, it holds a reference to queues and their sizes. It interfaces with the Client and the Consummers. The QueuePool implements a simple scheduling on put and get :
    - On put : arbitrarly  chooses a random queue and puts item into it  then update the queue size reference
    - On get_max_queue : returns a queue with the highest item count then updates the queue size reference

- `ConsumerBaseClass`: Abstract class interfaces implementing all the fetching logic for you worker. You should build your own workers by inheriting from this class then spawning them in your Dask cluster. The Consumers have a `start()` method where we run an async while True loop to get a queue reference from the QueuePool then directly communicate with the Queue providing highly scalable workflows. The Consummer will then get an item form the queue and schedule `process_item` on the dask worker's ThreadPoolExecutor, freeing the worker's eventloop to communicate with the scheduler, fetch tasks asynchronously etc ....

Performance and Limitations
-------
### Benchmarks
The **daskqueue** library is very well suited for IO bound jobs: by running multiple consummers and queues, communication asynchronously, we can bypass the dask scheduler limit and process **millions of tasks 🥰 !! **

The example copy code above was ran on cluster of 20 consummers and 5 queues. The tasks ran are basic file copy between two location (copying form NFS filer). We copied 200 000 files (~ 1.1To) without ever breaking a sweat !

We can clearly see the network saturation:

![Image](figures/copy%20async.PNG)

Looking at the scheduler metrics, we can have a mean of 19.3%
![Image](figures/copy%20async3.PNG)

You can take a look at the `benchmark/` directory for various benchmarks ran using `daskqueue` vs `dask`:
- We put  1_000_000 tasks using dask cluster (2 nodes- 1 thread per process- 4 queues- 8 consumers)
- The tasks were chunked using  into 1000 calls of 1000 tasks per batch
- The client submits to the QueuePool manager using
- The function is 'empty' : just passes and doesn't use CPU or IO
- Processing 1_000_000 empty tasks took 338s = 5min36s 😸!!

### Limitations
As for the limitation, given the current implementation, you should be mindfull of the following limitations (this list will be updated regularly):
- The workers don't implement a min or max tasks fetched and scheduled on the eventloop, they will continuously fetch an item, process it etc...
- We run the tasks in the workers ThreadPool, we inherit all the limitations that the standard dask.submit method have.
- Task that require multiprocessing/multithreading within a worker cannot be scheduled at the time, although this is something we are currently working on implementing
- The QueuePool implement simple scheduling on put and get. Alternative schedulers may assign jobs to queues using arbitrary criteria, but no other scheduler implementation is available at this time for QueuePool.

TODO
-------
- [x] Consumer should run arbitrary funcs (ala celery)
- [x] Use Worker's thread pool for long running tasks ( probe finished to get results)
- [x] Wrap consummers in a Consummers class
- [x] Implement a Distributed Join to know when to stop cluster
- [x] Implement a `concurrency_limit` as the maximum number of active, concurrent jobs each worker process will pick up from its queue at once.
- [x] Implement the various Queue Exceptions
- [ ] CI/CD : Push to PyPI on release
- [ ] Implement reliability : tasks retries, acks mechanisms ... ?
- [ ] Notify dask dahboard ??
- [ ] Run tasks on custom Worker's executors
- [ ] Add benchmarks
- [ ] Tests
- [ ] Support async dask client
- [ ] Bypass Queue mechanism by using zeroMQ ?

Contributing
--------------
Contributions are what makes the open-source community such an amazing place to learn, inspire, and create.
This project is still very very rough! Any contributions you make will benefit everybody else and are greatly appreciated  😍 😍 😍 !

Please try to create bug reports that are:

- Reproducible. Include steps to reproduce the problem.
- Specific. Include as much detail as possible: which version, what environment, etc.
- Unique. Do not duplicate existing opened issues.
- Scoped to a Single Bug. One bug per issue.

Releasing
---------
Releases are published automatically when a tag is pushed to GitHub.

```bash
git checkout master
git pull
# Set next version number
export RELEASE=x.x.x

# Create tags
git commit --allow-empty -m "Release $RELEASE"
git tag -a $RELEASE -m "Version $RELEASE"

# Push
git push upstream --tags
```

License
-------

**daskqueue** is copyright **Amine Dirhoussi**, and licensed under
the MIT license.  I am providing code in this repository to you under an open
source license.  This is my personal repository; the license you receive to
my code is from me and not from my employer. See the `LICENSE` file for details.
