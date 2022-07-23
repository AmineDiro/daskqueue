daskqueue
===============

Take a modern Python codebase to the next level of performance.

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

> **Note** :  Dask provides a [Queue implementation](https://docs.dask.org/en/stable/futures.html?highlight=queue#queues) but they are mediated by the central scheduler, and so they are not ideal for sending large amounts of data (everything you send will be routed through a central point). 


Install
-------

daskqueue requires Python 3.6 or newer.
You can install manually by cloning the repository:

```bash
$ git clone https://github.com/AmineDiro/daskqueue.git
$ cd daskqueue/
$ pip install .
```


Usage
-----

Take a look at the `examples/` folder. This simple example show how to copy files in parallel using Dask workers and a distributed queue

```python
from distributed import Client, Queue
from daskqueue import Consumer, QueuePool

def get_random_msg(start_dir:str,list_files:List[str],size:int)->List[Tuple[str,str]]: 
    pass

if __name__ == "__main__":
    client = Client(address="scheduler_address")

    n_queues = 5 
    n_consummers = 20 
    start_dir = ""
    dst_dir= ""

    # Queue Pool  with basic load balancing
    queue_pool = client.submit(QueuePool, n_queues, actor=True).result()

    # Start Consummers
    consummers = []
    for _ in range(n_consummers):
        f_consummer = client.submit(Consumer, queue_pool, actor=True)
        consummer = f_consummer.result()
        consummers.append(consummer)
        ## Start each consummer
        consummer.start()

    # Parallel file copy
    l_files = os.listdir(start_dir)

    for _ in range(100):
        ## We chunk the files into 
        msg = get_random_msg(start_dir,l_files,size=1000)
        queue_pool.put_many(msg)
```

> ❗For now the only think a consummer does is copy files 😄 ! You should change the `Consummer` class


Implementation  
-------
You should think of daskqueue as a very simple distributed version of aiomultiprocessing. We have three basic classes: 
- `QueueActor`: Wraps a simple AsyncIO Queue object in a Dask Actor, providing an interface for putting and getting item in a **distributed AND asynchronous** fashion. Each queue runs in a separate Dask Worker and can interface with different actors in the cluster.
- `QueuePool`: Basic Pool actor, it holds a reference to queues and their sizes. It interfaces with the Client and the Consummers. We implemented very simple load balacing on put and get calls
- `Consummer`: Should run as a dask actor, it has a `start()` method where we run an async loop to get a queue reference from the QueuePool then directly communicating with the Queue providing highly scalable workflows.

> 🚩 Note : Because each Consummer uses the Worker's event loop, it is recommanded to run a dask cluster with thread_per_core=1. 

Performance and Limitations
-------
The **daskqueue** library is very well suited for IO bound jobs: by running multiple consummers and queues, communication asynchronously, we can bypass the dask scheduler limit and process **millions of tasks 🥰 !! ** 

The example copy code above was ran on cluster of 20 consummers and 5 queues. The tasks ran are basic file copy between two location (copying form NFS filer). We copied 200 000 files (~ 1.1To) without ever breaking a sweat !

We can clearly see the network saturation: 

![Image](figures/copy%20async.PNG)

Looking at the scheduler metrics, we can have a mean of 19.3% 
![Image](figures/copy%20async3.PNG)


TODO
-------
- [ ] Consummer should run arbitrary funcs (ala celery)
- [ ] Implement a Distributed Join to know when to stop cluster
- [ ] Implement the various Queue Exceptions
- [ ] Wrap consummers in a Consummers class
- [ ] Bypass Queue mechanism by using zeroMQ ?
- [ ] Tests 

Contributing
--------------
Contributions are what makes the open-source community such an amazing place to learn, inspire, and create.  
This project is still very very rough! Any contributions you make will benefit everybody else and are greatly appreciated  😍 😍 😍 ! 

Please try to create bug reports that are:

- Reproducible. Include steps to reproduce the problem.
- Specific. Include as much detail as possible: which version, what environment, etc.
- Unique. Do not duplicate existing opened issues.
- Scoped to a Single Bug. One bug per issue.

License
-------

**daskqueue** is copyright [Amine Dirhoussi](https://jreese.sh), and licensed under
the MIT license.  I am providing code in this repository to you under an open
source license.  This is my personal repository; the license you receive to
my code is from me and not from my employer. See the `LICENSE` file for details.


