#!/usr/bin/python3
from distributed import Client, Queue, LocalCluster

if __name__ == "__main__":
    cluster = LocalCluster(
        dashboard_address=":42166", threads_per_worker=1, n_workers=20
    )
    client = Client(cluster)

    print(cluster.scheduler_address)
