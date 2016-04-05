# Copyright 2016, Yahoo Inc.
# Licensed under the terms of the Apache License, Version 2.0. See the LICENSE file associated with the project for terms.
import atexit
import binascii
import os
from time import sleep
from .thread_utils import MapPartitionsThread
from .partition_server import FlaskPartitionServer
from .coordinator import Coordinator


class Cluster(object):
    """
    This is the base class for a Cluster. A Cluster can be created with the SparkContext,
    the RDD to launch a cluster over, and a PartitionServer instance. Subclasses of Cluster
    may encapsulate the PartitionServer by creating it on init if appropriate. Cluster
    subclasses are also a convenient place to define via methods how client code can interact
    with the running cluster.
    """
    def __init__(self, sc, rdd, partition_server=None, cache_result=False, verbose=True):
        """
        :param SparkContext sc:
            the SparkContext
        :param RDD rdd:
            an RDD to run a cluster on
        :param PartitionServer partition_server:
            a PartitionServer subclass to execute on partitions
        :param bool cache_result:
            flag to cache the result RDD when the cluster is shutdown
        :param bool verbose:
            flag for verbose logging
        """
        self.sc = sc
        self.rdd = rdd
        self.partition_server = partition_server if partition_server else FlaskPartitionServer()
        self.cache_result = cache_result
        self.verbose = verbose

        self.coordinator = None
        self._is_active = False
        self.token = None

        def cleanup():
            if self.is_active():
                self.stop()

                # If a map thread exists, wait for partition servers to exit
                # on the executors for a clean shutdown
                if self.map_job and self.map_job.is_alive():
                    self.map_job.join()

        atexit.register(cleanup)

    def is_active(self):
        """Return whether the cluster is currently running"""
        return self._is_active

    def get_hosts(self):
        """Get a dict mapping partition index to pairs of host and port for all partition servers"""
        if self.coordinator:
            return self.coordinator.hosts
        else:
            return None

    def start(self, await_hosts=False):
        """Start the cluster

        :param bool await_hosts
            if True, this method blocks until all expected partition servers have registered themselves
        """
        if self.is_active():
            return

        # Generate a token to identify servers as belonging to this cluster
        self.token = binascii.hexlify(os.urandom(10))

        num_partitions = self.rdd.getNumPartitions()
        total_cores = int(self.sc._conf.get('spark.executor.instances')) * int(self.sc._conf.get('spark.executor.cores'))

        if self.verbose:
            print 'Preparing partition servers for RDD %d with %d partitions on %d cores' % (self.rdd.id(), num_partitions, total_cores)

        # Build a coordinator to manage the cluster and start it
        self.coordinator = Coordinator(await_partitions=num_partitions, verbose=self.verbose, token=self.token)
        self.coordinator.daemon = True
        self.coordinator.start()

        # Await coordinator startup
        coordinator_url = None
        while coordinator_url is None:
            coordinator_url = self.coordinator.get_url()

        # Provide the partition server with the coordinator url and the cluster token
        self.partition_server.set_coordinator_url(coordinator_url)
        self.partition_server.set_token(self.token)

        # start paritition servers
        self.map_job = MapPartitionsThread(self.rdd, self.partition_server, self.cache_result)
        self.map_job.daemon = True
        self.map_job.start()

        self._is_active = True

        while await_hosts and not self.coordinator.full_cluster:
            sleep(0.1)

    def stop(self):
        """Stop the cluster"""
        self.coordinator.shutdown_hosts()
        self.coordinator.shutdown()
        self._is_active = False

    def get_result_rdd(self):
        """If the cluster cached the result RDD, return it.

        Note that if the cluster is subsequently started, the reference to the RDD will be lost.
        It is up to clients to unpersist the RDD as necessary.
        """
        if self.cache_result and self.map_job:
            return self.map_job.result
        else:
            return None

