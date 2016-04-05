# Spark Partition Server

`spark-partition-server` is a set of light-weight Python components to launch servers on the executors of a Spark cluster.

## Overview

Spark is designed for manipulating and distributing data within the cluster, but not for allowing clients to interact with the data directly. `spark-partition-server` provides primitives for launching arbitrary servers on partitions of an RDD, registering and managing the partitions servers on the driver, and collecting any resulting RDD after the partition servers are shutdown.

There are many use-cases such as building ad hoc search clusters to query data more quickly by skipping Spark's job planning, allowing external services to interact directly with in-memory data on Spark as part of a computing pipeline, and enabling distributed computations amongst executors involving direct communication. Spark Partition Server itself provides building blocks for these use cases.

## Simple Usage Example

There are two core classes: PartitionServer and Cluster. A PartitionServer will run on partitions and a Cluster runs on the driver to launch and shutdown a cluster of PartitionServers. 

`FlaskPartitionServer` is a simple `PartitionServer` that launches a Flask app on partitions with a user-defined Flask Blueprint. Let's define a simple one with a single `get_list` route that returns a comma-separated string of data on the partition - not particularly useful, but good enough for a demo.

```python
from flask import Blueprint
import requests
from spark_partition_server import Cluster, FlaskPartitionServer

# Define a simple Flask server to run on partitions
class DemoPartitionServer(FlaskPartitionServer):
    def __init__(self, **kwargs):

        # Define server as Flask Blueprint
        blueprint = Blueprint('app', __name__)

        @blueprint.route('/get_list')
        def get_list():
            return ','.join(map(str, self.partition))

        super(DemoPartitionServer, self).__init__(blueprint=blueprint, **kwargs)

    def init_partition(self, itr, app, config):
        self.partition = list(itr)
```

The optional `init_partition` method builds any necessary state before the server is started. `FlaskPartitionServer` can be used without subclassing as well for quick and dirty prototyping or on the prompt.

```python
# Define a Cluster class to query partitions
class DemoCluster(Cluster):
    def __init__(self, *args, **kwargs):
        kwargs['partition_server'] = DemoPartitionServer()
        super(DemoCluster, self).__init__(*args, **kwargs)

    def get_partition_list(self, ind):
        url = 'http://%s:%d/app/get_list' % self.coordinator.hosts[ind]
        rsp = requests.get(url).text
        return map(int, rsp.split(','))
```

Our `Cluster` subclass uses our `DemoPartitionServer` and provides a simple method to query a single partition. A `Cluster` subclass is a good place to encapsulate ways your application can interact with the cluster.

```python
# Create an RDD with 2 partitions
rdd = sc.parallelize(range(1000), 2)

# Create and start a cluster on this RDD
c = DemoCluster(sc, rdd)
c.start()

# Fetch data from partition 0
c.get_partition_list(0)

# Shutdown the cluster (it can be restarted anytime)
c.stop()
```

## Architecture

A `Cluster` must always be created with an RDD of the desired number of partitions to serve and a `PartitionServer` subclass to launch. When a `Cluster` is started, it starts a simple coordinator server and launches a mapPartitions job with Spark, both in separate threads on the driver. The `PartitionServer` will be initialized on each partition, register its partition index, hostname, and port with the coordinator, and then do whatever it should for the application. Any failure in a `PartitionServer` will naturally trigger Spark to launch a new server on the partition, providing convenient robustness. The cluster is shutdown by calling a shutdown endpoint on all PartitionServers. `PartitionServers` can optionally return data as they exit that will be form a result RDD available on the driver.

### PartitionServer

The `PartitionServer` class is an abstract base class for partition servers. Subclasses must implement the `_launch_server` method to register the server with the coordinator by calling `_register()` and launch a HTTP server to listen on a `/control/shutdown` route and respond appropriately. Otherwise, subclasses are free to implement any other application logic.

#### FlaskPartitionServer

The `FlaskPartitionServer` subclass implements a simple way to launch a Flask server on a partition. It is initialized with a Flask Blueprint, which will launch under the `/app` URL prefix, and an optional initialization function which receives the partition iterator, the Flask app object, and a config dict (provided as a keyword arg when the `FlaskPartitionServer` is created) as arguments. The init function can modify state as necessary to set up the server. Below is a simple example that converts the partition to a list and provides an endpoint that return a concatentation of the partition.

```python
blueprint = Blueprint('app', __name__)
@blueprint.route('/concat')
def concat():
    from flask import current_app
    return ','.join(map(str, current_app.config['DATA']))

def init(itr, app, config):
    app.config.update(DATA=list(itr))
    
s = FlaskPartitionServer(blueprint=blueprint, init_partition=init)
```

Subclasses of `FlaskPartitionServer` can create the Blueprint itself implement `init_partition` directly as a method - this is more convenient in many cases because the app and the init method have access to any state stored on the instance. Here is the same server as above implemented as a subclass:

```python
class DemoPartitionServer(FlaskPartitionServer):
	def __init__(self, **kwargs):
		blueprint = Blueprint('app', __name__)
		@blueprint.route('/concat')
		def concat():
			return ','.join(map(str, self.partition))

		super(DemoPartitionServer, self).__init__(blueprint=blueprint, **kwargs)

	def init_partition(self, itr, app, config):
		self.partition = list(itr)
```

### Cluster

A `Cluster` represents the set of partition servers on the driver. It requires the SparkContext, an RDD, and a `PartitionServer`:

```python
rdd = sc.parallelize(range(1000), 2)

c = Cluster(sc, rdd, DemoPartitionServer())
c.start()

# ... later, to shutdown the cluster...
c.stop()
```

While the cluster is running, host information is available from `c.get_hosts()` as a dict from partition index to (hostname, port) tuples. Subclasses can implement methods to interact with the hosts as appropriate for the application.

### Getting results

`PartitionServer` subclasses can override the `_build_result` method to return data. This data might be the result of some computation, log data from the server, or anything else depending on application. A `Cluster` that launches a `PartitionServer` subclass that implements `_build_result` can capture this data in a cached RDD by initializing with `cache_result=True`:

```python
rdd = sc.parallelize(range(1000), 2)

c = Cluster(sc, rdd, DemoPartitionServer(), cache_result=True)
c.start()

# ... later ...
c.stop()

result_rdd = c.get_result_rdd()
```

Note that this RDD should be uncached before the cluster is started again, otherwise the reference will be lost.

## License

Code licensed under the Apache License, Version 2.0 license. See LICENSE file for terms.
