# Copyright 2016, Yahoo Inc.
# Licensed under the terms of the Apache License, Version 2.0. See the LICENSE file associated with the project for terms.
import requests
from .utils import get_open_port, get_host


class PartitionServer(object):
    """
    This is an abstract class for a PartitionServer. A PartitionServer is a server
    is a callable object that will create a server when it is called and call back
    to a coordinator to register its host and port. It it intended to be passed as
    a lambda to RDD.mapPartitionsWithIndex and should not return until the server
    is shutdown.
    """
    def __init__(self, port=None, config={}):
        """
        :param int port:
            an optional port
        :param dict config:
            an optional dict of configuration data
        """
        self.port = port
        self.config = config
        self.token = None

    def set_coordinator_url(self, url):
        """
        A PartitionServer must be provided a url for the coordinator before it is
        shipped to the executors so that it can register.
        """
        self.coordinator_url = url

    def set_token(self, token):
        self.token = token

    def _register(self):
        """Register with coordinator"""
        # TODO: if this partition is empty, tell the coordinator
        url = '%s/register' % self.coordinator_url

        if self.token:
            url = '%s?token=%s' % (url, self.token)

        requests.post(url, json={ "partition": self.partition_ind, "host": self.host, "port": self.port })

    def __call__(self, ind, itr):
        """
        This method is invoked by Spark. It does basic boilerplate
        state management and calls _launch_server, which should be
        overridden by subclasses to start a server on this partition.
        When it exits, it will call _build_result which can also be
        overridden by subclasses to return an iterable containing any
        state that should be saved.
        """
        self.partition_ind = ind;
        self.itr = itr
        self.host = get_host()
        if self.port is None:
            self.port = get_open_port()

        self._launch_server()

        return self._build_result()

    def _build_result(self):
        """Override to return state after the server is terminated"""
        return []

    def _launch_server(self):
        """Implement this method to start a server or perform work on the partition

        Subclasses must call self._register() in this method to register with the
        Coordinator. They also must launch an HTTP server on self.port with a
        /control/shutdown POST endpoint to respond to shutdown requests. Otherwise,
        subclasses are free to implement any suitable application logic.
        """
        raise NotImplementedError


class FlaskPartitionServer(PartitionServer):
    """A FlaskPartitionServer starts a Flask app on a partition.

    It can be initialized with a flask.Blueprint (which will be launched
    with the /app URL prefix) and an init function to setup in any
    datastructures or state for the server.

    The initialize function will be called with a partition iterator,
    the Flask app object, and the config object.
    """
    def __init__(self, blueprint=None, init_partition=None, **kwargs):
        super(FlaskPartitionServer, self).__init__(**kwargs)
        self.blueprint = blueprint 
        self._init_partition_fn = init_partition
        self.app = None
        self.shutdown_callback = None

    def _init_partition(self):
        if self._init_partition_fn:
            self._init_partition_fn(self.itr, self.app, self.config)
        else:
            try:
                self.init_partition(self.itr, self.app, self.config)
            except AttributeError:
                pass

    def _launch_server(self):
        """Create a Flask server with the provided blueprint and init function"""
        import flask
        from flask import request, Response

        # Create the flask partition server
        self.app = app = flask.Flask('FlaskPartitionServer%d' % self.partition_ind)

        # Add partition, host, port information to config for use by blueprint
        app.config.update(
            PARTITION=self.partition_ind,
            HOST=self.host,
            PORT=self.port,
            PARTITION_ITERATOR=self.itr,
            PARTITION_SERVER=self
        )

        self._init_partition()

        # Register the blueprint if provided
        if self.blueprint:
            app.register_blueprint(self.blueprint, url_prefix='/app')

        @app.route('/control/shutdown', methods=['POST'])
        def shutdown_server():

            # Check request token
            if self.token and self.token != request.args.get('token'):
                return Response(status=403)

            # Call shutdown callback
            if self.shutdown_callback is not None:
                self.shutdown_callback()

            # http://flask.pocoo.org/snippets/67/
            func = request.environ.get('werkzeug.server.shutdown')
            if func is None:
                raise RuntimeError('Not running with the Werkzeug Server')
            func()
            return 'Server shutting down...'

        @app.route('/control/ping', methods=['POST', 'GET'])
        def ping():
            return Response(status=200)

        self._register()

        # start server
        app.run(threaded=True, host='0.0.0.0', port=self.port)

    def set_shutdown_callback(self, fn):
        self.shutdown_callback = fn

