# Copyright 2016, Yahoo Inc.
# Licensed under the terms of the Apache License, Version 2.0. See the LICENSE file associated with the project for terms.
from flask import request
import requests
from threading import Thread
from .utils import get_host, get_open_port


class ServerThread(Thread):
    """
    Given a Flask app, a ServerThread runs the server in a separate
    thread, choosing an open port for the server if no port is specified.

    A '/control/shutdown' POST route is added to the server to enable it
    to be cleanly shutdown remotely.

    A ServerThread can be run by calling its `start` method. Subsequently
    calling its `shutdown` method will stop the server and the thread.
    """
    def __init__(self, app, port=None):
        super(ServerThread, self).__init__()
        self.port = port
        self.host = get_host()
        self.app = app

        # Add shutdown hook to app
        @self.app.route('/control/shutdown', methods=['POST'])
        def shutdown_server():
            # Adapted from http://flask.pocoo.org/snippets/67/
            func = request.environ.get('werkzeug.server.shutdown')
            if func is None:
                raise RuntimeError('Not running with the Werkzeug Server')
            func()
            return 'Server shutting down...'

    def run(self):
        """
        This overrides Thread.run and shouldn't be called directly (if it is
        the server will run in the calling thread). Call the `start`method
        to start the server in a separate thread.
        """
        # Get port if not assigned
        if self.port is None:
            self.port = get_open_port()

        self.app.run(threaded=True, host='0.0.0.0', port=self.port)

    def shutdown(self):
        """Cleanly shutdown the server by calling its /control/shutdown route"""
        requests.post('http://0.0.0.0:%d/control/shutdown' % self.port)

    def get_url(self):
        """
        Return the url of the server.
        """
        if self.host is None or self.port is None:
            return None
        else:
            return 'http://%s:%d' % (self.host, self.port)


class MapPartitionsThread(Thread):
    """
    A MapPartitionsThread is a thread that submits a mapPartitionsWithIndex job
    to the Spark cluster. It optionally caches the output RDD.
    """
    def __init__(self, rdd, partition_server, cache_result=False):
        super(MapPartitionsThread, self).__init__()
        self.rdd = rdd
        self.partition_server = partition_server
        self.cache_result = cache_result
        self.result = None

    def run(self):
        self.result = self.rdd.mapPartitionsWithIndex(self.partition_server, preservesPartitioning=True)
        if self.cache_result:
            self.result.cache()
        self.result.count()

