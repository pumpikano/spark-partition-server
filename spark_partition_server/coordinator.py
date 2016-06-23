# Copyright 2016, Yahoo Inc.
# Licensed under the terms of the Apache License, Version 2.0. See the LICENSE file associated with the project for terms.
import flask
from flask import request, Response, jsonify
import requests
import copy
from threading import Thread
from .thread_utils import ServerThread


class Coordinator(ServerThread):
    """
    A Coordinator is a server that received registration messages
    from hosts and establishes a list of all registered hosts.
    Hosts are stored in a dict that maps the partition index to
    a host,port pair for the server.

    In order to register, hosts must POST a json object containing
    keys 'partition', 'host', and 'port' to the /register route of
    the Coordinator. The Coordinator can shutdown all hosts when
    Coordinator.shutdown_hosts is called so long as all hosts provide
    a /control/shutdown route.

    The Coordinator is started and stopped by calling `start` and `stop`
    respectively.
    """
    def __init__(self, await_partitions=None, verbose=True, token=None):
        self.await_partitions = await_partitions
        self.verbose = verbose
        self.hosts = {}
        self.token = token
        self.register_callback = None

        # full_cluster will be None if the number of partitions
        # to await isn't specified
        self.full_cluster = False if await_partitions else None

        self._build_app()

        super(Coordinator, self).__init__(self.app)

    def _build_app(self):
        """A helper function to construct a Flask app."""

        self.app = flask.Flask('coordinator')

        @self.app.route('/register', methods=['POST'])
        def register():

            # Check request token
            if self.token and self.token != request.args.get('token'):
                return Response(status=403)

            j = request.get_json()
            partition, host, port = j['partition'], j['host'], j['port']

            old_entry = None
            if partition in self.hosts:
                old_entry = self.hosts[partition]

            self.hosts[partition] = (host, port)

            if self.verbose:
                print 'Registered partition %d at http://%s:%d' % (partition, host, port)

            if self.await_partitions == len(self.hosts):
                self.full_cluster = True
                if self.verbose:
                    print 'All %d expected partitions have registered' % (self.await_partitions)
                    self.print_hosts()

            if self.register_callback is not None:
                self.register_callback({
                    'partition_ind': partition,
                    'old_entry': old_entry,
                    'new_entry': (host, port),
                    'full_cluster': self.full_cluster
                })

            return Response(status=200)

        @self.app.route('/hosts', methods=['GET'])
        def get_hosts():
            return jsonify({
                'expected_partitions': self.await_partitions,
                'full_cluster': self.full_cluster,
                'hosts': self.hosts
            })

        @self.app.route('/status', methods=['GET'])
        def status():
            return jsonify({
                'expected_partitions': self.await_partitions,
                'current_partitions': len(self.hosts),
                'full_cluster': self.full_cluster
            })

    def shutdown_host(self, ind):
        """Shutdown the host on a given partition"""
        if ind in self.hosts:
            url = 'http://%s:%d/control/shutdown' % self.hosts[ind]
            if self.token:
                url = '%s?token=%s' % (url, self.token)
            requests.post(url)
            del self.hosts[ind]

    def shutdown_hosts(self):
        """Shutdown all hosts"""

        def thread_target(ind):
            self.shutdown_host(ind)

        threads = []

        # Copy hosts so that the dict does not change during iteration
        for k in copy.copy(self.hosts):
            thread = Thread(target=thread_target, args=(k,))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

        # Reset cluster state
        self.hosts = {}
        self.full_cluster = False if self.await_partitions else None

    def print_hosts(self):
        """A helper to print out all known hosts."""
        for k, d in self.hosts.iteritems():
            print '%d - http://%s:%d/' % (k, d[0], d[1])

    def set_register_callback(self, fn):
        self.register_callback = fn
