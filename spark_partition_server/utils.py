# Copyright 2016, Yahoo Inc.
# Licensed under the terms of the Apache License, Version 2.0. See the LICENSE file associated with the project for terms.
import socket


def get_open_port():
    """Find an open port

    Adapted from http://stackoverflow.com/questions/2838244/get-open-tcp-port-in-python
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("",0))
    s.listen(1)
    port = s.getsockname()[1]
    s.close()
    return port


def get_host():
    """Get the current hostname"""
    return socket.getfqdn()
