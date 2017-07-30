# -*- coding: utf-8 -*-

import os, socket
import collections


def create_listen_socket(addr, backlog=10):
    # accept strings "<address>:<port>" or "some/file/name" for unix sockets,
    # or 2-tuple (addr, port).

    if isinstance(addr, tuple):
        lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        lsock.bind(addr)

    elif ':' in addr:
        addr, port = addr.strip().split(':')
        addr = (addr, int(port))
        lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        lsock.bind(addr)
    else:
        # unix socket
        if os.path.exists(addr_str):
            os.remove(addr_str)
        lsock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        addr = addr_str
        lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        lsock.bind(addr)
        os.chmod(addr_str, 0o777)

    lsock.listen(backlog)

    return lsock


class CaseInsDict(collections.MutableMapping):

    def __init__(self, data=None, **kw):

        self._data = {}
        self.update(data if data is not None else {}, **kw)


    def __str__(self):
        return '{}{}'.format(self.__class__.__name__, dict(self._data.values()))

    def __setitem__(self, k, v):
        self._data[k.lower()] = (k, v)

    def __getitem__(self, k):
        return self._data[k.lower()][1]

    def __delitem__(self, k):
        del self._data[k.lower()]

    def __len__(self):
        return len(self._data)

    def __iter__(self):
        return (item[0] for item in self._data.values())

    def copy(self):
        return CaseInsDict(self._data.values())






