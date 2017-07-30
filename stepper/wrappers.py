#! /usr/bin/env python3
# -*- config: utf-8 -*-

'''
openssl s_client -connect localhost:5000

openssl genrsa -des3 -out mykey.orig.key 2048
openssl rsa -in mykey.orig.key -out mykey.key
openssl req -new -key mykey.key -out mykey.csr
openssl x509 -req -days 365 -in mykey.csr -signkey mykey.key -out mykey.crt
'''

import sys, socket, ssl
from ssl import SSLWantReadError, SSLWantWriteError

from .stepper import *

def make_wrapper(obj):

    if isinstance(obj, ssl.SSLSocket):
        return SSLSocketWrapper(obj)

    elif isinstance(obj, socket.socket):
        return SocketWrapper(obj)

    elif hasattr(obj, 'fileno'):
        return FDWrapper(obj)       # da fare
        # fcntl.fcntl(fd, fcntl.F_SETFL, fcntl.fcntl(fd, fcntl.F_GETFL) | os.O_NONBLOCK)
        # poi usando os.read scatta un BlockingIOError

    else:
        raise TypeError(str(type(obj)))


class SocketWrapper:

    def __init__(self, sock):

        self.socket = sock
        self.read_buffer = b''
        self._is_reading = False
        self._is_writing = False
        self.last_write_pos = None


    def read(self, size, timeout=None):

        if self.read_buffer:
            out_data, self.read_buffer = self.read_buffer[:size], self.read_buffer[size:]
            if len(out_data) == size:
                return out_data
        else:
            out_data = b''

        sock = self.socket

        # reading attempt before asking for data availability. It could be enough
        try:
            block = sock.recv(size - len(out_data))
            if not block:
                return out_data
            out_data += block
        except BlockingIOError:
            pass    # no data, it's fine

        if len(out_data) == size:
            return out_data

        poll = yield get_poll()

        poll.begin_read(sock)

        if timeout is not None:
            timeout = poll.start_timer(timeout)

        try:

            while len(out_data) != size:

                try:
                    block = sock.recv(size - len(out_data))
                    if not block:
                        break
                    out_data += block

                except BlockingIOError:
                    event = yield poll.wait_event()

                    if event.is_read(sock):
                        pass
                    elif event.is_timer(timeout):
                        self.read_buffer = out_data
                        timeout = None
                        raise Timeout('Timeout while reading') from None
                    else:
                        self.read_buffer = out_data
                        raise UnhandledEvent(event) from None

        finally:

            if timeout is not None:
                poll.stop_timer(timeout)

            poll.end_read(sock)

        return out_data


    def readline(self, stop=b'\n', maxsize=10*1024**2, greedy=False, timeout=None):

        if self.read_buffer:

            pos = self.read_buffer.find(stop)
            if pos != -1 or len(self.read_buffer) >= maxsize:
                pos = min(maxsize, pos + len(stop))
                out_data, self.read_buffer = self.read_buffer[:pos], self.read_buffer[pos:]
                return out_data

            else:
                out_data = self.read_buffer
                self.read_buffer = b''

        else:
            out_data = b''

        sock = self.socket

        if not greedy:

            # Read one byte at a time. Optimizable? Maybe.
            try:
                # This is an attempt to read without activating flow control
                while not out_data.endswith(stop) and len(out_data) < maxsize:
                    char = sock.recv(1)
                    if not char:
                        return out_data     # connection terminated
                    out_data += char
                else:
                    return out_data
            except BlockingIOError:
                pass

            poll = yield get_poll()
            poll.begin_read(sock)

            if timeout is not None:
                timeout = poll.start_timer(timeout)

            try:
                while not out_data.endswith(stop) and len(out_data) < maxsize:
                    try:
                        char = sock.recv(1)
                        if not char:
                            break
                        out_data += char

                    except BlockingIOError:
                        event = yield poll.wait_event()

                        if event.is_read(sock):
                            pass
                        elif event.is_timer(timeout):
                            timeout = None
                            self.read_buffer += out_data
                            raise Timeout('Timeout while reading') from None
                        else:
                            self.read_buffer += out_data
                            raise UnhandledEvent(event) from None

            finally:

                if timeout is not None:
                    poll.stop_timer(timeout)

                poll.end_read(sock)

        else:
            # Greedy reading; then search of stop bytes.
            self.read_buffer = out_data

            try:
                # This is an attempt to read without activating flow control
                self.read_buffer += sock.recv(64*1024)
            except BlockingIOError:
                pass    # no data, it's fine

            first_loop = True
            try:
                while stop not in self.read_buffer and len(self.read_buffer) < maxsize:

                    if first_loop:
                        poll = yield get_poll()
                        poll.begin_read(sock)

                        if timeout is not None:
                            timeout = poll.start_timer(timeout)

                        first_loop = False

                    try:
                        block = sock.recv(64*1024)
                        if not block:
                            break
                        self.read_buffer += block

                    except BlockingIOError:
                        event = yield poll.wait_event()

                        if event.is_read(sock):
                            pass
                        elif event.is_timer(timeout):
                            timeout = None
                            raise Timeout('Timeout while reading') from None
                        else:
                            raise UnhandledEvent(event) from None

            finally:
                if not first_loop:
                    if timeout is not None:
                        poll.stop_timer(timeout)

                    poll.end_read(sock)

            pos = self.read_buffer.find(stop)
            if pos != -1 or len(self.read_buffer) >= maxsize:
                pos = min(maxsize, pos + len(stop))
                out_data, self.read_buffer = self.read_buffer[:pos], self.read_buffer[pos:]

            else:
                out_data = self.read_buffer
                self.read_buffer = b''

        return out_data


    def write(self, data, timeout=None):
        # In case of unhandled event or timeout, the attribute last_write_pos contain
        # how much data has been sent.

        self.last_write_pos = None
        sock = self.socket

        poll = yield get_poll()
        poll.begin_write(sock)

        if timeout is not None:
            timeout = poll.start_timer(timeout)

        try:
            pos = 0
            n = 0
            while pos < len(data):

                try:
                    size = sock.send(data[pos:pos + 64*1024])
                    pos += size
                    n += 1
                    if n % 4 == 0:
                        # Limit the number of consecutive writes. A very fast
                        # socket could stall the main loop
                        raise BlockingIOError

                except BlockingIOError:
                    event = yield poll.wait_event()

                    if event.is_write(sock):
                        pass
                    elif event.is_timer(timeout):
                        self.last_write_pos = pos
                        timeout = None
                        raise Timeout('Timeout while writing') from None
                    else:
                        self.last_write_pos = pos
                        raise UnhandledEvent(event)

        finally:
            if timeout is not None:
                poll.stop_timer(timeout)

            poll.end_write(sock)


    def send_file(self, file_obj, stall_timeout=None):

        self.last_write_pos = None

        poll = yield get_poll()
        poll.begin_write(self.socket)

        if stall_timeout is not None:
            st_id = poll.start_timer(stall_timeout)
            st_pos = 0

        try:
            current_pos = 0
            while True:

                block = file_obj.read(64*1024)
                if not block:
                    break

                while block:
                    try:
                        size = self.socket.send(block)
                        current_pos += size
                        block = block[size:]
                    except BlockingIOError:
                        event = yield poll.wait_event()

                        if event.is_timer(st_id):
                            if st_pos == current_pos:
                                # peer has accepted no data since the beginning of the timeout.
                                self.last_write_pos = current_pos
                                stall_timeout = None
                                raise Timeout
                            else:
                                # timeout triggered, but some data has been accepted by peer;
                                # start a new timer.
                                st_id = poll.start_timer(stall_timeout)
                                st_pos = current_pos

                        elif event.is_write(self.socket):
                            pass
                        else:
                            self.last_write_pos = current_pos
                            raise UnhandledEvent(event)

        finally:

            if stall_timeout is not None:
                poll.stop_timer(st_id)

            poll.end_write(self.socket)


    def recv(self, size, timeout=None):
        # read any non-zero amount of data, up to size.

        if self.read_buffer:
            out, self.read_buffer = self.read_buffer[:size], self.read_buffer[size:]
            return out

        else:

            try:
                return self.socket.recv(size)
            except BlockingIOError:
                pass    # no data, it's fine

            poll = yield get_poll()

            poll.begin_read(self.socket)

            if timeout is not None:
                timeout = poll.start_timer(timeout)

            try:
                event = yield poll.wait_event()

                if event.is_timer(timeout):
                        timeout = None
                        raise Timeout

                elif event.is_read(self.socket):
                    return self.socket.recv(size)

                else:
                    raise UnhandledEvent(event)

            finally:
                if timeout is not None:
                    poll.stop_timer(timeout)


    def close(self):

        self.socket.close()
        del self.socket


class SSLSocketWrapper:

    def __init__(self, sslsocket):

        if not isinstance(sslsocket, ssl.SSLSocket):
            raise TypeError('SSLSocket instance required')

        self.read_buffer = b''
        self.socket = sslsocket
        self._is_reading = False
        self._is_writing = False


    def read(self, size, timeout=None):

        if self.read_buffer:
            out_data, self.read_buffer = self.read_buffer[:size], self.read_buffer[size:]
            if len(out_data) == size:
                return out_data
        else:
            out_data = b''

        poll = yield get_poll()

        if timeout is not None:
            timeout = poll.start_timer(timeout)

        try:
            while len(out_data) != size:

                do_wait = False

                try:
                    block = self.socket.read(size - len(out_data))
                    if not block:
                        return out_data
                    out_data += block

                except SSLWantReadError:
                    self._end_write(poll)
                    self._begin_read(poll)
                    do_wait = True

                except SSLWantWriteError:
                    self._end_read(poll)
                    self._begin_write(poll)
                    do_wait = True

                if do_wait:
                    event = yield poll.wait_event()
                    if event.is_read(self.socket) or event.is_write(self.socket):
                        pass
                    elif event.is_timer(timeout):
                        self.read_buffer = out_data
                        timeout = None
                        raise Timeout('Timeout while reading') from None
                    else:
                        self.read_buffer = out_data
                        raise UnhandledEvent(event)

        finally:
            self._end_read(poll)
            self._end_write(poll)

            if timeout is not None:
                poll.stop_timer(timeout)

        return out_data


    def readline(self, stop=b'\n', maxsize=10*1024**2, greedy=False, timeout=None):

        if self.read_buffer:

            pos = self.read_buffer.find(stop)
            if pos != -1 or len(self.read_buffer) >= maxsize:
                pos = min(maxsize, pos + len(stop))
                out_data, self.read_buffer = self.read_buffer[:pos], self.read_buffer[pos:]
                return out_data

            else:
                out_data = self.read_buffer
                self.read_buffer = b''

        else:
            out_data = b''

        poll = yield get_poll()

        if not greedy:

            # Read one byte at a time.
            try:

                if timeout is not None:
                    timeout = poll.start_timer(timeout)

                while not out_data.endswith(stop) and len(out_data) < maxsize:

                    do_wait = False

                    try:
                        char = self.socket.recv(1)
                        if not char:
                            break
                        out_data += char

                    except SSLWantReadError:
                        self._end_write(poll)
                        self._begin_read(poll)
                        do_wait = True

                    except SSLWantWriteError:
                        self._end_read(poll)
                        self._begin_write(poll)
                        do_wait = True

                    if do_wait:
                        event = yield poll.wait_event()
                        if event.is_read(self.socket) or event.is_write(self.socket):
                            pass
                        elif event.is_timer(timeout):
                            self.read_buffer = out_data
                            timeout = None
                            raise Timeout('Timeout while reading line') from None
                        else:
                            self.read_buffer = out_data
                            raise UnhandledEvent(event)

            finally:

                if timeout is not None:
                    poll.stop_timer(timeout)

                self._end_read(poll)
                self._end_write(poll)

        else:

            try:

                if timeout is not None:
                    timeout = poll.start_timer(timeout)

                while stop not in out_data and len(out_data) < maxsize:

                    do_wait = False

                    try:
                        block = self.socket.recv(64*1024)
                        if not block:
                            break
                        out_data += block

                    except SSLWantReadError:
                        self._end_write(poll)
                        self._begin_read(poll)
                        do_wait = True

                    except SSLWantWriteError:
                        self._end_read(poll)
                        self._begin_write(poll)
                        do_wait = True

                    if do_wait:
                        event = yield poll.wait_event()
                        if event.is_read(self.socket) or event.is_write(self.socket):
                            pass
                        elif event.is_timer(timeout):
                            self.read_buffer = out_data
                            timeout = None
                            raise Timeout('Timeout while reading line') from None
                        else:
                            self.read_buffer = out_data
                            raise UnhandledEvent(event)

            finally:

                if timeout is not None:
                    poll.stop_timer(timeout)

                self._end_read(poll)
                self._end_write(poll)

            pos = out_data.find(stop)
            if pos != -1 or len(out_data) >= maxsize:
                pos = min(maxsize, pos + len(stop))
                out_data, self.read_buffer = out_data[:pos], out_data[pos:]

            else:
                out_data = self.read_buffer
                self.read_buffer = b''

        return out_data


    def write(self, data, timeout=None):

        poll = yield get_poll()
        pos = 0

        if timeout is not None:
            timeout = poll.start_timer(timeout)

        try:

            while pos < len(data):

                do_wait = False

                try:
                    size = self.socket.write(data[pos: pos + 64*1024])
                    pos += size

                except SSLWantReadError:
                    self._end_write(poll)
                    self._begin_read(poll)
                    do_wait = True

                except SSLWantWriteError:
                    self._end_read(poll)
                    self._begin_write(poll)
                    do_wait = True

                if do_wait:
                    event = yield poll.wait_event()

                    if event.is_read(self.socket) or event.is_write(self.socket):
                        pass
                    elif event.is_timer(timeout):
                        self.last_write_pos = pos
                        timeout = None
                        raise Timeout('Timeout while writing') from None
                    else:
                        self.last_write_pos = pos
                        raise UnhandledEvent(event)

        finally:
            self._end_read(poll)
            self._end_write(poll)

            if timeout is not None:
                poll.stop_timer(timeout)


    def send_file(self, file_obj, stall_timeout=None):

        poll = yield get_poll()
        self.last_write_pos = None

        if stall_timeout is not None:
            st_id = poll.start_timer(stall_timeout)
            st_pos = 0

        try:
            current_pos = 0
            while True:

                block = file_obj.read(64*1024)
                if not block:
                    break

                pos = 0
                while pos < len(block):

                    do_wait = False

                    try:
                        size = self.socket.write(block[pos:])
                        pos += size

                    except SSLWantReadError:
                        self._end_write(poll)
                        self._begin_read(poll)
                        do_wait = True

                    except SSLWantWriteError:
                        self._end_read(poll)
                        self._begin_write(poll)
                        do_wait = True

                    if do_wait:
                        event = yield poll.wait_event()

                        if event.is_read(self.socket) or event.is_write(self.socket):
                            pass
                        elif event.is_timer(timeout):
                            if st_pos == current_pos:
                                # peer has accepted no data since the beginning of the timeout.
                                self.last_write_pos = current_pos
                                stall_timeout = None
                                raise Timeout from None
                            else:
                                # timeout triggered, but some data has been accepted by peer;
                                # start a new timer.
                                st_id = poll.start(stall_timeout)
                                st_pos = current_pos

                        else:
                            raise UnhandledEvent(event)

        finally:

            if stall_timeout is not None:
                poll.stop_timer(st_id)

            self._end_read(poll)
            self._end_write(poll)


    def _begin_read(self, poll):

        if not self._is_reading:
            poll.begin_read(self.socket)
            self._is_reading = True


    def _end_read(self, poll):

        if self._is_reading:
            poll.end_read(self.socket)
            self._is_reading = False


    def _begin_write(self, poll):

        if not self._is_writing:
            poll.begin_write(self.socket)
            self._is_writing = True


    def _end_write(self, poll):

        if self._is_writing:
            poll.end_write(self.socket)
            self._is_writing = False


    def close(self):

        self.socket.close()
        del self.socket

