#! /usr/bin/env python3
# -*- config: utf-8 -*-

'''
openssl s_client -connect localhost:5000

openssl genrsa -des3 -out mykey.orig.key 2048
openssl rsa -in mykey.orig.key -out mykey.key
openssl req -new -key mykey.key -out mykey.csr
openssl x509 -req -days 365 -in mykey.csr -signkey mykey.key -out mykey.crt
'''

import sys, socket
import collections, contextlib
from functools import partial
from urllib.error import HTTPError, ContentTooShortError

from .stepper import *
from .misc import CaseInsDict


def create_connection(addr, timeout=None):
    # NOTE:
    # the domain resolution made by socket.connect() is blocking.
    # Apparently Twisted calls socket.getaddrinfo in a thread. That's an idea.

    if isinstance(addr, str):
        host, sep, port = addr.partition(':')
        if not sep:
            raise ValueError('malformed address string ' + repr(addr))

        addr = (host, int(port))

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setblocking(False)

    try:
        sock.connect(addr)
    except BlockingIOError:
        pass        # non-blocking connect, it's fine

    poll = yield get_poll()

    if timeout is not None:
        timeout = poll.start_timer(timeout)

    poll.begin_write(sock)
    event = yield poll.wait_event()

    if event.is_write(sock):

        try:
            # try to send 0 bytes, just to check whether the connection succeded
            sock.send(b'')
        except OSError as err:
            poll.end_write(sock)
            raise err from None

    elif event.is_timer(timeout):
        raise Timeout('Timeout while connecting')

    else:
        raise UnhandledEvent(event)

    if timeout is not None:
        poll.stop_timer(timeout)

    poll.end_write(sock)

    return sock


def run_in_thread(func, *args, **kw):

    poll = yield get_poll()

    thread_id = poll.run_thread_for_result(partial(func, *args, **kw))
    event = yield poll.wait_event()

    if not event.is_thread(thread_id):
        raise UnhandledEvent(event)
    elif isinstance(event.result, Exception):
        raise event.result
    else:
        return event.result


def run_and_wait_task(task):

    poll = yield get_poll()

    poll.run_task_for_result(task)
    event = yield poll.wait_event()

    if not event.is_task(task):
        raise UnhandledEvent(event)
    elif event.exception:
        raise event.exception from None
    else:
        return event.result


def wait_timer(timeout):

    poll = yield get_poll()

    timer_id = poll.start_timer(timeout)
    event = yield poll.wait_event()

    if event.is_timer(timer_id):
        return
    else:
        poll.stop_timer(timer_id)
        raise UnhandledEvent(event) from None


def wait_message(msgbox):
    '''
    Convenience function that waits a message coming from a specific msgbox.
    Opening and closing the messagebox IS NOT done here.
    This functions raises an UnhandledEvent exception for any unexpected event.
    '''

    poll = yield get_poll()
    event = yield poll.wait_event()

    if event.is_msg(msgbox):
        return event.msg
    else:
        raise UnhandledEvent(event) from None


def wait_broadcast(channel):
    '''
    Convenience function that waits a message coming from a specific broadcast channel.
    Listening and un-listening the channel IS NOT done here.
    This functions raises an UnhandledEvent exception for any unexpected event.
    '''

    poll = yield get_poll()
    event = yield poll.wait_event()

    if event.is_broadcast(channel):
        return event.msg
    else:
        raise UnhandledEvent(event) from None


def run_tasks_parallel(*tasks):

    poll = yield get_poll()

    results = {}
    for t in tasks:
        poll.run_task_for_result(t)

    wait_tasks = set(tasks)
    while wait_tasks:
        event = yield poll.wait_event()
        if event.type == EV_TASK_RESULT and event.task in wait_tasks:
            results[event.task] = event.result
            wait_tasks.remove(event.task)
        else:
            # there are, likely, tasks still running. Their result events
            # will arrive. Need some better thoughts about how to deal with this case.
            raise UnhandledEvent(event) from None

    return [results[t] for t in tasks]


def socket_relay(src_socket, dst_socket, buffer_size=1024**2, sent_callback=None):

    buffer = collections.deque()
    bsize = 0
    reading = writing = False
    src_closed = False

    poll = yield get_poll()

    try:
        while not src_closed or buffer:

            if bsize < buffer_size and not reading and not src_closed:
                poll.begin_read(src_socket)
                reading = True
            elif bsize >= buffer_size and reading:
                poll.end_read(src_socket)
                reading = False

            if buffer and not writing:
                poll.begin_write(dst_socket)
                writing = True
            elif not buffer and writing:
                poll.end_write(dst_socket)
                writing = False

            event = yield poll.wait_event()

            if event.is_read(src_socket):
                recv_running = True
                block = src_socket.recv(64*1024)
                recv_running = False
                if not block:
                    src_closed = True
                    if reading:
                        poll.end_read(src_socket)
                        reading = False
                else:
                    if not buffer:
                        # buffer empty, attempt to relay data immediately
                        try:
                            send_running = True
                            size = dst_socket.send(block)
                            send_running = False
                            if sent_callback:
                                sent_callback(size)
                            block = block[size:]
                        except BlockingIOError:
                            pass

                    if block:
                        buffer.append(block)
                        bsize += len(block)

            elif event.is_write(dst_socket):

                block = buffer.popleft()
                send_running = True
                size = dst_socket.send(block)
                send_running = False
                if sent_callback:
                    sent_callback(size)

                block = block[size:]
                if block:
                    buffer.appendleft(block)
                bsize -= size

    except OSError as err:

        with contextlib.suppress(OSError):
            if recv_running:
                dst_socket.shutdown(socket.SHUT_WR)
            elif send_running:
                src_socket.shutdown(socket.SHUT_RD)

    finally:

        if src_closed:
            with contextlib.suppress(OSError):
                dst_socket.shutdown(socket.SHUT_WR)

        if reading:
            poll.end_read(src_socket)

        if writing:
            poll.end_write(dst_socket)


# ------------------------------------------------------------------------------------

class HttpRequest:

    def __init__(self):
        self.sockw = None


    def get(self, url, data=None, timeout=None):
        # TO DO: handle 301 MOVED

        protocol, sep, url = url.partition('://')
        if not sep:
            raise ValueError('missing protocol sign ://')

        if protocol != 'http':
            raise ValueError('unsupported protocol ' + protocol)

        domain, _, path = url.partition('/')
        domain, _, port = domain.partition(':')
        port = int(port) if port else 80

        sock = yield from create_connection((domain, port), timeout=timeout)
        method = 'GET'
        headers = {'Connection': 'Close'}
        protocol = 'HTTP/1.0'

        req_data = ['{} {} {}'.format(method, path or '/', protocol)]
        print('+++', req_data)

        for k, v in headers.items():
            req_data.append('{}: {}'.format(k, v))

        req_data = ('\r\n'.join(req_data) + '\r\n\r\n').encode('utf-8')

        self.sockw = sockw = SocketWrapper(sock)
        yield from sockw.write(req_data, timeout=timeout)

        header_lines = []
        while True:
            line = yield from sockw.readline(greedy=True, timeout=timeout)
            line = line.rstrip()
            if not line:
                break
            header_lines.append(line.decode('utf-8'))

        status_line = header_lines.pop(0)
        protocol, _, status_line = status_line.partition(' ')
        code, _, msg = status_line.partition(' ')

        headers = CaseInsDict()
        for line in header_lines:
            k, _, v = line.partition(':')
            headers[k] = v.strip()

        self.read_to_be = int(headers.get('content-length', 1024**5))

        if code != '200':
            raise HTTPError(url, int(code), msg, headers, None)

        return {
            'status': int(code),
            'headers': headers
        }


    def read(self, size=None):
        # response content

        if self.sockw is None:
            return b''

        if size is None:
            # read everything!
            out = []
            while self.read_to_be > 0:
                block = yield from self.sockw.read(min(64*1024, self.read_to_be))
                if not block:
                    break

                out.append(block)
                self.read_to_be -= len(block)

            self.sockw.socket.close()
            self.sockw = None
            return b''.join(out)

        else:

            block = yield from self.sockw.read(min(size, self.read_to_be))
            if not block:
                self.sockw.socket.close()
                self.sockw = None

                if self.read_to_be:
                    raise ContentTooShortError

            self.read_to_be -= len(block)
            return block


# ------------------------------------------------------------------------------------

def socket_send(sock, data):

    poll = yield get_poll()
    poll.begin_write(sock)

    pos = 0
    while pos < len(data):

        event = yield poll.wait_event()

        if event['type'] == EV_WRITE and event['obj'] is sock:
            sent = sock.write(data[pos:])
            pos += sent

        else:
            raise UnhandledEvent(event)


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

