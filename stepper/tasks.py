#! /usr/bin/env python3
# -*- config: utf-8 -*-

import sys, os, socket, ssl
import mimetypes, traceback, shlex, types
from ssl import SSLWantReadError, SSLWantWriteError
import cgi, re, tempfile
from http import client as httpclient
from http.cookies import SimpleCookie
from urllib.parse import unquote, parse_qs

from .stepper import *
from .wrappers import make_wrapper, SocketWrapper
from .misc import create_listen_socket, CaseInsDict

class CloseConnection(Exception):
    pass

def socket_listener(listen_on, task_func, backlog=100, enable_keepalive=False):

    if isinstance(listen_on, socket.socket):
        lsock = listen_on
    else:
        lsock = create_listen_socket(listen_on, backlog=backlog)

    lsock.setblocking(False)

    poll = yield get_poll()
    poll.begin_read(lsock)

    while True:

        yield poll.wait_event()
        sock, remote_addr = lsock.accept()
        sock.setblocking(False)
        if enable_keepalive:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

        poll.run_task(task_func(sock, remote_addr))

        sock = remote_addr = None


def ssl_socket_listener(certfile, keyfile, listen_on, task_func, backlog=100, enable_keepalive=False):

    if isinstance(listen_on, socket.socket):
        lsock = listen_on
    else:
        lsock = create_listen_socket(listen_on, backlog=backlog)

    lsock.setblocking(False)

    ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    ssl_context.load_cert_chain(certfile=certfile, keyfile=keyfile)

    poll = yield get_poll()
    poll.begin_read(lsock)

    while True:

        yield poll.wait_event()
        sock, remote_addr = lsock.accept()
        sock.setblocking(False)
        if enable_keepalive:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

        sslsock = ssl_context.wrap_socket(sock, server_side=True, do_handshake_on_connect=False)
        poll.run_task(task_func(sslsock, remote_addr))

        sock = sslsock = remote_addr = None


class HttpServer:

    keep_alive_timeout = 5      # time available to re-use the kept alive connection
    client_timeout = 20         # time to send and receive headers
    send_content_timeout = 180  # time to completely send the response payload. I'm not sure about this
    max_content_size = 10*1024**2

    printlog = lambda self, *args: print(*args, file=sys.stderr)

    sockw_map = {}

    def task(self, sock, remote_addr):

        self.sockw = make_wrapper(sock)
        req_line_timeout = self.client_timeout   # for the first request use client_timeout,
                                                 # then keep_alive_timeout for all future requests
        self.response_cookie = None
        self.protocol = None

        while True:

            try:
                method, uri, self.protocol = \
                    yield from self.read_request_line(timeout=req_line_timeout)
                req_line_timeout = self.keep_alive_timeout

            except ConnectionResetError:
                return                 # client properly disconnected

            except ssl.SSLError as err:
                self.printlog(err)
                return

            except IOError as err:
                # risposte di errore
                self.printlog(err)
                yield from self.send_response(self.error_response(*err.args))
                return

            try:
                self.req_headers = req_headers = \
                            yield from self.read_headers(timeout=self.client_timeout)
            except Timeout:
                self.printlog('client {} timed out while receiving headers', self.remote_addr)
                return
            except IOError as err:
                yield from self.send_response(self.error_response(*err.args))
                return

            content_length = int(req_headers.get('Content-Length', 0))
            if content_length:
                content_type, params = cgi.parse_header(req_headers['Content-Type'])
                if content_type == 'application/x-www-form-urlencoded':
                    content = yield from self.sockw.read(content_length)
                    form_vars = parse_qs(content.decode('utf-8'))

                elif content_type == 'multipart/form-data':
                    form_vars = yield from self.parse_multipart(req_headers, params['boundary'], self.sockw)

                else:
                    pass        # leave other content types to the application
            else:
                form_vars = None

            keep_connection, init_headers = self.parse_keep_alive(self.protocol, req_headers)
            resp_headers = CaseInsDict()
            resp_headers.update(init_headers)

            try:
                call_func = getattr(self, 'do_' + method)
            except AttributeError as err:
                yield from self.send_response(self.error_response(
                    httpclient.METHOD_NOT_ALLOWED, 'method {} not allowed\n'.format(method)
                ))
                return

            path, qs = self.split_uri(uri)
            input_vars = parse_qs(qs, keep_blank_values=True)
            if form_vars:
                for k, v in form_vars.items():
                    if k in input_vars:
                        input_vars[k].extend(v)
                    else:
                        input_vars[k] = v

            for k, v in list(input_vars.items()):       # remove list for single-value keys
                if len(v) == 1:
                    input_vars[k] = v[0]

            env = {
                'REQUEST_METHOD': method,
                'REQUEST_URI': uri,
                'REQUEST_PROTOCOL': self.protocol,
                'REMOTE_ADDR': remote_addr[0],
                'REMOTE_PORT': str(remote_addr[1]),
                'SCRIPT_NAME': unquote(path),
                'QUERY_STRING': qs,
                'request_headers': req_headers,
                'input': input_vars
            }

            try:

                rs = call_func(env)

                if isinstance(rs, types.GeneratorType):
                    status_code, headers, content = yield from rs
                else:
                    status_code, headers, content = rs

            except CloseConnection:
                self.sockw.socket.close()
                return

            except Exception as err:
                self.printlog('REQUEST HANDLER ERROR: ' + traceback.format_exc())
                yield from self.send_response(
                    env, self.error_response(httpclient.BAD_GATEWAY, 'application error')
                )
                return

            resp_headers.update(headers)

            yield from self.send_response(env, (status_code, resp_headers, content))

            keep_connection = keep_connection and (resp_headers.get('connection', '').lower() == 'keep-alive')

            if not keep_connection:
                break

            del headers, req_headers, self.req_headers, resp_headers, content, self.response_cookie


    def send_response(self, env, response_data):

        status_code, headers, content = response_data

        if 'content-length' not in headers and isinstance(content, bytes):
            headers['Content-Length'] = len(content)

        if self.response_cookie and 'cookie' not in headers:
            headers['Set-Cookie'] = str(self.response_cookie).partition(':')[2]

        try:

            yield from self.sockw.write(
                self.build_response_header(status_code, headers, self.protocol or 'HTTP/1.0'),
                timeout=self.client_timeout
            )

            if content:
                if isinstance(content, bytes):
                    yield from self.sockw.write(content, timeout=self.send_content_timeout)
                elif hasattr(content, 'read'):
                    # file-like object
                    yield from self.sockw.send_file(content, stall_timeout=10)
                else:
                    raise AssertionError(type(content))

        except Timeout:
            self.printlog('client {} timed out while sending content', self.remote_addr)
            return


    def read_request_line(self, timeout=None):

        try:
            request_line = yield from self.sockw.readline(maxsize=1024, timeout=timeout, greedy=True)
            if not request_line:
                raise ConnectionResetError       # chiusura pulita
        except Timeout:
            self.printlog('closing kept alive connection due to silent client')
            raise ConnectionResetError

        # request line
        try:
            method, uri, protocol = request_line.strip().split(b' ')
            method = method.decode('ascii', errors='replace')
            uri = uri.decode('utf-8', errors='replace')
            protocol = protocol.decode('ascii', errors='replace')

            if protocol not in ('HTTP/1.1', 'HTTP/1.0', 'HTTP/0.9'):
                raise IOError(httpclient.BAD_REQUEST, 'unsupported protocol {}\n'.format(protocol))

        except ValueError as err:
            raise IOError(httpclient.BAD_REQUEST, 'malformed request line\n')

        return method, uri, protocol


    def read_headers(self, timeout=None):

        header_lines = []

        # single timeout for the whole header block
        if timeout is not None:
            poll = yield get_poll()
            timeout = poll.start_timer(timeout)

        try:
            while True:
                line = (yield from self.sockw.readline(maxsize=1024)).strip()
                if not line:
                    break
                header_lines.append(line)
        except UnhandledEvent as err:
            assert err['event'] == EV_TIMER and err['timer_id'] == timer_id
            raise Timeout from None

        if timeout is not None:
            poll.stop_timer(timeout)

        try:
            headers = self.parse_header_lines([l.decode('utf-8') for l in header_lines])
        except Exception as err:
            self.server.printlog('error while parsing request headers: ' + str(err))
            raise IOError(httpclient.BAD_REQUEST, 'malformed header\n')

        return headers


    def parse_header_lines(self, lines):

        out = CaseInsDict()
        for l in lines:
            k, sep, v = l.partition(':')
            if not sep:
                raise IOError(httpclient.BAD_REQUEST, 'bad header\n')

            # header ripetuti vanno in lista
            if k in out:
                if not isinstance(out[k], list):
                    out[k] = [out[k]]
                out[k].append(v.strip())
            else:
                out[k] = v.strip()

        return out


    @staticmethod
    def build_response_header(code, headers, protocol):

        headers = '\r\n'.join('{}: {}'.format(*kv) for kv in headers.items())
        return '{} {} {}\r\n{}\r\n\r\n'.format(protocol, code, httpclient.responses[code], headers).encode('utf-8')


    @staticmethod
    def parse_keep_alive(protocol, req_headers):

        # there's a bug in the keep-alive logic here. Investigation needed.
        headers = {}
        headers['Connection'] = 'close'
        return False, headers

        if protocol == 'HTTP/1.1':
            if 'connection' in req_headers:
                if req_headers.get('connection', '').lower() == 'keep-alive':
                    headers['Connection'] = 'Keep-Alive'
                    keep_connection = True
                else:
                    headers['Connection'] = 'Close'
                    keep_connection = False
            else:
                keep_connection = True      # in HTTP/1.1 è implicito se non specificato

        elif protocol == 'HTTP/1.0':
            if req_headers.get('connection', '').lower() == 'keep-alive':
                headers['Connection'] = 'Keep-Alive'
                keep_connection = True
            else:
                headers['Connection'] = 'Close'
                keep_connection = False

        elif protocol == 'HTTP/0.9':
            keep_connection = False

        else:
            raise AssertionError

        return keep_connection, headers


    def get_cookies(self):

        cookie_headers = self.req_headers.get('cookie')
        if not isinstance(cookie_headers, list):
            cookie_headers = [cookie_headers]
        return [SimpleCookie(c) for c in cookie_headers]


    def set_cookies(self, *cookies):

        assert len(cookies) == 1, 'TO DO: multiple cookie handling'
        self.response_cookie = cookies[0]


    @staticmethod
    def split_uri(url):
        # separa percorso da query string

        url, _, qs = url.partition('?')
        return url, qs


    @staticmethod
    def redirect_response(location):
        return (
            httpclient.MOVED_PERMANENTLY,
            {'Location': location, 'Connection': 'Close'},
            None
        )


    @staticmethod
    def static_file_response(local_filename, content_type=None):

        if os.path.isfile(local_filename):

            if content_type is None:
                content_type = mimetypes.guess_type(local_filename)[0]
                if not content_type:
                    content_type = 'application/octet-stream'

            return (
                httpclient.OK,
                {
                    'Content-Type': content_type,
                    'Content-Length': os.stat(local_filename).st_size,
                },
                open(local_filename, 'rb')
            )

        else:
            return (httpclient.NOT_FOUND, {}, b'not found.\n')


    @staticmethod
    def error_response(status_code, text):

        return (
            status_code,
            {'Content-Type': 'text/plain;charset=utf-8', 'Connection': 'Close'},
            text.encode('utf-8')
        )


    @staticmethod
    def plain_text_response(text):

        content = text.encode('utf-8')
        return (
            httpclient.OK,
            {'Content-Type': 'text/plain;charset=utf-8', 'Content-Length': len(content)},
            content
        )


    @staticmethod
    def html_response(html):

        content = html.encode('utf-8')
        return (
            httpclient.OK,
            {'Content-Type': 'text/html;charset=utf-8', 'Content-Length': len(content)},
            content
        )


    @staticmethod
    def parse_multipart(headers, boundary, sockw):

        def valid_boundary(s, _vb_pattern="^[ -~]{0,200}[!-~]$"):
            return re.match(_vb_pattern, s)

        if not valid_boundary(boundary):
            raise ValueError('bad multipart data - invalid boundary')

        bytes_to_read = int(headers['Content-Length'])
        line_buffer = []

        new_part_line = '--' + boundary
        last_part_line = new_part_line + '--'
        new_part_line = new_part_line.encode('ascii') + b'\r\n'       # or iso-8859-1 ?
        last_part_line = last_part_line.encode('ascii') + b'\r\n'
        all_parts = []
        part = part_data = None

        status = 'new-part'
        while bytes_to_read > 0:

            if line_buffer:
                line = line_buffer.pop(0)
            else:
                line = yield from sockw.readline(greedy=True, maxsize=bytes_to_read, stop=b'\r\n')
                bytes_to_read -= len(line)

            if status == 'new-part':
                if line == new_part_line:
                    part = {}
                    status = 'read-header'
                elif line == last_part_line:
                    break
                else:
                    raise ValueError('bad multipart data - missing start line')

            elif status == 'read-header':
                assert part is not None

                line = line.decode('utf-8').strip()

                if not line:
                    part_data = tempfile.SpooledTemporaryFile(max_size=1*1024**2)
                    status = 'read-data'
                else:
                    header_kv, params = cgi.parse_header(line)
                    try:
                        k, v = header_kv.split(':')
                    except ValueError:
                        raise ValueError('bad multipart data - error parsing header')
                    part[k.strip().lower()] = (v.strip(), params)

            elif status == 'read-data':

                if line == last_part_line or line == new_part_line:

                    if part_data.tell() == 0:
                        raise ValueError('bad multipart data - empty part content')

                    end_pos = part_data.tell() - 2
                    part_data.seek(end_pos)
                    if part_data.read(2) != b'\r\n':
                        raise ValueError('bad multipart data - incomplete content')

                    part_data.seek(end_pos)
                    part_data.truncate()
                    # store the part payload under the None key - no collision with other keys
                    part[None] = part_data

                    all_parts.append(part)
                    part = part_data = None

                    line_buffer.append(line)
                    status = 'new-part'

                else:
                    part_data.write(line)

        assert bytes_to_read == 0, bytes_to_read

        output = {}
        for part in all_parts:

            data_size = part[None].tell()   # file cursor was left at the end
            part[None].seek(0)

            content_disp = part.get('content-disposition')
            if not content_disp:
                raise ValueError('missing Content-Disposition header')

            if content_disp[0] == 'form-data':
                cd_params = content_disp[1]
                if 'filename' in cd_params or data_size > 128*1024:
                    # if the payload is suspiciously big, treat it as a file upload
                    v = {
                        'filename': cd_params.get('filename'),
                        'file': part[None],
                        'size': data_size
                    }
                else:
                    part_data = part[None].read()
                    content_type, params = part.get('content-type', ('text/plain', {}))
                    if content_type == 'text/plain':
                        v = part_data.decode(params.get('charset', 'utf-8'), errors='replace')
                    else:
                        # don't know whether other content types happen
                        v = part_data

                name = cd_params.get('name', '?')
                if name in output:
                    output[name].append(v)
                else:
                    output[name] = [v]

            else:
                raise ValueError('unknown Content-Disposition {}'.format(content_disp[0]))

        return output


class TelnetCmdServer:
    # http://www.termsys.demon.co.uk/vtansi.htm
    # https://tools.ietf.org/html/rfc854

    IAC_DONT_ECHO = '\xFF\xFE\x01'

    prompt = '> '
    network_timeout = 120
    prompt_timeout = None
    welcome_message = None

    def task(self, sock, remote_addr):

        prompt = self.prompt.encode('utf-8')

        self._stop = False
        sockw = SocketWrapper(sock)

        if self.welcome_message:
            yield from sockw.write(self.welcome_message.encode('utf-8') + b'\r\n')

        try:

            while not self._stop:

                yield from sockw.write(prompt, timeout=self.network_timeout)
                line = yield from sockw.readline(maxsize=1024, timeout=self.prompt_timeout)
                if not line:
                    break

                try:
                    cmd, *params = shlex.split(line.strip().decode('utf-8', errors='replace'))
                except ValueError as err:
                    response = '** ' + str(err)
                else:
                    try:
                        if hasattr(self, 'cmd_' + cmd):
                            response = getattr(self, 'cmd_' + cmd)(*params)
                        else:
                            response = '** no command {}'.format(cmd)
                    except TypeError as err:
                        response = '** ' + str(err)

                if isinstance(response, types.GeneratorType):
                    # it's a task, so hand over control to it
                    response = yield from response

                if isinstance(response, str):
                    response = response.encode('utf-8')

                if response is not None:
                    yield from sockw.write(response + b'\r\n', timeout=self.network_timeout)

        except OSError as err:
            pass

        finally:
            sockw.close()
            del sockw


    def cmd_quit(self):
        self._stop = True



if __name__ == '__main__':
    pass










