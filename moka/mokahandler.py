#! /usr/bin/env python3
# -*- coding: utf-8 -*-

# mokahandler.py
# http://www.io.com/~maus/HttpKeepAlive.html
# http://www.w3.org/Protocols/rfc2616/rfc2616-sec5.html
# （笑）

__version__ = (0, 14, 4)
__version_string__ = '.'.join(str(x) for x in __version__)
server_string = 'Moka/' + __version_string__

'''
0.14    :   * supporto alla chiave 'cookies' nel dizionario response.
            * aggiunto keep_blank_values=True a parse_qs.
0.13    :   * spostata la riga di status nell'header Status
0.11    :   * aggiunto GATEWAY_PROTOCOL = {fastcgi, scgi, http} ad env
0.8     :   * revisione della logica del protocollo fastcgi
0.7     :   * parametrizzato il periodo di chiamata di gc.collect() (gc_collect);
              il default è a zero, che disabilita.
            * aggiunto fastcgi (parzialmente ma suffieciente per nginx).
            * cambiato il parametro http in protocol, che prende in valore http,
              scgi, o fastcgi
0.6     :   * messa una chiamata a gc.collect() ogni 100 esecuzioni
0.5.3   :   * pulizia di codice non utilizzato.
0.5.0   :   * lascia passare verso l'applicazione i content-type non gestiti.
            * mette in env['raw_content'] il content della richiesta.

'''

import sys, os, time, optparse, configparser, socket, signal, errno, io
import re, gc, traceback, platform
from functools import partial

from struct import pack, unpack, pack_into
from urllib.parse import unquote
import cgi, urllib, http
from urllib.parse import parse_qs
import http.client as httpclient
from select import select
import fcntl

# ------------------------------------------------------------------

from ctypes import cdll, c_int, c_char_p, create_string_buffer, string_at
libmoka = os.path.join(os.path.dirname(__file__), '.', 'libmoka-%s.so' % platform.machine())
libmoka = cdll.LoadLibrary(libmoka)
libmoka.get_errstr.restype = c_char_p
libmoka.fastcgi_parse_name_value_buffer.argtypes = [c_char_p, c_int, c_char_p, c_int]
libmoka.fastcgi_stream_into_buffer.argtypes = [c_int, c_int, c_int, c_char_p, c_int]
parse_buffer = create_string_buffer(10*1024)
#~ stream_buffer = create_string_buffer(1024**2)
#~ build_buffer = create_string_buffer(64*1024 + 1024)

# ------------------------------------------------------------------

# costanti
CONTENT_LENGTH_LIMIT = 10*1024**2    # 100M. Occhio che gli upload vengono tenuti tutti in memoria

# variabili globali
config = None


def printlog(*args):

    args = ['mokahandler(%d):' % os.getpid()] + list(args)
    print(*args, file=sys.stderr)
    sys.stderr.flush()


def read_config_file(filename, bool_args=(), int_args=()):

    cp = configparser.ConfigParser(strict=False)
    try:
        cp.readfp(open(filename))
        config_common = dict(cp.items('common'))
        config = config_common.copy()
        config.update(dict(cp.items('moka-handler')))
    except (IOError, OSError, configparser.Error) as err:
        printlog('error reading config file %s: %s' % (filename, err))
        return {}

    for arg in bool_args:
        if arg in config:
            config[arg] = True if config[arg].upper() in ('1', 'Y', 'YES',
                                        'TRUE') else False
    for arg in int_args:
        if arg in config:
            config[arg] = int(config[arg])

    for arg in config:
        if arg not in int_args and args not in bool_args:
            config[arg] = config[arg].format(**config_common)

    return config


class HTTPError(Exception):

    def __init__(self, status_code, message):

        self.status_code = status_code
        self.message = message


    def __str__(self):
        return 'HTTPError[%s, %r]' % (self.status_code, self.message)


    def response(self):
        '''
        ritorna la risposta http con la pagina di errore (header + html),
        in tupla nella forma status, response_headers, output
        '''

        page = '<html><body><h2>%(status_code)s %(message)s</h2></body></html>' % \
                                self.__dict__

        return (
            '%s %s' % self.status_code, httpclient.responses[self.status_code],
            {'Content-Type': 'text/html'},
            page.encode('ascii')
        )


def recv_fd(src_fd):

    rc = libmoka.recv_fd(src_fd)
    if rc < 0:
        raise IOError(libmoka.get_errstr())

    return rc

## FASTCGI ################################################################

FCGI_BEGIN_REQUEST = 1
FCGI_ABORT_REQUEST = 2
FCGI_END_REQUEST = 3
FCGI_PARAMS = 4
FCGI_STDIN = 5
FCGI_STDOUT = 6
FCGI_STDERR = 7
FCGI_DATA = 8
FCGI_GET_VALUES = 9
FCGI_GET_VALUES_RESULT = 10

FCGI_FLAG_KEEP_CONN = 1

FCGI_ROLE_RESPONDER = 1
FCGI_ROLE_AUTHORIZER = 2
FCGI_ROLE_FILTER = 3

# Values for protocolStatus component of FCGI_EndRequestBody
FCGI_REQUEST_COMPLETE = 0
FCGI_CANT_MPX_CONN = 1
FCGI_OVERLOADED = 2
FCGI_UNKNOWN_ROLE = 3

FCGI_END_REQUEST_BLOCK = pack('>LBBBB', 0, FCGI_REQUEST_COMPLETE, 0, 0, 0)
FCGI_PADDINGS = [b'*' * n for n in range(8)]


def fastcgi_read_record(rfile):

    header = rfile.read(8)
    version, type, req_id, content_len, padding_len, reserved = unpack('>BBHHBB', header)

    if req_id == 0:
        raise AssertionError('Management record non gestito')

    content = rfile.read(content_len)
    if padding_len:
        rfile.read(padding_len)

    return type, req_id, content


def fastcgi_write_record(wfile, type, req_id, content):

    content_len = len(content)
    padding_len = content_len % 8
    if padding_len:
        padding_pen = 8 - padding_len

    wfile.write(pack('>BBHHBB', 1, type, req_id, content_len, padding_len, 0))
    wfile.write(content)
    wfile.write(FCGI_PADDINGS[padding_len])


def PY_fastcgi_parse_name_value(data):

    out = {}

    while data:

        # name size
        b0 = data[0]
        if b0 & 0x80:
            nsize = unpack('!L', data[:4])[0] & 0x7ffffff
            data = data[4:]
        else:
            nsize = b0
            data = data[1:]

        # value size
        b0 = data[0]
        if b0 & 0x80:
            vsize = unpack('!L', data[:4])[0] & 0x7ffffff
            data = data[4:]
        else:
            vsize = b0
            data = data[1:]

        vsize += nsize
        out[data[:nsize].decode('utf-8')] = data[nsize:vsize].decode('utf-8')
        data = data[vsize:]

    return out


def C_fastcgi_parse_name_value(data):
    # versione veloce

    global parse_buffer

    if not libmoka.fastcgi_parse_name_value_buffer(
                    data, len(data), parse_buffer, len(parse_buffer)):

        items = string_at(parse_buffer).decode('utf-8').split('\n')
        return dict(zip(items[0::2], items[1::2]))

    else:
        raise RuntimeError('%s %s' % (libmoka.get_errno(), libmoka.get_errstr()))


fastcgi_parse_name_value = PY_fastcgi_parse_name_value


class FastcgiWFileWrapper:
    # spezza le operazioni di scrittura nei record fastcgi

    def __init__(self, req_id, wfile):

        self.req_id = req_id
        self.wfile = wfile
        self.buffer = []
        self.bsize = 0


    def write(self, data):

        self.buffer.append(data)
        self.bsize += len(data)

        if self.bsize > 65535:
            buffer = b''.join(self.buffer)
            while len(buffer) > 65535:
                block, buffer = buffer[:65535], buffer[65535:]
                fastcgi_write_record(self.wfile, FCGI_STDOUT, self.req_id, block)

            self.buffer = [buffer]
            self.bsize = len(buffer)


    def flush(self):

        buffer = b''.join(self.buffer)
        while buffer:
            block, buffer = buffer[:65535], buffer[65535:]
            fastcgi_write_record(self.wfile, FCGI_STDOUT, self.req_id, block)

        self.wfile.flush()
        self.buffer = []
        self.bsize = 0


    def close(self):

        self.flush()
        fastcgi_write_record(self.wfile, FCGI_STDOUT, self.req_id, b'')
        fastcgi_write_record(self.wfile, FCGI_END_REQUEST, self.req_id, FCGI_END_REQUEST_BLOCK)
        self.wfile.flush()
        self.wfile.close()


class FastcgiRFileWrapper:

    def __init__(self, req_id, rfile):

        self.req_id = req_id
        self.rfile = rfile
        self.reader = self._reader(rfile)
        next(self.reader)       # avvio del generatore


    def read(self, size):
        return self.reader.send(size)


    def readline(self, max_size):

        out = []
        tot = 0
        while tot < max_size:
            block = self.read(min(16*1024, max_size - tot))
            if not block:
                break

            line, sep, rest = block.partition(b'\n')
            if not sep:
                out.append(line)
                tot += len(line)
            else:
                out.append(line)
                out.append(sep)
                # rimetto in testa al buffer quello che non ho usato
                self.buffer = rest + self.buffer
                break

        return b''.join(out)


    def _reader(self, rfile):

        self.buffer = b''
        stop_reading = False
        size = yield

        while True:

            while (len(self.buffer) < size) and not stop_reading:

                type, req_id, block = fastcgi_read_record(self.rfile)
                #~ assert type == FCGI_STDIN and req_id == self.req_id

                if not block:
                    stop_reading = True
                else:
                    self.buffer += block

            send, self.buffer = self.buffer[:size], self.buffer[size:]
            size = yield send


    def close(self):

        self.rfile.flush()
        self.rfile.close()
        del self.reader


def serve_fastcgi_request(sock, app_function, finish_func=None):

    rfile = sock.makefile('rb')
    wfile = sock.makefile('wb')
    sock.close()

    type, req_id, content = fastcgi_read_record(rfile)

    #~ assert type == FCGI_BEGIN_REQUEST

    # lettura del record BEGIN_REQUEST
    role, flags, reserved = unpack('>HB5s', content)

    #~ assert not (flags & FCGI_FLAG_KEEP_CONN)   # non gestito il caso di tenere su la connessione

    # REQUEST PARAMS. viene ripetuto fino a quando ne arriva uno vuoto
    params_stream = []
    while True:

        type, rid, content = fastcgi_read_record(rfile)
        #~ assert type == FCGI_PARAMS and rid == req_id
        if not content:
            break

        params_stream.append(content)

    params_stream = b''.join(params_stream)
    env = fastcgi_parse_name_value(params_stream)
    env['GATEWAY_PROTOCOL'] = 'fastcgi'

    # occhio: run_application chiude rfile e wfile
    # anche rfile sarebbe da chiudere in un wrapper. per ora mi accontento di BytesIO
    run_application(
        app_function,
        FastcgiRFileWrapper(req_id, rfile), FastcgiWFileWrapper(req_id, wfile),
        env
    )

    if finish_func:
        finish_func(env)


## SCGI ###################################################################

def scgi_netstring_read_size(fin):

    size = b''
    while True:
        c = fin.read(1)
        if c == b':':
            break
        elif not c:
            raise IOError('short netstring received. ' + repr(size))
        size += c

    return int(size)


def scgi_netstring_read_string(fin):

    size = scgi_netstring_read_size(fin)
    data = []
    while size > 0:
        block = fin.read(size)
        if not block:
            raise IOError('short netstring received')
        data.append(block)
        size -= len(block)

    if fin.read(1) != b',':
        raise IOError('missing netstring terminator')

    return b''.join(data)


def scgi_read_scgi_env(f):

    items = [h.decode('utf-8') for h in scgi_netstring_read_string(f).split(b'\x00')]
    items.pop()   ## togli l'ultimo

    return dict(zip(items[0::2], items[1::2]))


def serve_scgi_request(sock, app_function, finish_func=None):

    rfile = sock.makefile('rb')
    wfile = sock.makefile('wb')
    sock.close()

    env = scgi_read_scgi_env(rfile)
    env['GATEWAY_PROTOCOL'] = 'scgi'

    # occhio: run_application chiude rfile e wfile
    run_application(app_function, rfile, wfile, env)

    #~ wfile.write(b'HTTP/1.1\r\nContent-Type:text/html\r\nContent-Length:59\r\n\r\n<html><body><div style="color: #080">{}</div></body></html>')
    #~ wfile.flush()

    if finish_func:
        finish_func(env)

## HTTP ##################################################################

class TimeoutSocketIO(socket.SocketIO):
    # vedi SocketIO in socket.py

    def __init__(self, sock, mode, timeout=None):

        super().__init__(sock, mode)
        self._timeout = timeout


    def set_timeout(self, timeout):
        self._timeout = timeout


    def _wait_read(self):

        r, w, e = select([self._sock], [], [], self._timeout)
        if not r:
            raise IOError('timeout while reading')


    def _wait_write(self):

        r, w, e = select([], [self._sock], [], self._timeout)
        if not w:
            raise IOError('timeout while writing')


    def readinto(self, b):

        self._checkClosed()
        self._checkReadable()
        self._wait_read()
        return self._sock.recv_into(b)

    def write(self, b):

        self._checkClosed()
        self._checkWritable()
        self._wait_write()
        return self._sock.send(b)


def read_http_request_line(f, log):
    # legge la prima linea inviata dal client. se non è accettabile
    # ritorna None.

    r = f.readline(4097)
    if len(r) == 4097:
        raise IOError('line limit exceeded for http request line')
    if not r:
        raise EOFError

    r = r.decode('utf-8')
    r = r.strip()
    env = {}

    log['request_line'] = r
    p = r.split()
    if len(p) != 3:
        return None

    env['REQUEST_METHOD'], req_uri, env['SERVER_PROTOCOL'] = p
    env['REQUEST_URI'] = unquote(req_uri)
    path, sep, env['QUERY_STRING'] = req_uri.partition('?')
    env['SCRIPT_NAME'], sep, env['PATH_INFO'] = path.partition('/')

    return env


def read_http_header(f):
    # ritorna None in caso di header errato

    env = {}
    while True:
        r = f.readline(4097)
        if len(r) == 4097:
            raise IOError('line limit exceeded for http header')
        if not r:
            raise IOError('connection closed while reading http header')
        r = r.decode('utf-8')

        if r == '\r\n':
            break

        k, sep, v = r.strip().partition(':')
        if not sep:
            return None
        k = k.upper()
        v = v.strip()

        if k in ('COOKIE', 'RANGE', 'CONNECTION'):
            env['HTTP_' + k] = v
        elif k == 'HOST':
            env['HTTP_HOST'] = v
            if ':' in v:
                env['SERVER_NAME'], env['SERVER_PORT'] = v.split(':')
            else:
                env['SERVER_NAME'] = v
                env['SERVER_PORT'] = ''
        else:
            env['*http_' + k.lower()] = v

    return env


def calc_keep_connection(env):

    # connection: keep-alive. a quanto pare in
    # /usr/local/stow/python-3.1.1/lib/python3.1/wsgiref/handlers.py
    # riga 170 non sono permessi su lato server gli header definiti
    # di hop-by-hop (es... proxy=), in quanto il webserver è una
    # applicazione end-to-end.
    # http://www.w3.org/Protocols/rfc2616/rfc2616-sec13.html
    # ho cercato di imitare il comportamento di lighttpd a riguardo
    # ma wsgiref me lo vieta. Faccio ragionamenti differenti in base
    # al protocollo.

    if env['SERVER_PROTOCOL'] == 'HTTP/1.1':
        # in HTTP/1.1 è implicito keep-alive se non diversamente
        # specificato
        keep_connection = \
            env.get('HTTP_CONNECTION', 'keep-alive').lower() == 'keep-alive'

    elif env['SERVER_PROTOCOL'] == 'HTTP/1.0':
        # in HTTP/1.0 non è dichiarato l'uso, quindi conto
        # sull'indicazione esplicita del client
        keep_connection = \
            env.get('HTTP_CONNECTION', '').lower() == 'keep-alive'

    else:
        # HTTP/0.9
        keep_connection = False


def set_env(sock, rfile):
    # compone l'ambiente.
    # ritorna: env, headers, eventuale applicazione per l'errore,
    # dizionario con dati di log

    error = None
    addr, port = sock.getpeername()
    env = {
        'SERVER_SOFTWARE': server_string,
        'REMOTE_ADDR': addr,
        'REMOTE_PORT': port
    }
    server_headers = [('Server', server_string)]
    log_info = {'addr': addr}

    rlenv = read_http_request_line(rfile, log=log_info)
    if rlenv is not None:
        if rlenv['SERVER_PROTOCOL'] not in ('HTTP/1.1', 'HTTP/1.0', 'HTTP/0.9'):
            if rlenv['SERVER_PROTOCOL'].startswith('HTTP/'):
                raise HTTPError('505', 'HTTP Version Not Supported')
            else:
                raise HTTPError('400', 'Bad Request')

        else:
            hdrenv = read_http_header(rfile)
            if hdrenv is None or (rlenv['SERVER_PROTOCOL'] == 'HTTP/1.1' and \
                            'HTTP_HOST' not in hdrenv):
                raise HTTPError('400', 'Bad Request')

            else:
                # tutto ok.
                env.update(rlenv)
                env.update(hdrenv)

    else:
        raise HTTPError('400', 'Bad Request')

    return env, server_headers, error, log_info


def serve_http_request(sock, application, finish_func=None):

    rfile_socketio = TimeoutSocketIO(sock, 'rb', timeout=30)
    rfile = io.BufferedReader(rfile_socketio, buffer_size=io.DEFAULT_BUFFER_SIZE)
    wfile = io.BufferedWriter(
        TimeoutSocketIO(sock, 'wb', timeout=180), buffer_size=io.DEFAULT_BUFFER_SIZE
    )
    client_addr, client_port = sock.getpeername()
    count = 0
    app_function = application

    try:
        while True:

            env, server_headers, error, log_info = set_env(sock, rfile)
            env['GATEWAY_PROTOCOL'] = 'http'
            if count > 0:
                printlog('reusing kept-alive connection!')

            if not error:
                keep_connection = calc_keep_connection(env)
            else:
                app_function = error
                keep_connection = False
                # wsgihandler vuole necessariamente un SERVER_PROTOCOL
                env['SERVER_PROTOCOL'] = 'HTTP/1.0'

            handler = WrapResponseWSGIHandler(
                rfile, wfile, sys.stderr, env.copy(), multithread=True, multiprocess=True,
                add_headers=server_headers
            )

            try:
                handler.run(app_function)

                if app_function is application and finish_func:
                    finish_func()

            except Exception:
                handler.close()
                raise


            #~ printlog('LOG:', client_addr, env.get('HTTP_HOST', '-'),
                    #~ time.strftime('[%d/%b/%Y:%H:%M:%S]'), '"%(request_line)s"' % log_info,
                    #~ '<http status>')

            if keep_connection:
                # dopo la prima richiesta servita il timeout lo tengo breve
                rfile_socketio.set_timeout(5)
            else:
                break
            count += 1

    except EOFError: ## No, conviene usare eccezioni specifiche
        if count == 0:
            printlog('dumb client %s:%s' % (client_addr, client_port))

    wfile.close()
    rfile.close()

#########################################################################

def parse_http_request(env, rfile):
    # ritorna: {parametri estratti da query_string e POST-form-data}, content
    # content è il content della richiesta in forma grezza (se presente,
    # altrimenti None)

    qs_params = parse_qs(env['QUERY_STRING'], keep_blank_values=True)
    form_params = out = content = None

    if env['REQUEST_METHOD'] == 'GET':
        # la query string è sufficiente
        pass

    elif env['REQUEST_METHOD'] == 'POST':

        if 'CONTENT_TYPE' not in env:
            if env['CONTENT_LENGTH'] != '0':
                # non so esattamente quale sia la risposta corretta
                raise HTTPError(409, 'Missing Content-Type')

        else:
            ct, p = cgi.parse_header(env['CONTENT_TYPE'])

            if ct == 'multipart/form-data':

                if int(env['CONTENT_LENGTH']) > CONTENT_LENGTH_LIMIT:
                    raise ValueError('Maximum CONTENT_LENGTH exceeded')
                form_params = parse_multipart(env, p['boundary'], rfile)

            elif ct == 'application/x-www-form-urlencoded':

                ctlen = int(env['CONTENT_LENGTH'])
                if ctlen > CONTENT_LENGTH_LIMIT:
                    raise ValueError('Maximum CONTENT_LENGTH exceeded')

                # la query string è il contenuto di tutto l'input
                qs = content = rfile.read(ctlen)
                if len(qs) != ctlen:
                    raise HTTPError(400, 'Incomplete content')
                form_params = parse_qs(qs.decode('utf-8'))

            elif ct == 'text/plain':
                if env['CONTENT_LENGTH'] != '0':
                    raise ValueError('unhandled text/plain with content data')

            else:
                # content-type non gestito.
                ctlen = int(env['CONTENT_LENGTH'])
                if ctlen > CONTENT_LENGTH_LIMIT:
                    raise ValueError('Maximum CONTENT_LENGTH exceeded')
                content = rfile.read(ctlen)
                if len(content) != ctlen:
                     raise ValueError('incomplete content')

    # unione dei parametri di form e query string
    if form_params:
        # porto tutti i parametri in form_params
        for k, v in qs_params.items():
            if k in form_params:
                form_params[k].extend(v)
            else:
                form_params[k] = v
        p = form_params
    else:
        p = qs_params

    # eliminazione delle liste per quei parametri che hanno un solo valore
    for k, v in p.items():
        if len(v) == 1:
            p[k] = v[0]

    return p, content


def parse_multipart(env, boundary, rfile):

    def valid_boundary(s, _vb_pattern="^[ -~]{0,200}[!-~]$"):
        return re.match(_vb_pattern, s)

    if not valid_boundary(boundary):
        raise ValueError('invalid boundary')

    bytes_to_read = int(env['CONTENT_LENGTH'])
    line_buffer = []

    new_part = '--' + boundary
    last_part = new_part + '--'
    # per ridurre il numero di decodifiche delle bytestring lette
    # passo subito questi delimitatori in bytestring. Probabilmente la codifica
    # corretta è iso-8859-1
    new_part = new_part.encode('utf-8') + b'\r\n'
    last_part = last_part.encode('utf-8') + b'\r\n'
    params = []
    new_param = None

    status = 'new-part'
    while bytes_to_read > 0:

        ## lettura di una linea
        if line_buffer:
            line = line_buffer.pop(0)
        else:
            line = rfile.readline(bytes_to_read)
            bytes_to_read -= len(line)

        if status == 'new-part':
            if line == new_part:
                new_param = {}
                status = 'read-header'
            elif line == last_part:
                break
            else:
                raise ValueError('content malformed (1)')

        elif status == 'read-header':
            if new_param is None:
                raise ValueError('content malformed (2)')

            line = line.decode('utf-8').strip()
            if not line:
                # linea vuota, termine dell'header
                param_data = []
                status = 'read-data'
            else:
                ctlike, p = cgi.parse_header(line)
                # il content-type-like lo spezzo sul :, la prima parte va a comporre
                # la chiave e la seconda, con il valore del content-type-like,
                # la metto sotto la chiave 1 (numerico) nel dizionario parametri.
                # l'1 è numerico per non andare a coprire le chiave in forma
                # di stringa che sono già presenti
                try:
                    k, v = ctlike.split(':')
                except ValueError:
                    raise ValueError('content malformed (4)')
                p[1] = v.strip()
                new_param[k.strip().lower()] = p

        elif status == 'read-data':

            if line == new_part or line == last_part:
                # termine del blocco dati. è richiesto che il blocco dati
                # finisca con \r\n
                if not param_data or not param_data[-1].endswith(b'\r\n'):
                    raise ValueError('content malformed (3)')

                param_data[-1] = param_data[-1][:-2]  # tolgo l'ultimo \r\n
                # il blocco dati lo metto sotto la chiave None per non
                # rischiare di sovrascrivere le chiavi in formato stringa
                # degli altri valori
                new_param[None] = b''.join(param_data)
                params.append(new_param)
                param_data = new_param = None

                line_buffer.append(line)
                status = 'new-part'
            else:
                param_data.append(line)

    assert bytes_to_read == 0

    # rilettura delle parti estratte e conversione in dizionario dei
    # parametri
    output = {}
    for p in params:
        try:
            cd = p['content-disposition']
            assert cd[1].lower() == 'form-data'

            if 'filename' in cd:
                # è un file in upload
                filedata = p.pop(None)  # lo estraggo per liberare memoria
                v = {
                    'filename': cd['filename'],
                    'file': io.BytesIO(filedata),
                    'size': len(filedata)
                }
            else:
                # è un normale campo. il valore è nel blocco dati
                v = p[None].decode('utf-8')

            pname = cd['name']
            if pname in output:
                output[pname].append(v)
            else:
                output[pname] = [v]

        except KeyError as err:
            raise ValueError('content malformed, missing or wrong Content-Disposition [%s]' % err)

    return output


def merge_headers(d1, d2):
    # sovrascrive le chiavi di d1 con d2 in modo case insensitive

    for k2, v in d2.items():
        k2l = k2.lower()
        for k1 in d1:
            if k1.lower() == k2l:
                d1[k1] = v
                break
        else:
            d1[k2] = v

    return d1


def run_application(application_function, rfile, wfile, env):

    f_time = time.time

    try:
        env['start_time'] = f_time()
        env['input'], _ = parse_http_request(env, rfile)
        env['printlog'] = partial(print, file=sys.stderr)
        rs = application_function(env)
        env['app_func_time'] = f_time()

        if isinstance(rs, dict):
            http_status = rs.get('status', '200 OK')
            if 'headers' in rs:
                response_headers = rs['headers']
            else:
                response_headers = {'Content-Type': 'text/html; charset=utf-8'}

            if 'fileobj' in rs:
                output = rs['fileobj']
            else:
                output = rs['content']
                if not isinstance(output, bytes):
                    output = str(output).encode('utf-8')

                if 'content-length' not in [k.lower() for k in response_headers.keys()]:
                    response_headers['Content-Length'] = len(output)

            cookies = rs.get('cookies')

        else:
            # l'esito è la pagina html, in forma di stringa o bytestring.
            # se non lo è, qualunque cosa sia diventa una stringa e poi utf-8.
            output = str(rs).encode('utf-8') if not isinstance(rs, bytes) else rs

            http_status = '200 OK'
            response_headers = {'Content-Type': 'text/html; charset=utf-8'}
            cookies = None

    except HTTPError as err:
        http_status, response_headers, output = err.response()
        cookies = None

    except Exception as err:
        http_status, response_headers, output = error_response(err, sys.exc_info())
        printlog('{} {}\n{}'.format(http_status, err, sys.exc_info()))
        cookies = None

    ## costruzione dell'output finale e invio.
    try:
        #~ status_plus_headers = ['{} {}'.format(env['SERVER_PROTOCOL'], http_status).encode('ascii')]
        #~ status_plus_headers.extend(('%s: %s' % i).encode('utf-8') for i in response_headers.items())
        #~ status_plus_headers = b'\r\n'.join(status_plus_headers)
        #~ wfile.write(status_plus_headers)
        ## Pare che per il protocollo CGI (o fastcgi?)
        ## lo status sia meglio metterlo nell'header 'Status'
        response_headers['Status'] = env['response_status'] = http_status
        response_headers = '\r\n'.join('%s: %s' % i for i in response_headers.items())

        if cookies:
            # qui mi aspetto che cookies sia un SimpleCookie
            response_headers += '\r\n' + str(cookies)

        wfile.write(response_headers.encode('utf-8'))
        wfile.write(b'\r\n\r\n')


        if isinstance(output, bytes):
            wfile.write(output)
        else:
            # fileobj
            while True:
                data = output.read(64*10240)
                if not data:
                    break
                t0 = time.time()

                wfile.write(data)

    except socket.error as err:
        printlog('socket error', str(err))

    finally:
        wfile.flush()
        wfile.close()
        rfile.close()
        env['end_time'] = f_time()


def error_response(err, exc_info):
    # ritorna status, response_headers, output
    return (
        '500 ERROR',
        {'Content-Type': 'text/plain; charset=utf-8'},
        b'*** ERROR ***\n' + ''.join(traceback.format_exception(*exc_info)).encode('utf-8')
    )

    #~ return {
        #~ 'status': '200 OK',
        #~ 'headers': {'Content-Type': 'text/plain; charset=utf-8'},
        #~ 'content': b'*** ERROR ***\n' + ''.join(traceback.format_exception(*exc_info)).encode('utf-8')
    #~ }


##########################################################################

def main_loop(app_module, serve_req_func):

    global config
    count = 0
    max_exec = config.get('max_exec', 100000000)
    gc_collect_period = config.get('gc_collect', 0)
    out_fd = sys.stdout.fileno()
    in_fd = sys.stdin.fileno()
    finish_func = getattr(app_module, 'finish', None)

    while True:

        try:
            # segnala lo stato di pronto
            os.write(out_fd, b'R')
            com = os.read(in_fd, 1)
            if not com:
                if hasattr(app_module, 'cleanup'):
                    app_module.cleanup()
                sys.exit(0)
            elif com != b'S':
                print('received', repr(com))
                raise SystemExit('mokahandler: ERR protocol', repr(com))

            fd = recv_fd(in_fd)
            #~ printlog('### fd received', time.time())

        except OSError as err:
            raise SystemExit('mokahandler: ERR ' + str(err))

        conn = socket.fromfd(fd, socket.AF_INET, socket.SOCK_STREAM)
        conn.setblocking(1)
        os.close(fd)

        serve_req_func(conn, app_module.application, finish_func)

        # il socket puntato da conn conviene che venga chiuso
        # da serve_req_func prima di eseguire finish_func, altrimenti
        # il client rimane appeso durante l'esecuzione di finish_func.
        # comunque lo chiudo anche qui.
        # nota: non ho controllato cosa fa serve_http_request.
        conn.close()
        count += 1
        sys.stderr.flush()

        if gc_collect_period and not (count % gc_collect_period):
            gc.collect()

        if count >= max_exec:
            printlog('served {} connections, exiting'.format(count))
            os.write(out_fd, b'Q')
            com = os.read(in_fd, 1)
            if not com:
                # connessione chiusa lato moka. ottimo!
                if hasattr(app_module, 'cleanup'):
                    app_module.cleanup()
                sys.exit(0)
            else:
                raise SystemExit('mokahandler: ERROR: moka server didn\'t close its socket; quitting.')


if __name__ == '__main__':

    parser = optparse.OptionParser()
    parser.add_option('-c', '--config', type='str', action='store', dest='config')
    opt, args = parser.parse_args(sys.argv[1:])

    config = read_config_file(opt.config, bool_args=('http', ), int_args=('max_exec', ))

    protocol = config.get('protocol', 'http')
    if protocol == 'http':
        serve_req_func = serve_http_request
    elif protocol == 'scgi':
        serve_req_func = serve_scgi_request
    elif protocol == 'fastcgi':
        serve_req_func = serve_fastcgi_request
    else:
        raise SystemExit('wrong protocol %r' % protocol)

    if not config.get('application_module', False):
        raise SystemExit('application module name needed')

    chdir = config.get('basedir', False)
    if chdir:
        os.chdir(chdir)
        if '' not in sys.path:
            sys.path.insert(0, '')

    module_name = config.get('application_module', False)
    try:
        m = __import__(module_name)
    except Exception as err:
        printlog('error loading application: %s' % err)
        printlog('TRACEBACK:\n%s' % ''.join(traceback.format_exception(*sys.exc_info())))
        raise SystemExit(1)

    assert hasattr(m, 'application')

    m.init()

    main_loop(m, serve_req_func)





