# -*- coding: utf-8 -*-

from __future__ import division, print_function

__version__ = (0, 13)

import sys, json, itertools
from functools import partial

try:
    import ssl
except ImportError:
    ssl = None

user_agent = 'jrpc/0'     # 'jrpc/' + '.'.join(str(n) for n in __version__)


if sys.version_info[0] < 3:

    import BaseHTTPServer as httpserver
    import SocketServer as socketserver
    import httplib as httpclient
    import urllib2 as urllib_request

    if ssl:
        # pezza per problemi in python 2.6 con la versione di ssl
        # http://bugs.python.org/issue11220
        import httplib, socket

        class HTTPSConnectionV3(httplib.HTTPSConnection):
            def __init__(self, *args, **kwargs):
                httplib.HTTPSConnection.__init__(self, *args, **kwargs)

            def connect(self):
                sock = socket.create_connection((self.host, self.port), self.timeout)
                if self._tunnel_host:
                    self.sock = sock
                    self._tunnel()
                try:
                    self.sock = ssl.wrap_socket(sock, self.key_file, self.cert_file, ssl_version=ssl.PROTOCOL_SSLv3)
                except ssl.SSLError as e:
                    ## print("Trying SSLv3.")
                    self.sock = ssl.wrap_socket(sock, self.key_file, self.cert_file, ssl_version=ssl.PROTOCOL_SSLv23)

        class HTTPSHandlerV3(urllib_request.HTTPSHandler):
            def https_open(self, req):
                return self.do_open(HTTPSConnectionV3, req)

        # aggiunta per TLSv1

        class HTTPSConnectionTLSv1(httplib.HTTPSConnection):
            def __init__(self, *args, **kwargs):
                httplib.HTTPSConnection.__init__(self, *args, **kwargs)

            def connect(self):
                sock = socket.create_connection((self.host, self.port), self.timeout)
                if self._tunnel_host:
                    self.sock = sock
                    self._tunnel()

                self.sock = ssl.wrap_socket(sock, self.key_file, self.cert_file, ssl_version=ssl.PROTOCOL_TLSv1)

        class HTTPSHandlerTLSv1(urllib_request.HTTPSHandler):
            def https_open(self, req):
                return self.do_open(HTTPSConnectionTLSv1, req)

        # install opener
        urllib_request.install_opener(urllib_request.build_opener(HTTPSHandlerTLSv1()))

else:
    from http import server as httpserver
    from http import client as httpclient
    import socketserver
    import urllib.request as urllib_request
    basestring = str


PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602
INTERNAL_ERROR = -32603
SERVER_ERROR = -32000   # da -32000 a -32099 per errori applicativi




# classe per gli errori di protocollo.
# NOTA: gli errori legati all'http li lascio generare a urllib_request (sono
# sottoclassi di IOError).
class JrpcError(Exception):

    def __init__(self, error_text, code=None, id=None, message=None, data=None):

        super(JrpcError, self).__init__(error_text)

        if message is None:
            message = error_text

        self.message = message
        self.code = code
        self.data = data

    def build_error_response(self, request):

        if self.code is None:
            raise ValueError('missing error code')

        return build_error_response(request, self.code, self.message, self.data)


class HttpJrpcClient:

    Error = JrpcError

    def __init__(self, url, timeout=120, protocol_version='2.0', default_params=None,
                    response_size_limit=10*1024**2, no_proxy=False):

        # OCCHIO: IL no_proxy FA A PUGNI CON LA PEZZA PER LA VERSIONE SSL
        # CHE VEDI SOPRA. E' DA SISTEMARE OPPURE TOGLIERE IL no_proxy E
        # LASCIARE TUTTO ALLE VARIABILI D'AMBIENTE (TIPO... TOGLILE!)

        assert protocol_version == '2.0'
        self.protocol_version = '2.0'

        if not url.startswith('http://') and not url.startswith('https://'):
            raise ValueError('server url protocol must be http or https (using %s)' % url)

        if not ssl and url.startswith('https://'):
            raise ValueError('ssl module unavailable, unable to use https')

        self.url = url
        self.timeout = timeout
        self.response_size_limit = response_size_limit
        self.no_proxy = no_proxy
        self.default_params = default_params
        self.id_generator = itertools.count()



    def __getattr__(self, name):
        return partial(self.call_method, name)


    def call_method(self, name, *args, **params):

        if args:
            raise ValueError('only keyword parameters')

        if self.default_params:
            p = self.default_params.copy()
            p.update(params)
            params = p

        req_id = next(self.id_generator)
        request_data = {
            'jsonrpc': self.protocol_version,
            'method': name,
            'params': params,
            'id': req_id
        }

        req = urllib_request.Request(self.url, json.dumps(request_data).encode('ascii'),
                {'Content-Type': 'application/json-rpc',
                 'User-Agent': user_agent})

        if self.no_proxy:
            proxy_handler = urllib_request.ProxyHandler({})
            opener = urllib_request.build_opener(proxy_handler)
            call = opener.open(req, timeout=self.timeout)
        else:
            call = urllib_request.urlopen(req, timeout=self.timeout)

        try:
            response_size = int(call.headers['Content-Length'])
        except KeyError:
            raise KeyError('missing Content-Length header in server response')

        if response_size > self.response_size_limit:
            raise JrpcError('response size limit exceeded')

        try:
            response = json.loads(call.read().decode('utf-8'))
        except (SyntaxError, ValueError) as err:
            raise JrpcError('JSON error: ' + str(err))

        if response.get('jsonrpc') != '2.0':
            raise JrpcError('invalid response')

        if response.get('id') != req_id:
            raise JrpcError('non-matching id in server response')

        if 'error' in response:
            raise JrpcError('error from server: ' + response['error'].get('message', '*no message available*'))

        return response['result']


class HttpJrpcRequestHandler(httpserver.BaseHTTPRequestHandler):

    def map_method(self, name):
        # questo deve ritornare None se il metodo non esiste
        raise NotImplementedError

    def do_GET(self):
        self.send_error(httpclient.METHOD_NOT_ALLOWED, 'JSON-OVER-HTTP requires POST requests')

    def do_POST(self):

        try:

            request = parse_request(self.rfile.read(int(self.headers['Content-Length'])))
            method = self.map_method(request['method'])
            if method is None:
                raise JrpcError('no method named {}'.format(request['method']), METHOD_NOT_FOUND)

            try:
                if 'params' not in request:
                    rs = method()
                elif isinstance(request['params'], dict):
                    rs = method(**request['params'])
                elif isinstance(request['params'], list):
                    rs = method(*request['params'])
                else:
                    rs = method(request['params'])

                reply = build_response(request, rs)

            except JrpcError:
                raise

            except Exception as err:
                reply = build_error_response(request, INVALID_PARAMS).encode('ascii')
                self.send_response(200, 'OK')
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', len(reply))
                self.end_headers()
                self.wfile.write(reply)
                self.wfile.flush()
                raise

        except JrpcError as err:
            reply = err.build_error_response(request)

        #~ except AttributeError:
            #~ reply = build_error_response(request, METHOD_NOT_FOUND)

        self.send_response(200, 'OK')
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', len(reply))
        self.end_headers()
        self.wfile.write(reply.encode('utf-8'))



## ------------------------------------------------------------------------------------

def parse_request(json_data):
    '''
    legge una richiesta, verifica la correttezza e ritorna un dizionario.
    in caso di errore genera un JrpcError
    '''

    try:
        req = json.loads(json_data.decode('utf-8'))
    except (SyntaxError, ValueError) as err:
        raise JrpcError('invalid json. ' + str(err), code=PARSE_ERROR)

    if not isinstance(req, dict):
        raise JrpcError('invalid request', code=INVALID_REQUEST, id=None)

    if req.get('jsonrpc') != '2.0':
        raise JrpcError('invalid "jsonrpc" member', code=INVALID_REQUEST, id=req.get('id'))

    try:
        req['id']
        req['method']
    except KeyError as err:
        raise JrpcError('missing member "' + str(err) + '"', code=INVALID_REQUEST, id=req.get('id'))

    if not isinstance(req['method'], basestring):
        raise JrpcError('invalid method name', code=INVALID_REQUEST, id=req.get('id'))

    return req


def build_response(parsed_request, response_result):
    '''
    costruisce la risposta della richiesta indicata (già passata da parse_request()).
    response_result è in contenuto della risposta alla chiamata.
    '''

    response = {
        'id': parsed_request['id'],
        'result': response_result,
        'jsonrpc': '2.0'
    }
    return json.dumps(response)


def build_error_response(parsed_request, code, message='error', data=None):

    response = {
        'id': parsed_request['id'],
        'error': {
            'code': code,
            'message': message
        },
        'jsonrpc': '2.0'
    }

    if data is not None:
        response['error']['data'] = data

    return json.dumps(response)


## ------------------------------------------------------------------------------------
def test_server():

    PORT = 7000

    class LocalHandler(HttpJrpcRequestHandler):

        def prova(self, **data):
            return '**> ' + str(data) + ' <**'

    socketserver.TCPServer.allow_reuse_address = True

    httpd = socketserver.TCPServer(("", PORT), LocalHandler)

    print("waiting, port", PORT)
    httpd.serve_forever()


if __name__ == '__main__':

    args = sys.argv[1:]
    if args and args[0] == 'server':
        test_server()
        sys.exit(0)

    #~ c = HttpJrpcClient('http://192.168.7.1:9999/', no_proxy=True)
    #~ c = HttpJrpcClient('https://192.168.7.1:4443/', no_proxy=False)
    #~ print('***\n', c.example({'x': 'y'}), '\n***')

    c = HttpJrpcClient('http://localhost:7000', no_proxy=False)
    print(c.prova(test=123))


    '''
    curl --data '{"id": 9, "jsonrpc": "2.0", "method": "prova"}' http:/localhost:7000
    '''



