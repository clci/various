#! /usr/bin/env python3
# -*- coding: utf-8 -*-

## nota: serve python 3.2+

# http://python.ca/scgi/protocol.txt
# http://scotdoyle.com/python-epoll-howto.html

## ChangeLog
## 0.12.3   : * eliminato loop_count.
## 0.12.0   : * gestione logfile, e riapertura dello stesso al cambio di handler.
##              nota che lo stderr degli handler finisce sul logfile
## 0.11.0   : * passato ad epoll, e altre piccole modifiche
## 0.10.9   : * cambiato il nome del parametro weblisten in webinterface
## 0.10.8   : * sostituito os.dup() con os.dup2() nel fork(). Necessario per python 3.4+
## 0.10.7   : * cambiato msgutil.c con libmoka.c, e aggiunte le funzioni C per fastcgi
## 0.10.3   : * logica di chiusura degli handler inattivi.
## 0.10.2   : * revisione completa di MokaServer, Handler.
## 0.10.0   : * rimozione della gestione delle richieste pendenti. In caso
##            di indisponibilità di handler pronti non si legge dal socket
##            di ascolto e si lasciano le eventuali richieste pendenti in mano
##            al web server.
##            * aggiunto in configurazione il parametro backlog_size.
##            * ottimizzazioni varie.
## 0.9.6    : ...

__version__ = (0, 13, 0)
__version_string__ = '.'.join(str(x) for x in __version__)

import sys, os, time, socket, signal, errno, select, pwd, resource
import subprocess, threading, optparse, configparser, itertools
import platform
from functools import partial

import http.server as httpserver
import http.client as httpclient
import cgi

from ctypes import cdll, c_int, c_char_p
libmoka = os.path.join(os.path.dirname(__file__), '.', 'libmoka-%s.so' % platform.machine())
libmoka = cdll.LoadLibrary(libmoka)
libmoka.get_errstr.restype = c_char_p

debug_file = None


def send_fd(dest_fd, fd):

    rc = libmoka.send_fd(dest_fd, fd)
    if rc < 0:
        raise IOError(libmoka.get_errstr())


def printlog(*args):

    txt = ' '.join(str(a) for a in args)
    while True:
        try:
            tname = threading.current_thread().getName()
            cur_time = time.strftime('%y%m%d-%H%M%S')
            print('{} {}: {}'.format(cur_time, tname, txt))
            break
        except IOError:
            # può avvenire [Errno 4] Interrupted system call
            pass


def debuglog(*args):

    return

    if debug_file:
        txt = ' '.join(str(a) for a in args)
        print('{} {}: {}'.format(time.strftime('%y%m%d-%H%M%S'),
                threading.current_thread().getName(), txt), file=debug_file)
        debug_file.flush()


# decoratore per debug
def pdebug(f):

    def decorator(*args, **kw):
        debuglog('| >', f.__name__, args, kw, ']')
        r = f(*args, **kw)
        debuglog('| <', f.__name__, 'return:', r,  ']')
        return r

    return decorator


def session_detach():

    if os.getppid() == 1:
        # il padre e' init, non c'e' bisogno di staccarsi
        pass

    else:

        signal.signal(signal.SIGTTOU, signal.SIG_IGN)
        signal.signal(signal.SIGTTIN, signal.SIG_IGN)
        signal.signal(signal.SIGTSTP, signal.SIG_IGN)

        if os.fork() != 0:
            sys.exit(0)

        os.setpgrp()

        signal.signal(signal.SIGHUP, signal.SIG_IGN)

        if os.fork() != 0:
            sys.exit(0)

    os.close(sys.stdin.fileno())
    #~ os.close(sys.stdout.fileno())
    #~ os.close(sys.stderr.fileno())

    ## sposta la directory corrente su un filesystem che
    ## non verra' sicuramente smontato
    #~ os.chdir('/')    # no, questo serve

    ## annulla qualunque modo di creazione file ereditato
    os.umask(0)


## -------------------------------------------------------------------------
class Tag:

    def __init__(self, tag_name, *content, **attr):

        self.tag_name = tag_name

        self.content = []
        self.add(*content)

        if 'klass' in attr:
            attr['class'] = attr.pop('klass')
        self.attr = attr


    def iter_indent(self):

        attr_str = ''.join(' %s="%s"' % a for a in self.attr.items() if a[1] is not None)

        if self.content:
            if len(self.content) == 1:
                yield '<{0}{1}>{2}</{0}>'.format(
                            self.tag_name, attr_str, self.content[0])
            else:
                yield '<{0}{1}>'.format(self.tag_name, attr_str)
                for c in self.content:
                    #~ yield str(c)   # evita l'indentazione (più veloce)
                    for row in c:
                        yield '  ' + row
                yield '</{0}>'.format(self.tag_name)

        else:
            yield '<{0}{1} />'.format(self.tag_name, attr_str)


    def iter_no_indent(self):

        attr_str = ''.join(' %s="%s"' % a for a in self.attr.items() if a[1] is not None)

        if self.content:
            if len(self.content) == 1:
                yield '<{0}{1}>{2}</{0}>'.format(
                            self.tag_name, attr_str, self.content[0])
            else:
                yield '<{0}{1}>'.format(self.tag_name, attr_str)
                for c in self.content:
                    yield str(c)   # evita l'indentazione (più veloce)
                    #~ for row in c:
                        #~ yield '  ' + row
                yield '</{0}>'.format(self.tag_name)

        else:
            yield '<{0}{1} />'.format(self.tag_name, attr_str)


    __iter__ = iter_no_indent


    def __str__(self):
        return '\n'.join(self)


    def add(self, *args):
        self.content.extend(c if isinstance(c, Tag) else cgi.escape(str(c)) for c in args)


    @classmethod
    def indent_enabled(cls, status):
        cls.__iter__ = cls.iter_indent if status else cls.iter_no_indent

    @classmethod
    def indent_status(cls):
        return cls.__iter__ == cls.__iter__



class WebInterfaceHTTPServer(httpserver.HTTPServer):
    allow_reuse_address = True

    def __init__(self, addr, handler, mokaserver, config):

        self.moka = mokaserver
        self.config = config
        super().__init__(addr, handler)
        printlog('web interface, address %s:%s' % addr)


class WebInterfaceHandler(httpserver.BaseHTTPRequestHandler):

    def do_GET(self):

        if self.path == '/reset':
            self.close_handlers()
            self.reload_response()
            return

        elif self.path.startswith('/kill/'):
            pid = int(self.path[6:])
            # controllo che il pid sia tra quelli degli handler
            status = self.server.moka.get_status()
            if not status:
                printlog('*** LOCKED ***')
                self.reload_response()
                return

            if pid not in [h['pid'] for h in status['handlers']]:
                printlog('pid %s does not belong to any handler' % pid)
                self.reload_response()
                return

            # ok, pid buono
            try:
                printlog('killing pid %s' % pid)
                os.kill(pid, signal.SIGTERM)
            except OSError as err:
                printlog(str(err))
            self.reload_response()
            return

        elif self.path == '/env':

            self.send_response(httpclient.OK)
            self.send_header('Content-Type', 'text/plain;charset=utf-8')
            self.end_headers()

            page = []
            for k, v in sorted(os.environ.items()):
                page.append('%-30s : %s' % (k, v))

            self.wfile.write('\n'.join(page).encode('utf-8'))
            self.wfile.flush()
            return


        self.send_response(httpclient.OK)
        self.send_header('Content-Type', 'text/html;charset=utf-8')
        self.end_headers()

        now = time.time()

        page = [
            '<html><head>\n' \
            '<meta http-equiv="content-type" content="text/html; charset=utf-8" />\n' \
            '<title>%(name)s - %(host)s</title>\n' \
            '''
            <style type="text/css">
                body { background: #F4FAFF; color: #025; font-family: monospace }
                a { color: black; text-decoration: none }
                a:hover { color: white; background: #025 }
            </style>
            ''' \
            '</head><body>\n' \
            '<b>%(name)s</b><br>\n' \
            'host: %(host)s<br>\n' \
            'config: %(configfile)s<br>\n' \
            'pid: %(pid)s<br>\n' \
            'started: %(start_time)s (%(start_delta)s)<br>\n' \
            '%(time)s<br>\n' \
            '-----------------------------------------------------------<br>\n' \
            '<a href="/reset">[RESET HANDLERS]</a> | <a href="/env">[ENV]</a><br>\n' \
            '-----------------------------------------------------------<br>\n' \
                    % {
                        'name': '⚗ Moka Server ' + __version_string__,
                        'time': time.ctime(),
                        'start_time': time.ctime(self.server.moka.start_time),
                        'start_delta': self.long_timedelta_to_str(now - self.server.moka.start_time),
                        'host': os.uname()[1],
                        'configfile': self.server.config['configfile'],
                        'pid': os.getpid()
                      }
        ]

        status = self.server.moka.get_status()

        if status:
            table = []
            table.append('HANDLERS:')

            tabstr = '%(id)6s | %(sig)s %(pid)6s %(vmsize)10s %(rssize)10s %(count)6s %(status)-10s %(t)s'
            table.append(
                tabstr % {'id': 'ID', 'sig': 'sig', 'pid': 'PID', 'vmsize': 'VM',
                            'rssize': 'RSS', 'status': 'STATUS', 'count': 'COUNT', 't': 'TIME'}
            )

            count = 0
            for h in status['handlers']:
                h['sig'] = '<a href="/kill/%(pid)s" title="SIGTERM pid %(pid)s">[T]</a>' % h
                h.update(self.read_mem_size(h['pid']))
                h['t'] = time.strftime(self.timedelta_to_str(now - h['status_time']))
                table.append(tabstr % h)
                count += h['count']

            page.append('<pre>')
            page.append('\n'.join(table))
            page.append('</pre>')
            page.append('----------------------------------------------<br>\n')
            tot_count = status['total_count'] + count
            page.append('page count: %s (%.4f pages/hour)<br>\n' % \
                        (tot_count, tot_count / ((time.time() - self.server.moka.start_time) / 3600.0)))
            page.append('</body></html>')
        else:
            printlog('*** LOCKED ***')
            page.append('*** LOCKED ***')

        try:
            self.wfile.write(''.join(page).encode('utf-8'))
            self.wfile.flush()
        except (IOError, OSError) as err:
            printlog('ERR: ' + str(err))


    def read_mem_size(self, pid):

        out = {}
        try:
            for line in open('/proc/%s/status' % pid, 'r'):

                if line.startswith('VmSize:'):
                    out['vmsize'] = line.partition(':')[2].strip()
                elif line.startswith('VmRSS:'):
                    out['rssize'] = line.partition(':')[2].strip()

                if len(out) == 2:
                    return out

        except Exception as err:
            pass

        return {'vmsize': 'ERR', 'rssize': 'ERR'}


    def log_message(self, *args, **kw):
        pass


    def close_handlers(self):
        os.kill(os.getpid(), signal.SIGHUP)


    def reload_response(self):

        self.send_response(httpclient.FOUND)   # 302
        self.send_header('Location', '/')
        self.end_headers()


    @staticmethod
    def timedelta_to_str(t):

        s = ''
        h, t = divmod(t, 3600)
        if h:
            s += '%dh' % h
        m, t = divmod(t, 60)
        if m or h:
            s += '%dm' % m

        s += '%ds' % t
        return s


    @staticmethod
    def long_timedelta_to_str(t):

        s = ''
        d, t = divmod(t, 86400)
        if d:
            s += '%dd' % d
        h, t = divmod(t, 3600)
        if d or h:
            s += '%dh' % h
        m, t = divmod(t, 60)
        if d or h or m:
            s += '%dm' % m

        return s


## -------------------------------------------------------------------------

class Handler:

    id_counter = itertools.count()
    epoll = None
    fd_map = [None] * (16*1024)

    def __init__(self, configfile):

        self.configfile = configfile
        self.id = next(self.id_counter)
        self.count = 0
        self.pid = None
        self.status = None
        self.status_time = None
        self.start_time = None
        self.close_when_ready = False
        self.closed = False


    def start(self):

        ## socketpair per l'invio di dati e di socket verso l'handler
        localsock, remotesock = socket.socketpair()

        ## pipe per ricevere lo stdout dell'handler
        rpipe, wpipe = os.pipe()

        pid = os.fork()

        if pid == 0:
            ## HANDLER
            # sostituzione di stdin/stdout
            os.dup2(remotesock.fileno(), 0)
            os.dup2(wpipe, 1)

            # lo stderr lo lascio com'è. chiudo tutto il resto,
            # quindi anche localsock e rpipe
            os.closerange(3, resource.getrlimit(resource.RLIMIT_NOFILE)[0])

            ## nuovo interprete
            h = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'mokahandler.py')
            os.execvpe(h, ('MOKAHANDLER', '-c', self.configfile), dict(os.environ))

        ## SERVER
        self.pid = pid
        self.stdin_socket = localsock
        self.stdin_socket_fd = localsock.fileno()
        self.read_pipe_fd = rpipe
        remotesock.close()
        os.close(wpipe)
        self.status = 'INIT'
        self.start_time = self.status_time = time.time()

        self.register_fd(rpipe)

        return self      # per comodità


    def fileno(self):
        return self.read_pipe_fd


    def register_fd(self, fd):

        #~ assert fd not in self.fd_map
        self.epoll.register(fd, select.EPOLLIN)
        self.fd_map[fd] = self


    def unregister_fd(self, fd):

        assert self.fd_map[fd] is self
        self.fd_map[fd] = None
        self.epoll.unregister(fd)


    #~ @pdebug
    def send_socket(self, sock, addr=None):

        # segnala all'handler l'arrivo di un socket e mandalo
        self.stdin_socket.send(b'S')
        send_fd(self.stdin_socket_fd, sock.fileno())
        self.count += 1
        self.status = 'BUSY'
        self.status_time = time.time()
        self.client_addr = addr


    #~ @pdebug
    def read(self):

        c = os.read(self.read_pipe_fd, 1)
        if c == b'R':
            if not self.close_when_ready:
                self.status = 'READY'
                self.status_time = time.time()
            else:
                self.close()
                self.status = 'CLOSING'
                self.status_time = time.time()

        elif c == b'Q':
            self.close()
            self.status = 'CLOSING'
            self.status_time = time.time()

        elif not c:
            # chiusura inaspettata... handler morto
            self.close()
            self.status = 'DIED'
            self.status_time = time.time()

        else:
            #~ print('+++++++++', repr(os.read(self.read_pipe_fd, 100000)), file=sys.stderr)
            # può essere che qualcosa, nell'handler, scrive sul fd
            # destinato alla comunicazione con Moka server. In caso di
            # errore mi risulta che py-postgres lo faccia (non ho
            # approfondito sul perchè).
            # In sintesi, un handler che fa questo deve morire.
            self.close()
            self.status = 'TERMINATED'
            self.status_time = time.time()
            os.kill(self.pid, signal.SIGTERM)


    def close_request(self):

        # chiusura pulita
        self.close_when_ready = True

        if self.status == 'READY':
            self.close()
            self.status = 'CLOSING'
            self.status_time = time.time()


    def close(self):

        if not self.closed:
            self.stdin_socket.close()
            os.close(self.read_pipe_fd)
            self.unregister_fd(self.read_pipe_fd)
            self.stdin_socket = self.read_pipe_fd = self.stdin_socket_fd = None
            self.closed = True


class MokaServer:

    # tempo di inattività minimo per la chiusura di un handler pronto
    IDLE_TIMEOUT = 300.0

    def __init__(self, listen_socket, configfile, min_handlers=4, max_handlers=10,
                    log_reloader=None):

        self.listen_socket = listen_socket
        self.configfile = configfile
        self.min_handlers = min_handlers
        self.max_handlers = max_handlers
        self.log_reloader = log_reloader
        self.stop_new_handlers = None

        self.h_list = []        # lista di tutti gli handler pronti o occupati
        self.ready_list = []    # solo quelli pronti da usare
        self.h_list_for_replace = []
        self.exit = False
        self.start_handlers_method = self.check_and_start_new_handlers
        self.sig_queue = []
        self.epoll = select.epoll()

        # conteggio della pagine servite. viene incrementato sommando
        # il totale di ogni handler nel momento in cui viene chiuso
        self.total_count = 0

        self.lock = threading.Lock()


    def start(self):

        signal.signal(signal.SIGHUP, self.sig_handler)
        signal.signal(signal.SIGINT, self.sig_handler)
        signal.signal(signal.SIGCHLD, self.sig_handler)

        self.start_time = time.time()
        self.main_loop()


    def sig_handler(self, sig_number, frame):
        self.sig_queue.append(sig_number)


    def main_loop(self):

        Handler.epoll = epoll = self.epoll

        listen_socket = self.listen_socket
        listen_fd = listen_socket.fileno()
        listening = False
        h_list = self.h_list
        ready_list = self.ready_list
        lock = self.lock
        sig_queue = self.sig_queue

        while True:

            if not self.exit:
                self.start_handlers_method()
            else:
                if not self.h_list:
                    # richiesta di uscita e tutti gli handler terminati
                    break

            if (ready_list and not self.exit) and not listening:
                epoll.register(listen_fd, select.EPOLLIN)
                listening = True
            elif (not ready_list or self.exit) and listening:
                epoll.unregister(listen_fd)
                listening = False

            timeout = .2 if self.stop_new_handlers else 15

            try:
                events = epoll.poll(timeout)

            except select.error as err:
                if err.args[0] == errno.EINTR:
                    #~ printlog('************** EINTR **************')
                    events = []
                    if self.exit and listen_socket:
                        if listening:
                            epoll.unregister(listen_fd)
                            listening = False
                        listen_socket.close()
                        listen_socket = None
                        for h in self.h_list:
                            h.close_request()
                else:
                    raise

            with lock:
                if events:
                    for fd, event_mask in events:

                        if fd == listen_fd:
                            sock = listen_socket.accept()
                            #~ printlog('### about to pass', time.time())
                            self.pass_request(*sock)
                            sock = None

                        else:
                            self.listen_handler(fd)

                else:
                    # è scattato il timeout
                    self.close_idle_handlers()

                # gestione segnali
                if sig_queue:
                    sig_number = sig_queue.pop(0)

                    if sig_number == signal.SIGCHLD:
                        self.check_closed_handlers()

                    elif sig_number == signal.SIGHUP:
                        if self.start_handlers_method != self.start_handlers_for_replace:

                            if self.log_reloader:
                                self.log_reloader()

                            printlog('** REPLACING HANDLERS')
                            self.start_handlers_method = self.start_handlers_for_replace
                        else:
                            printlog('** HANDLER REPLACE ALREADY REQUESTED')

                    elif sig_number == signal.SIGINT:
                        printlog('** INTERRUPT REQUEST - EXIT')
                        self.exit = True

                    else:
                        printlog('!! unhandled signal', sig_number)


    def listen_handler(self, fd):
        # gestione dei messaggi provenienti dagli handler, sul
        # loro stdout. da usare solo quando select conferma
        # la presenza di dati da leggere

        h = Handler.fd_map[fd]
        h.read()

        if h.status == 'READY':
            h.ready_list.append(h)

        else:
            if h.status == 'CLOSING':
                printlog('handler id {0.id} (pid {0.pid}) quit request'.format(h))

            else:
                # per quello che riguarda gli altri stati, non c'è molto da
                # gestire, si tratta di condizioni d'errore
                if h in self.ready_list:
                    printlog('handler {0.id} (pid {0.pid}, status={0.status}) unexpected change from READY status'.format(h))
                    self.ready_list.remove(h)


    def check_closed_handlers(self):

        while True:
            try:
                (pid, rc) = os.waitpid(-1, os.WNOHANG)
            except OSError as err:
                if err.errno == errno.ECHILD:
                    break
                raise

            if pid == 0:
                break

            # ricerca dell'handler con questo pid
            for h in self.h_list:
                if h.pid == pid:
                    break
            else:
                raise AssertionError('pid {0} not listed in any queue'.format(pid))

            self.h_list.remove(h)
            status = h.status
            h.close()
            self.total_count += h.count

            if status == 'CLOSING':
                # la chiusura di questo handler è attesa, quindi tutto bene
                printlog('handler id {0.id} (pid {0.pid}) exited, rc=0x{1:04x}'.format(h, rc))

            else:
                # chiusura inaspettata
                printlog('handler id {0.id} (pid {0.pid}) unexpected close, status={2}, rc=0x{1:04x}'.format(h, rc, status))

                if status == 'INIT':
                    # scoppiato un handler durante il suo avvio. meglio
                    # aspettare un poco prima di lanciarne altri
                    self.stop_new_handlers = time.time() + 2.0

                if h in self.ready_list:
                    self.ready_list.remove(h)


    def check_and_start_new_handlers(self):

        if self.stop_new_handlers:
            if time.time() >= self.stop_new_handlers:
                self.stop_new_handlers = None
            else:
                return

        if (len(self.h_list) < self.min_handlers) or \
                (not self.ready_list and len(self.h_list) < self.max_handlers):

            # verifico che non ce ne siano già in INIT
            if not any(h.status == 'INIT' for h in self.h_list):
                h = Handler(self.configfile).start()
                self.h_list.append(h)
                h.ready_list = self.ready_list
                printlog('started handler {0.id} (pid {0.pid})'.format(h))


    def start_handlers_for_replace(self):

        if self.stop_new_handlers:
            if time.time() >= self.stop_new_handlers:
                self.stop_new_handlers = None
            else:
                return

        if len(self.h_list_for_replace) < self.min_handlers:

            # verifico che non ce ne siano già in INIT
            if not any(h.status == 'INIT' for h in self.h_list):
                h = Handler(self.configfile).start()
                self.h_list.append(h)
                h.ready_list = self.h_list_for_replace
                printlog('started handler for replace {0.id} (pid {0.pid})'.format(h))

        else:
            # tutti i nuovi handler sono READY.
            # chiudo tutti gli altri
            for h in self.h_list:
                if h not in self.h_list_for_replace:
                    # handler vecchio
                    h.close_request()
                    if h in self.ready_list:
                        self.ready_list.remove(h)

                else:
                    # handler nuovo
                    h.ready_list = self.ready_list
                    self.ready_list.append(h)
                    self.h_list_for_replace.remove(h)

            self.start_handlers_method = self.check_and_start_new_handlers
            printlog('** HANDLERS REPLACED')



    def pass_request(self, sock, addr):

        # cerco di tenere occupato sempre lo stesso handler, visto che
        # potrebbe essere quello con più moduli caricati.
        # Non sono sicuro che sia la logica più sensata.
        h = self.ready_list.pop()
        try:
            h.send_socket(sock, addr)
        except (IOError, socket.error) as err:
            printlog('** ERROR WHILE SENDING SOCKET TO HANDLER ID '
               '{0.id} (pid {0.pid}): {1} - HANDLER CLOSED'.format(h, str(err)))
            h.close()
            return False

        return True


    def close_idle_handlers(self):

        if len(self.ready_list) > self.min_handlers:
            idle_handlers = [(h.status_time, h) for h in self.ready_list]
            if idle_handlers:
                # ne chiudo uno solo ad ogni giro. non c'e' fretta
                idle_handlers.sort()
                t, h = idle_handlers[0]
                if (time.time() - t) > self.IDLE_TIMEOUT:
                    printlog('closing idle handler id %s' % h.id)
                    h.close_request()
                    self.ready_list.remove(h)


    def get_status(self):
        # informazioni generali sullo stato interno del mokaserver, thread-safe

        if not self.lock.acquire(True, timeout=5):
            return None

        '''
        ## VECCHIA LOGICA, python versione precedente a 3.2
        # i threading.Lock non permettono di fissare un timeout. tento
        # di acquisirlo per tentativi
        n = 20
        while n > 0:
            if not self.lock.acquire(False):
                # è bloccato
                time.sleep(.05)
                n -= 1
            else:
                break

        if n == 0:
            # lock non ottenuto
            return None
        '''

        try:
            tot = self.total_count
            hlist = []

            for h in self.h_list:
                hlist.append({
                    'pid': h.pid,
                    'id': h.id,
                    'count': h.count,
                    'status': h.status,
                    'status_time': h.status_time
                })

        finally:
            self.lock.release()

        return {
            'handlers': hlist,
            'total_count': tot
        }


## -------------------------------------------------------------------------

def create_listen_socket(addr_str, backlog=10):
    # valuta la stringa per capire se serve un socket TCP o su file
    # backlog: vedi man listen

    if ':' in addr_str:
        # socket di rete
        addr, port = addr_str.strip().split(':')
        addr = (addr, int(port))
        lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        lsock.bind(addr)
    else:
        # socket su file
        if os.path.exists(addr_str):
            os.remove(addr_str)
        lsock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        addr = addr_str
        lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        lsock.bind(addr)
        os.chmod(addr_str, 0o777)

    lsock.listen(backlog)

    return lsock


def read_config_file(filename, bool_args=(), int_args=()):

    cp = configparser.ConfigParser(strict=False)
    try:
        cp.readfp(open(filename))
        config_common = dict(cp.items('common'))
        config = config_common.copy()
        config.update(dict(cp.items('moka-server')))
    except (IOError, OSError, configparser.Error) as err:
        printlog('error reading config file %s: %s' % (filename, err))
        raise

    for arg in bool_args:
        if arg in config:
            config[arg] = True if config[arg].upper() in ('1', 'Y', 'YES',
                                        'TRUE') else False
    for arg in int_args:
        if arg in config:
            config[arg] = int(config[arg])

    for arg in config:
        if arg not in int_args and arg not in bool_args:
            config[arg] = config[arg].format(**config_common)

    return config


def change_user(username):

    if os.getuid() == 0:        # si cambia solo se si è root
        new_uid = pwd.getpwnam(username)[2]
        if new_uid != os.getuid():
            os.seteuid(new_uid)
            return True

    return False


def log_reloader(filename):

    if filename is not None:
        f = open(filename, 'at', buffering=1)
        os.dup2(f.fileno(), 1)
        os.dup2(f.fileno(), 2)
        f.close()



if __name__ == '__main__':

    parser = optparse.OptionParser()
    parser.add_option('-c', '--config', type='str', action='store', dest='configfile')
    parser.add_option('-D', '--detach', action='store_true', dest='detach',
                        default=False)
    parser.add_option('-p', '--pidfile', type='str', action='store', dest='pidfile',
                        default=None)

    opt, args = parser.parse_args(sys.argv[1:])
    if args:
        raise SystemExit('** don\'t use args, only options')

    config = read_config_file(
        opt.configfile, bool_args=('http', ),
        int_args=('min_handlers', 'max_handlers', 'backlog_size')
    )
    config['configfile'] = opt.configfile

    if 'setenv' in config:
        for var in config['setenv'].split('\n'):
            name, sep, value = var.partition('=')
            assert sep == '='
            os.environ[name.strip()] = value.strip()

    # le opzioni a linea di comando le unisco alla configurazione da file
    for c in ('detach', 'pidfile'):
        config[c] = getattr(opt, c)

    # opzioni necessarie
    if not config['listen']:
        raise SystemExit('ERROR: missing listen option')

    # avvio
    threading.currentThread().name = 'main'

    log_reloader(config.get('logfile'))

    min_handlers = config.get('min_handlers', 4)
    max_handlers = config.get('max_handlers', 20)

    # questo va preso prima di cambiare utente
    server_socket = create_listen_socket(config['listen'],
                        backlog=config.get('backlog_size', max_handlers*4))

    if 'user' in config and change_user(config['user']):
        printlog('user changed: ' + config['user'])

    os.chdir(config['basedir'])

    if config['detach']:
        session_detach()

    if config.get('debuglog'):
        debug_file = open(config['debuglog'], 'a', buffering=1)

    printlog('moka %s start! pid' % __version_string__, os.getpid())

    if config['pidfile']:
        printlog('writing pid file %s' % opt.pidfile)
        open(opt.pidfile, 'w').write(str(os.getpid()))

    printlog('server socket: ' + config['listen'])
    server = MokaServer(
        server_socket, opt.configfile,
        min_handlers=min_handlers, max_handlers=max_handlers,
        log_reloader=partial(log_reloader, config.get('logfile'))
    )

    if config.get('webinterface', None):
        addr, _, port = config['webinterface'].partition(':')
        webaddr = (addr, int(port))
        webserver = WebInterfaceHTTPServer(webaddr, WebInterfaceHandler,
                                        mokaserver=server, config=config)
        th = threading.Thread(target=webserver.serve_forever, args=(), name='web-interface')
        th.daemon = True
        th.start()

    server.start()

    printlog('exit.')


