#! /usr/bin/env/python3
# -*- coding: utf-8 -*-

__version__ = (0, 22)

import sys, time, datetime, threading, queue, socket

if sys.version_info[0] < 3:
    import cPickle as pickle
    import Queue as queue
else:
    import pickle, queue



'''
0.22    : - generato un FileNotFoundError in connessione per sqlite
0.20    : - aggiunto pyodbc
0.18    : - aggiunto firebirdsql, in sola lettura
          - aggiunto PARSE_DECLTYPES per sqlite3
0.17.1  : - timeout a 60 secondi per Sqlite3Connection
0.17    : - Sqlite3Connection come contextmanager
0.16.1  : - aggiunta la gestione di SELECT ... FOR UPDATE per sqlite3 facendo
          - un BEGIN IMMEDIATE TRANSACTION
0.14    : - aggiunto OperationalError per sqlite3 e dbproxy (per intercettare create table
            di tabelle esistenti). Al momento non so cosa va per postgres
0.11    : - modifiche per compatibilita' con python2
0.10    : - creazione degli indici in create_table(), corretti alcuni bug
0.9     : - aggiunto metodo insert(), che per praticità costruisce da solo l'sql.
          - aggiunto metodo now_dtstr()

'''
# questi moduli li carico quando servono, e quando succede
# li rendo globali
postgres = None
sqlite3 = None
firebirdsql = None
pyodbc = None


def connect(dbstring, **kwargs):

    if dbstring.startswith('postgres:'):
        return use_postgresql(dbstring)

    elif dbstring.startswith('sqlite3:'):
        return use_sqlite3(dbstring[8:], **kwargs)

    elif dbstring.startswith('proxy:'):
        return db_proxy_server_connection(dbstring[6:])

    elif dbstring.startswith('firebirdsql:'):
        return use_firebirdsql(dbstring[12:], **kwargs)

    elif dbstring.startswith('odbc:'):
        return use_pyodbc(dbstring.partition(':')[2])

    else:
        raise AssertionError(dbstring)


def use_postgresql(dbstring):
    global postgres

    assert dbstring.startswith('postgres:') or dbstring.startswith('psql:')

    if postgres is None:

        try:
            ## python-postgres
            import postgresql.driver.dbapi20 as postgres
            import postgresql.exceptions as pg_exceptions

            # aggiungo alla classe esposta all'applicazione
            # gli errori di postgres
            PostgresConnection.Error = pg_exceptions.Error
            PostgresConnection.IntegrityError = pg_exceptions.UniqueError
        except ImportError:
            ## psycopg2
            import psycopg2 as postgres
            PostgresConnection.Error = postgres.Error
            PostgresConnection.IntegrityError = postgres.IntegrityError

    _, _, connstring = dbstring.partition(':')
    conn_params = PostgresConnection.read_connstring(connstring)

    db = PostgresConnection(**conn_params)
    db.dbstring = dbstring

    return db


def use_sqlite3(dbstring, **kwargs):

    global sqlite3

    if sqlite3 is None:
        import sqlite3
        Sqlite3Connection.Error = sqlite3.Error
        Sqlite3Connection.OperationalError = sqlite3.OperationalError
        Sqlite3Connection.IntegrityError = sqlite3.IntegrityError

    return Sqlite3Connection(dbstring, **kwargs)


def use_firebirdsql(dbstring, **kwargs):

    global firebirdsql

    if firebirdsql is None:
        import firebirdsql
        FirebirdsqlConnection.Error = firebirdsql.Error
        FirebirdsqlConnection.OperationalError = firebirdsql.OperationalError
        FirebirdsqlConnection.IntegrityError = firebirdsql.IntegrityError

    conn_params = FirebirdsqlConnection.read_connstring(dbstring)
    db = FirebirdsqlConnection(**conn_params)
    db.dbstring = dbstring

    return db


def db_proxy_server_connection(dbstring):
    return DBProxyServerConnection(dbstring)


def use_pyodbc(dsn):

    global pyodbc

    if pyodbc is None:
        import pyodbc
        PyodbcConnection.Error = pyodbc.Error
        PyodbcConnection.OperationalError = pyodbc.OperationalError
        PyodbcConnection.IntegrityError = pyodbc.IntegrityError

    return PyodbcConnection(dsn)

#----------------------------------------------------------------------
class Base:

    @staticmethod
    def dtstr_to_datetime(dtstr):
        return datetime.datetime.strptime(dtstr, '%Y%m%d%H%M%S')

    @staticmethod
    def datetime_to_dtstr(d):
        return d.strftime('%Y%m%d%H%M%S')

    @staticmethod
    def now_dtstr():
        return time.strftime('%Y%m%d%H%M%S')

    @staticmethod
    def dtstr_to_date(dtstr):
        return datetime.date(int(dtstr[0:4]), int(dtstr[4:6]), int(dtstr[6:8]))

    @staticmethod
    def date_to_dstr(d):
        return d.strftime('%Y%m%d')

    @staticmethod
    def dstr_to_date(dstr):
        return datetime.date(int(dstr[0:4]), int(dstr[4:6]), int(dstr[6:8]))



class PostgresConnection(Base):

    paramstyle = 'format'    # http://www.python.org/dev/peps/pep-0249/

    class UnsupportedError(Exception): pass

    def __init__(self, user, password, host, database, port=5432):

        self.db = postgres.connect(user=user, host=host, password=password,
                    database=database, port=port)
        #~ db = postgres.connect(user='x', database='y', host=None, port=None, unix='/var/run/postgresql/.s.PGSQL.5432')

    def __del__(self):
        self.close()

    def execute(self, sql, bind=()):

        cur = self.db.cursor()
        cur.execute(sql, bind)
        return cur


    def execute_dict(self, *args, **kw):
        return CursorWrapperDict(self.execute(*args, **kw))


    def cursor(self):
        return self.db.cursor()


    def close(self):

        if self.db:
            self.db.close()
            self.db = None


    def savepoint(self, name):
        self.db.cursor().execute('savepoint "{}"'.format(name))


    def commit(self):
        self.db.commit()


    def rollback(self):
        self.db.rollback()

    def rollback_savepoint(self, name):
        self.db.cursor().execute('rollback to savepoint "{}"'.format(name))


    def insert(self, table, **data):

        cols, values = zip(*data.items())
        sql = """insert into %s (%s) values (%s)""" % (table, ','.join(cols), ','.join(['%s']*len(values)))
        self.db.cursor().execute(sql, tuple(values))


    def drop_table(self, name, if_exists=False):

        self.db.cursor().execute("""drop table {} {}""".format('if exists' if if_exists else '', name))
        self.db.commit()   # a quanto pare con postgres si committano anche i drop table


    def create_table(self, name, columns, primary_key=None, indexes=None,
                        if_not_exists=False):

        global COLUMN_TYPE_ACCEPTED

        if not all(c[1].lower() in COLUMN_TYPE_ACCEPTED for c in columns):
            return self._build_error(
                sqlite3.OperationalError('column type not in ' + ', '.join(self.COLUMN_TYPE_ACCEPTED))
            )

        if if_not_exists:
            cur = self.db.cursor()
            try:
                cur.execute("""select 1 from {} where 1 = 2""".format(name))
                return      # nessun errore, tabella esistente
            except self.db.Error as err:
                pass

        # cambio di tipi da quelli generici a quelli di postgres
        type_map = {
            'text': 'text', 'integer': 'integer', 'float': 'float', 'binary': 'bytea',
            'bigint': 'bigint'
        }
        new_col_def = []
        for c in columns:
            c = list(c)
            c[1] = type_map[c[1]]
            new_col_def.append(c)

        sql_commands = build_sql_create_table(
            name=name, columns=new_col_def, primary_key=primary_key,
            indexes=indexes
        )


        for sql in sql_commands:
            try:
                cur = self.db.cursor()
                cur.execute(sql)
                self.db.commit()    # a quanto pare con postgres va committato il create table
            except self.db.Error as err:
                return self._build_error(err)


    @staticmethod
    def read_connstring(connstring):

        out = {}
        user_pwd, sep, connstring = connstring.partition('@')
        if sep != '@':
            raise ValueError('wrong connstring ' + init_string)

        out['user'], sep, out['password'] = user_pwd.partition('/')
        if sep != '/':
            raise ValueError('wrong connstring ' + init_string)

        out['database'], sep, host = connstring.partition('/')
        if ':' in host:
            out['host'], _, port = host.partition(':')
            out['port'] = int(port)
        else:
            out['host'] = host

        return out


class Sqlite3Connection(Base):

    paramstyle = 'format'    # http://www.python.org/dev/peps/pep-0249/

    class UnsupportedError(Exception): pass


    def __init__(self, database, **kwargs):

        database, _, params = database.partition('::')
        if params:

            params = params.split(',')

            if 'PARSE_DECLTYPES' in params:
                kwargs['detect_types'] = sqlite3.PARSE_DECLTYPES

        else:
            params = []

        kwargs.setdefault('timeout', 60)
        try:
            self.db = sqlite3.connect(database, **kwargs)
        except sqlite3.OperationalError as err:
            raise FileNotFoundError('{} / file:{}'.format(err, database))

        if 'ENABLE_DATETIME' in params:
            # ATTENZIONE: QUESTA IMPOSTAZIONE VALE A LIVELLO DI MODULO
            # E PER ESSERE USATA DEVE ESSERE ATTIVATO ANCHE PARSE_DECLTYPES
            sqlite3.register_converter(
                'DATETIME', lambda v: datetime.datetime.strptime(v.decode('ascii'), '%Y%m%d%H%M%S')
            )
            sqlite3.register_adapter(
                'DATETIME', lambda v: v.strftime('%Y%m%d%H%M%S')
            )

        # disabilito l'autocommit e me lo gestisco a mano. Senza di questo
        # non funzionano i savepoint (non ho approfondito sul perche')
        self.db.isolation_level = None

        # attributo presente direttamente sulla connessione a sqlite3, ma
        # solo a partire da python 3.2. lo gestisco a mano per compatibilità
        # con python 2
        self.in_transaction = False


    def __del__(self):
        self.close()


    def execute(self, sql, bind=()):

        sql = self._check_transaction(sql)
        return self.db.execute(self._change_placeholders(sql), bind)


    def execute_dict(self, *args, **kw):
        return CursorWrapperDict(self.execute(*args, **kw))


    def cursor(self):
        return NotImplementedError('DA FARE: gestione della transazione automatica per i cursori espliciti')


    def close(self):

        if self.db:
            self.db.close()
            self.db = None


    def savepoint(self, name):

        if not self.in_transaction:
            self.db.execute('begin')
            self.in_transaction = True

        self.db.execute('savepoint {}'.format(name))


    def commit(self):

        self.db.commit()
        self.in_transaction = False

    def rollback(self):

        self.db.rollback()
        self.in_transaction = False


    def rollback_savepoint(self, name):
        self.db.execute('rollback to savepoint {}'.format(name))


    def insert(self, table, **data):

        cols, values = zip(*data.items())
        sql = """insert into %s (%s) values (%s)""" % (table, ','.join(cols), ','.join(['?']*len(values)))
        self.execute(sql, tuple(values))


    def drop_table(self, name, if_exists=False):

        self.db.execute("""drop table {} {}""".format('if exists' if if_exists else '',
                            name))
        self.in_transaction = False  # non sono sicuro che sqlite annulli la transazione


    def create_table(self, name, columns, primary_key=None, indexes=None,
                        if_not_exists=False):

        if not all(c[1].lower() in COLUMN_TYPE_ACCEPTED for c in columns):
            return self._build_error(
                sqlite3.OperationalError('column type not in ' + ', '.join(COLUMN_TYPE_ACCEPTED))
            )

        if if_not_exists:
            cur = self.db.cursor()
            try:
                cur.execute("""select 1 from {} where 1 = 2""".format(name))
                return      # nessun errore, tabella esistente
            except self.db.Error as err:
                pass

        sql_commands = build_sql_create_table(
            name=name, columns=columns, primary_key=primary_key,
            indexes=indexes
        )

        for sql in sql_commands:
            self.db.execute(sql)

        self.in_transaction = False


    def _check_transaction(self, sql):
        # apro una transazione se la query non e' una select


        if not self.in_transaction:
            if sql.lstrip()[:7].lower() != 'select ':
                self.db.execute('begin')
                self.in_transaction = True
                return sql
            elif sql.rstrip()[-10:].lower() == 'for update':
                self.db.execute('begin immediate transaction')
                self.in_transaction = True
                return sql.rstrip()[:-10]
            else:
                return sql
        else:
            if sql.rstrip()[-10:].lower() == 'for update':
                raise AssertionError('already in transaction')

            return sql


    def schema(self):

        cur = self.db.execute("""
            select type, name, tbl_name, sql from sqlite_master
            order by name
        """)

        col_names = [r[0] for r in cur.description]
        return [dict(zip(col_names, row)) for row in cur]


    @staticmethod
    def _change_placeholders(sql):
        return sql.replace('%s', '?')


class FirebirdsqlConnection:

    paramstyle = 'format'    # http://www.python.org/dev/peps/pep-0249/

    class UnsupportedError(Exception): pass


    def __init__(self, dsn, user, password):
        self.db = firebirdsql.connect(dsn=dsn, user=user, password=password)
        self.db.charset = 'latin-1'


    def __del__(self):
        self.close()

    def execute(self, sql, bind=()):

        assert sql.lstrip().upper().startswith('SELECT ')
        return self.db.cursor().execute(self._change_placeholders(sql), bind)


    def execute_dict(self, *args, **kw):
        return CursorWrapperDict(self.execute(*args, **kw))


    def close(self):

        if self.db:
            self.db.close()
            self.db = None


    def savepoint(self, name):
        raise NotImplementedError

    def commit(self):
        self.db.commit()

    def rollback(self):
        self.db.rollback()

    def rollback_savepoint(self, name):
        raise NotImplementedError


    def insert(self, table, **data):

        cols, values = zip(*data.items())
        sql = """insert into %s (%s) values (%s)""" % (table, ','.join(cols), ','.join(['?']*len(values)))
        self.execute(sql, tuple(values))


    def drop_table(self, name, if_exists=False):
        raise NotImplementedError

        self.db.execute("""drop table {} {}""".format('if exists' if if_exists else '',
                            name))
        self.in_transaction = False  # non sono sicuro che sqlite annulli la transazione


    def create_table(self, name, columns, primary_key=None, indexes=None,
                        if_not_exists=False):
        raise NotImplementedError


    def schema(self):
        raise NotImplementedError


    @staticmethod
    def _change_placeholders(sql):
        return sql.replace('%s', '?')

    @staticmethod
    def read_connstring(connstring):

        out = {}
        user_pwd, sep, connstring = connstring.partition('@')
        if sep != '@':
            raise ValueError('wrong connstring ' + init_string)

        out['user'], sep, out['password'] = user_pwd.partition('/')
        if sep != '/':
            raise ValueError('wrong connstring ' + init_string)

        host, sep, database = connstring.partition('/')
        if sep != '/':
            raise ValueError('wrong connstring ' + init_string)
        out['dsn'] = host + ':' + database

        return out



class PyodbcConnection(Base):
    # https://www.connectionstrings.com

    paramstyle = 'format'    # http://www.python.org/dev/peps/pep-0249/

    def __init__(self, dsn, **kwargs):

        self.db = pyodbc.connect(dsn, timeout=10)       # timeout per il login
        self.db_brand = self.guess_brand(dsn)

        kwargs.setdefault('timeout', 60)
        self.db.timeout = kwargs['timeout']     # timeout per le query.


    def __del__(self):
        self.close()

    def guess_brand(self, dsn):
        return None

    def execute(self, sql, bind=()):
        return self.db.execute(self._change_placeholders(sql), bind)

    def execute_dict(self, *args, **kw):
        return CursorWrapperDict(self.execute(*args, **kw))

    def cursor(self):
        return NotImplementedError

    def close(self):

        if self.db:
            self.db.close()
            self.db = None


    def savepoint(self, name):
        self.db.execute('savepoint {}'.format(name))

    def commit(self):
        self.db.commit()

    def rollback(self):
        self.db.rollback()

    def rollback_savepoint(self, name):
        self.db.execute('rollback to savepoint {}'.format(name))


    def insert(self, table, **data):

        cols, values = zip(*data.items())
        sql = """insert into %s (%s) values (%s)""" % (table, ','.join(cols), ','.join(['?']*len(values)))
        self.execute(sql, tuple(values))


    def drop_table(self, name, if_exists=False):

        self.db.execute("""drop table {} {}""".format('if exists' if if_exists else '',
                            name))


    def create_table(self, name, columns, primary_key=None, indexes=None,
                        if_not_exists=False):

        # non l'ho mai provata su SQLServer. C'è da guardare i tipi di dato

        if not all(c[1].lower() in COLUMN_TYPE_ACCEPTED for c in columns):
            return self._build_error(
                sqlite3.OperationalError('column type not in ' + ', '.join(COLUMN_TYPE_ACCEPTED))
            )

        if if_not_exists:
            cur = self.db.cursor()
            try:
                cur.execute("""select 1 from {} where 1 = 2""".format(name))
                return      # nessun errore, tabella esistente
            except self.db.Error as err:
                pass

        sql_commands = build_sql_create_table(
            name=name, columns=columns, primary_key=primary_key,
            indexes=indexes
        )

        for sql in sql_commands:
            self.db.execute(sql)

        self.in_transaction = False


    def schema(self):

        cur = self.db.cursor().tables()
        return [{'name': r[2], 'type':r[3]} for r in cur]


    @staticmethod
    def _change_placeholders(sql):
        return sql.replace('%s', '?')



class DBProxyServerConnection:

    class UnsupportedError(Exception): pass    # funzionalità non disponibili

    class Error(Exception): pass
    class OperationalError(Error): pass
    class Warning(Exception): pass
    class IntegrityError(Error): pass

    DEFAULT_SERVER_ADDR = '127.0.0.1:1555'


    def __init__(self, connstring):

        server, sep, dbstring = connstring.partition(',')
        if not sep:
            dbstring = server
            server = '127.0.0.1'

        self.server = server
        self.dbstring = dbstring
        self._connect_to_server(server, dbstring)


    def _send(self, d, flush=True):

        pickle.dump(d, self.wfile, protocol=self.pickle_protocol)
        if flush:
            self.wfile.flush()


    def _recv(self):

        try:
            out = pickle.load(self.rfile)
            return out
            #~ return pickle.load(self.rfile, errors='replace')
        except EOFError:
            self.rfile.close()
            self.wfile.close()
            self.rfile = self.wfile = None
            raise EOFError('connection terminated')


    def _connect_to_server(self, server, dbstring):

        if ':' in server:
            addr, port = server.split(':')
            server_addr = (addr, int(port))
        else:
            server_addr = (server, int(self.DEFAULT_SERVER_ADDR.split(':')[1]))

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(server_addr)

        self.rfile = sock.makefile('rb')
        self.wfile = sock.makefile('wb')

        sock.close(); del sock

        self.wfile.write(dbstring.encode('ascii') + b'\n')
        self.wfile.flush()

        # attesa della conferma
        rs = self.rfile.readline(1024)
        values = rs.decode('ascii').strip().split(',')
        if values[0] != 'OK':
            raise self.Error('connection rejected: ' + repr(rs))

        self.pickle_protocol = 2 if 'P2' in values else 3
        self.cursors_to_delete = []


    def reconnect(self):

        self.wfile.close()
        self.rfile.close()
        self._connect_to_server(self.server, self.dbstring)


    def _com(self, com, **data):

        if self.cursors_to_delete:
            data['_del_cursors'] = self.cursors_to_delete
            self.cursors_to_delete = []

        data['com'] = com
        self._send(data)
        rs = self._recv()

        # la risposta è una tra None (per quei comandi che non prevedono dati
        # di ritorno) oppure un dizionario con _necessariamente_ la chiave 'rs'

        if rs and rs['rs'] == 'err':
            raise self._build_exception(rs)

        return rs


    def _fetch_cursor(self, cursor_id, count):

        rs = self._com('fetchmany', id=cursor_id, count=count)

        assert rs['rs'] == 'rows'
        return rs['d']


    def _del_cursor(self, cursor_id):

        rs = self._com('del_cursor', id=cursor_id)
        assert rs is None


    def _defer_del_cursor(self, cursor_id):
        self.cursors_to_delete.append(cursor_id)


    def _build_exception(self, r):

        if 'type' in r:
            err = {
                'Error': self.Error,
                'OperationalError': self.OperationalError,
                'Warning': self.Warning,
                'IntegrityError': self.IntegrityError,
                'UnsupportedError': self.UnsupportedError
            }[r['type']]
        else:
            err = self.Error     # errore non-sqlite

        return err(r['text'])


    def schema(self):

        r = self._com('schema')
        assert r['rs'] == 'schema'
        return r['obj_list']     # controlla che esca come PyodbcConnection.schema


    def execute(self, sql, params=None):

        rs = self._com('exe', sql=sql, p=params)
        assert rs['rs'] == 'cursor'
        return DBProxyServerCursor(self, rs['id'], description=rs['descr'], rowcount=rs['rowcount'])


    def execute_dict(self, *args, **kw):
        return CursorWrapperDict(self.execute(*args, **kw))


    def executemany(self, sql, data_sequence):

        self._send({'com': 'exemany', 'sql': sql}, flush=False)

        data_block = []
        for data in data_sequence:
            data_block.append(data)
            if len(b) == 16:
                self._send(data_block, flush=False)
                data_block = []

        if data_block:
            self._send(data_block, flush=False)

        self._send(None)   # segnala il termine dei dati e flush
        rs = self._recv()

        assert isinstance(r, dict) and 'rs' in r

        if r['rs'] == 'cursor':
            return Cursor(self, rs['id'], description=rs['descr'], rowcount=rs['rowcount'])

        elif r['rs'] == 'err':
            raise self._build_exception(r)

        else:
            raise AssertionError


    def savepoint(self, name):

        r = self._com('savepoint', name=name)
        assert r is None


    def commit(self):

        r = self._com('commit')
        assert r is None


    def rollback(self):

        r = self._com('rollback')
        assert r is None


    def rollback_savepoint(self, name):

        r = self._com('rollback_savepoint', name=name)
        assert r is None


    def insert(self, table, **data):

        cols, values = zip(*data.items())
        sql = """insert into %s (%s) values (%s)""" % (table, ','.join(cols), ','.join(['%s']*len(values)))
        self.execute(sql, tuple(values))


    def drop_table(self, name, if_exists=False):

        r = self._com('drop_table', name=name, if_exists=if_exists)
        if r is not None:
            raise self._build_exception(r)


    def create_table(self, name, columns, primary_key=None, indexes=None,
                        if_not_exists=False):

        r = self._com(
            'create_table', name=name, columns=columns, primary_key=primary_key,
            indexes=indexes, if_not_exists=if_not_exists
        )
        if r is not None:
            raise self._build_exception(r)


    def close(self):

        self.rfile.close()
        self.wfile.close()
        del self.rfile, self.wfile
        self.dbstring = None


class DBProxyServerCursor:

    paramstyle = 'format'    # http://www.python.org/dev/peps/pep-0249/
    defer_cursor_delete = True

    def __init__(self, connection, cursor_id, description, rowcount):

        self.conn = connection
        self.id = cursor_id
        self.description = description
        self.rowcount = rowcount
        self.prefetched_rows = []


    def __del__(self):

        if self.defer_cursor_delete:
            self.conn._defer_del_cursor(self.id)
        else:
            try:
                self.conn._del_cursor(self.id)
            except EOFError:
                pass

        del self.conn, self.id


    def __iter__(self):

        while True:

            if self.prefetched_rows:
                yield self.prefetched_rows.pop(0)

            else:
                block = self.conn._fetch_cursor(self.id, 200)
                if not block:
                    break
                self.prefetched_rows = block


    def fetchone(self):

        if self.prefetched_rows:
            return self.prefetched_rows.pop(0)
        else:
            row = self.conn._fetch_cursor(self.id, 1)
            return row[0] if row else None


    def fetchmany(self, count):

        assert count >= 0

        if self.prefetched_rows:
            rows, self.prefetched_rows = (self.prefetched_rows[:count],
                                    self.prefetched_rows[count:])
        else:
            rows = []

        while len(rows) < count:

            block = self.conn._fetch_cursor(self.id, min(count - len(rows), 2048))
            rows.extend(block)
            if not block:
                # righe finite
                break

        return rows


    def fetchall(self):

        rows = self.prefetched_rows
        self.prefetched_rows = []
        while True:
            block = self.conn._fetch_cursor(self.id, 2048)

            if not block:
                break

            rows.extend(block)
            if len(block) < 2048:
                # sono arrivate meno righe di quelle richieste; è
                # chiaro che non ce ne sono più
                break

        return rows


class CursorWrapperDict:

    def __init__(self, cursor):
        self._cursor = cursor


    def __getattr__(self, name):
        return getattr(self._cursor, name)


    def __iter__(self):

        cursor_iter = iter(self._cursor)
        col_names = [c[0].lower() for c in self._cursor.description]

        while True:
            r = next(cursor_iter)
            yield dict(zip(col_names, r))


    def fetchall(self):

        col_names = [c[0].lower() for c in self._cursor.description]
        return [dict(zip(col_names, r)) for r in self._cursor.fetchall()]


    def fetchone(self):

        r = self._cursor.fetchone()
        if r is None:
            return None

        if not hasattr(self, '_col_names'):
            self._col_names = [c[0].lower() for c in self._cursor.description]

        return dict(zip(self._col_names, r))


    def fetchmany(self, n):

        if not hasattr(self, '_col_names'):
            self._col_names = [c[0].lower() for c in self._cursor.description]

        return [dict(zip(self._col_names, r)) for r in self._cursor.fetchall()]


## ----------------------------------------------------------------------

COLUMN_TYPE_ACCEPTED = ('integer', 'bigint', 'text', 'float', 'binary')

def build_sql_create_table(name, columns, primary_key=None, indexes=None):
    '''
    definizione colonna:
    [
        (nome, tipo),
        (nome, tipo, False),
        (nome, tipo, True)      il terzo parametro (opzionale) è il not null
    ]
    '''

    sql_commands = []

    type_map = {
        'integer': 'integer',
        'bigint': 'integer',
        'text': 'text',
        'float': 'float',
        'binary': 'binary'
    }

    # nota: occhio a mettere i "" attorno ai nomi delle colonne con postgres.
    # se lo fai il nome della colonna diventa case-sensitive e richiede anche i "".

    sql = ['create table {} ('.format(name)]
    for col in columns:
        col_name = col[0]; type = col[1]
        if len(col) > 2 and col[2]:
            sql.append('    {} {} not null,'.format(col_name, type_map[type]))
        else:
            sql.append('    {} {},'.format(col_name, type_map[type]))

    if primary_key:
        if isinstance(primary_key, str):
            primary_key=[primary_key]

        sql.append('    primary key ({})'.format(','.join("{}".format(n) for n in primary_key)))
    else:
        sql[-1] = sql[-1][:-1]  # tolgo l'ultima virgola

    sql.append(')')
    sql_commands.append('\n'.join(sql))

    if indexes:
        for i, columns in enumerate(indexes, 1):
            sql_commands.append(
                "create index {0}_I{1} on {0} ({2})".format(name, i, ','.join(columns))
            )

    return sql_commands


## ----------------------------------------------------------------------


if __name__ == '__main__':

    db = connect('sqlite:/tmp/test.sqlite')
    db.create_table(
        'abc', [
            ('a', 'integer'),
            ('b', 'integer'),
            ('c', 'integer'),
        ],
        if_not_exists=True
    )
    db.execute('delete from abc')
    db.commit()

    for n in range(3):
        db.execute('insert into abc (a,b,c) values (%s,%s,%s)', (n,n,n))

    db.savepoint('a')
    for n in range(4, 7):
        db.execute('insert into abc (a,b,c) values (%s,%s,%s)', (n,n,n))

    db.rollback_savepoint('a')
    #~ db.rollback()
    for r in db.execute('select * from abc'):
        print(r)


















