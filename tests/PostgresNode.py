#coding: utf-8

import os
import random
import socket
import subprocess
import pwd
import tempfile
import shutil

# Try to use psycopg2 by default. If psycopg2 isn't available then use pg8000
# which is slower but much more portable because uses only pure-Python code
try:
    import psycopg2 as pglib
except ImportError:
    try:
        import pg8000 as pglib
    except ImportError:
        raise ImportError('You must have psycopg2 or pg8000 modules installed')


registered_nodes = []
last_assigned_port = int(random.random() * 16384) + 49152;

class ClusterException(Exception):
    pass

class ResultSet:
    """Wrapper for Cursor to eliminate differences between psycopg and pg8000"""
    def __init__(self, cursor):
        assert(cursor)
        self.cursor = cursor

    def __enter__(self):
        """Context manager protocol"""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Context manager protocol"""
        self.cursor.close()

    def __iter__(self):
        """Iterator protocol"""
        return self

    def next(self):
        """Iterator protocol"""
        res = self.cursor.fetchone()
        if not res:
            raise StopIteration
        return res

    def fetch(self):
        """Returns next row"""
        return self.cursor.fetchone()

    def fetchall(self):
        return self.cursor.fetchall()

    def close(self):
        self.cursor.close()

    def __unicode__(self):
        return unicode(self.fetchall())

    def __str__(self):
        return str(self.fetchall())


class PostgresNode:
    def __init__(self, name, port):
        self.name = name
        self.port = port
        # self.data_dir = '/tmp/pg_data_%s' % port
        self.data_dir = tempfile.mkdtemp()
        self.working = False
        self.config = {}
        self.load_pg_config()

    def load_pg_config(self):
        """ Loads pg_config output into dict """
        pg_config = os.environ.get('PG_CONFIG') if 'PG_CONFIG' in os.environ else 'pg_config'

        out = subprocess.check_output([pg_config])
        for line in out.split('\n'):
            if not line:
                continue
            key, value = line.split('=', 1)
            self.config[key.strip()] = value.strip()

    def get_bin_path(self, filename):
        """ Returns full path to an executable """
        if not 'BINDIR' in self.config:
            return filename
        else:
            return '%s/%s' % (self.config['BINDIR'], filename)

    def init(self):
        """ Performs initdb """

        # initialize cluster
        initdb = self.get_bin_path('initdb')
        ret = subprocess.call([initdb, self.data_dir])
        if ret:
            raise ClusterException('Cluster initialization failed')

        # add parameters to config file
        config_name = '%s/postgresql.conf' % self.data_dir
        with open(config_name, 'a') as conf:
            conf.write('fsync = off\n')
            conf.write('log_statement = all\n')
            conf.write('port = %s\n' % self.port)

    def append_conf(self, filename, string):
        """Appends line to a config file like 'postgresql.conf' or 'pg_hba.conf'

        A new line is not automatically appended to the string
        """
        config_name = '%s/%s' % (self.data_dir, filename)
        with open(config_name, 'a') as conf:
            conf.write(string)

    def start(self):
        """ Starts cluster """
        pg_ctl = self.get_bin_path('pg_ctl')
        ret = subprocess.call([pg_ctl, '-D', self.data_dir, '-w', 'start'])
        if ret:
            raise ClusterException('Cluster startup failed')

        self.working = True

    def stop(self):
        """ Stops cluster """
        pg_ctl = self.get_bin_path('pg_ctl')
        ret = subprocess.call([pg_ctl, '-D', self.data_dir, '-w', 'stop'])
        if ret:
            raise ClusterException('Cluster stop failed')

        self.working = False

    def cleanup(self):
        """Stops cluster if needed and removes the data directory"""

        # stop server if it still working
        if self.working:
            self.stop()

        # remove data directory
        shutil.rmtree(self.data_dir)

    def psql(self, dbname, query):
        """Executes a query by the psql

        Returns a tuple (code, stdout, stderr) in which:
        * code is a return code of psql (0 if alright)
        * stdout and stderr are strings, representing standard output and
          standard error output
        """
        psql = self.get_bin_path('psql')
        psql_params = [psql, '-XAtq', '-c', query, '-p %s' % self.port, dbname]

        # start psql process
        process = subprocess.Popen(
            psql_params,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)

        # wait untill it finishes and get stdout and stderr
        out, err = process.communicate()
        return process.returncode, out, err

    def safe_psql(self, dbname, query):
        """Executes a query by the psql

        Returns the stdout if succeed. Otherwise throws the ClusterException
        with stderr output
        """
        ret, out, err = self.psql(dbname, query)
        if ret:
            raise ClusterException('psql failed:\n' + err)
        return out

    def execute(self, dbname, query):
        """Executes the query and returns all rows"""
        connection = pglib.connect(
            database=dbname,
            user=get_username(),
            port=self.port,
            host='127.0.0.1')
        cur = connection.cursor()

        cur.execute(query)
        res = cur.fetchall()
        # return ResultSet(cur)

        cur.close()
        connection.close()

        return res


def get_username():
    """ Returns current user name """
    return pwd.getpwuid(os.getuid())[0]

def get_new_node(name):
    global registered_nodes
    global last_assigned_port

    port = last_assigned_port + 1
    # found = False
    # while found:
    #   # Check first that candidate port number is not included in
    #   # the list of already-registered nodes.
    #   found = True
    #   for node in registered_nodes:
    #       if node.port == port:
    #           found = False
    #           break

    #   if found:
    #       socket(socket.SOCK,
    #              socket.PF_INET,
    #              socket.SOCK_STREAM,
    #              socket.getprotobyname('tcp'))

    node = PostgresNode(name, port)
    registered_nodes.append(node)
    last_assigned_port = port

    return node

def clean_all():
    global registered_nodes
    for node in registered_nodes:
        node.cleanup()
    registered_nodes = []

if __name__ == '__main__':
    node = get_new_node('test')
    node.init()
    node.start()
    # try:
    # with node.execute('postgres', 'select generate_series(1, 5), random() * 10') as res:
    #     for row in res:
    #         print row

    print node.execute('postgres', 'select generate_series(1, 5), random() * 10')

    # except:
    #     node.stop()
    # print node.psql('postgres', 'select 1')
    # print node.psql('postgres', 'select generate_series(1,10)')
    node.stop()
    stop_all()
