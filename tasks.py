from invoke import Collection, task
from psycopg2.extras import DictCursor, LoggingConnection
from tabulate import tabulate

import logging
import psycopg2
import queries
import time


class DatabaseNotFound(Exception):
    pass


def setup_database(c, dbname=None):
    # setup basic logging
    logging.basicConfig(level=logging.INFO,
                        format='%(levelname)s: %(message)s')
    c.logger = logging.getLogger(__name__)

    # set database name
    if dbname:
        c.repack.dbname = dbname

    # does this work?
    c.config.conn = psycopg2.connect(
        connection_factory=LoggingConnection,
        dbname=c.repack.dbname,
        host='localhost')
    c.config.conn.initialize(c.logger)
    c.config.conn.set_session(autocommit=True)

    if queries.assert_database_name(c, dbname):
        c.logger.info(f'dbname={c.repack.dbname}')
    else:
        raise DatabaseNotFound(f'{c.repack.dbname} not found!')

    # install necessary extensions
    with c.config.conn.cursor() as cursor:
        cursor.execute('CREATE EXTENSION IF NOT EXISTS pg_repack;')


def get_bloated_tables(c):
    with c.config.conn.cursor(cursor_factory=DictCursor) as cursor:
        cursor.execute(queries.show_database_bloat(), [c.repack.threshold])
        return cursor.fetchall()
    return None


@task
def build(c):
    """
    Build Docker image from Dockerfile.
    """
    cmd = f'docker build -t {c.repack.image} .'
    c.run(cmd, hide=False)


@task
def show(c, dbname=None):
    """
    Show all available tables to repack.
    """
    setup_database(c, dbname)
    result = get_bloated_tables(c)
    print(tabulate(result, headers='keys', tablefmt='psql', floatfmt=".2f"))


@task(help={
    'dbname': 'Name of the database to connect to',
    'table': 'Name of the table to rebuild',
})
def repack(c, dbname=None, table=None):
    """
    Repack the specified table, or auto-select one if not specified.
    """
    setup_database(c, dbname)

    if table:
        c.repack.table = table
    else:
        result = get_bloated_tables(c)[0]
        if result:
            c.logger.info('table={} tbloat={} totalwaste={}'.format(
                result['tablename'], result['tbloat'], result['totalwaste']))
            print('Ctrl-C to abort or repack in 5 seconds...')
            time.sleep(5)
            c.repack.table = result['tablename']
        else:
            c.logger.error('No candidate table found.')
            return None

    cmd = ' '.join([
        'docker run',
        '-e', 'PGOPTIONS="-c idle_in_transaction_session_timeout=0"',
        c.repack.image, c.repack.dbname,
        '-h', c.repack.host,
        '-t', c.repack.table,
        ])
    start_time = time.monotonic()
    c.run(cmd)
    elapsed_time = time.monotonic() - start_time
    c.logger.info(f'Completed in {elapsed_time:.2f} seconds.')


ns = Collection(build, show, repack)
ns.configure({
    'repack': {
        'dbname': 'tfdev1',
        'host': 'host.docker.internal',
        'image': 'peloton/pg_repack:0.1',
        'threshold': 0,     # 1000000,
    }
})
