from invoke import Collection, task
from psycopg2.extras import LoggingConnection
from tabulate import tabulate

import logging
import psycopg2
import queries
import time


class DatabaseNotFound(Exception):
    """
    Database not found
    """


def setup_database(c, dbname=None):
    """
    Setup logging, dbname, connections, etc
    """
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

    # are we connected to the right database?
    if queries.assert_database_name(c, dbname):
        c.logger.info(f'dbname={c.repack.dbname}')
    else:
        raise DatabaseNotFound(f'{c.repack.dbname} not found!')

    # install necessary extensions
    with c.config.conn.cursor() as cursor:
        cursor.execute('CREATE EXTENSION IF NOT EXISTS pg_repack;')


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
    result = queries.get_bloated_tables(c)
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
        c.logger.info(f'table={table}')
        c.repack.table = table
    else:
        c.logger.info('Table not specified, auto-selecting table...')
        # pick last (smallest) table from output
        result = queries.get_bloated_tables(c)[-1]
        if result:
            c.logger.info('table={} tbloat={} totalwaste={}'.format(
                result['tablename'], result['tbloat'], result['prettywaste']))
            print('Ctrl-C to abort or repack in 5 seconds...')
            time.sleep(5)
            c.repack.table = result['tablename']
        else:
            c.logger.error('No candidate table found.')
            return None

    # https://learning.oreilly.com/library/view/mastering-postgresql-96/9781783555352/b88418eb-983e-446e-a715-9028b03fa48f.xhtml
    c.logger.info('https://www.postgresql.org/docs/11/pgstattuple.html')
    result = queries.get_dead_tuple_percent(c, c.repack.table)
    print(tabulate(result, headers='keys', tablefmt='psql', floatfmt=".2f"))

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
    c.logger.info(f'done in {elapsed_time:.2f} seconds.')

    result = queries.get_dead_tuple_percent(c, c.repack.table)
    print(tabulate(result, headers='keys', tablefmt='psql', floatfmt=".2f"))


ns = Collection(build, show, repack)
ns.configure({
    'repack': {
        'dbname': None,
        'host': 'host.docker.internal',
        'image': 'peloton/pg_repack:0.1',
        'schema': 'public',
        'tbloat': 1.1,
        'threshold': 1000000000,
    }
})
