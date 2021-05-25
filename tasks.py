from invoke import Collection, task
from psycopg2.extras import DictCursor, LoggingConnection
from tabulate import tabulate

import logging
import psycopg2
import queries
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def setup_database(c, dbname=None):
    if dbname:
        c.repack.dbname = dbname
    logger.info(f'dbname={c.repack.dbname}')
    c.config.conn = psycopg2.connect(
        connection_factory=LoggingConnection,
        dbname=c.repack.dbname,
        host='localhost')
    c.config.conn.initialize(logger)
    c.config.conn.set_session(autocommit=True)
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
            logger.info('table={} tbloat={} totalwaste={}'.format(
                result['tablename'], result['tbloat'], result['totalwaste']))
            print('Ctrl-C to abort or repack in 5 seconds...')
            time.sleep(5)
            c.repack.table = result['tablename']
        else:
            logger.error('No candidate table found.')
            return None

    cmd = ' '.join([
        'docker run', c.repack.image, c.repack.dbname,
        '-h', c.repack.host,
        '-t', c.repack.table,
        ])
    env = {
        # neither of these seem to work
        # 'PGOPTIONS': '-c idle_in_transaction_session_timeout=100ms'
        'PGOPTIONS': '-c statement_timeout=100ms'
        }
    start_time = time.monotonic()
    c.run(cmd, env=env)
    elapsed_time = time.monotonic() - start_time
    logger.info(f'Completed in {elapsed_time:.2f} seconds.')


ns = Collection(build, show, repack)
ns.configure({
    'repack': {
        'dbname': 'tfdev1',
        'host': 'host.docker.internal',
        'image': 'peloton/pg_repack:0.1',
        'threshold': 0,     # 1000000,
    }
})
