from invoke import Collection, task

import psycopg2
import psycopg2.extras
import queries
import time


def initdb(c, dbname=None):
    if dbname:
        c.repack.dbname = dbname
    c.config.conn = psycopg2.connect(dbname=c.repack.dbname, host='localhost')
    c.config.conn.set_session(autocommit=True)
    with c.config.conn.cursor() as cursor:
        cursor.execute('CREATE EXTENSION IF NOT EXISTS pg_repack;')


def selectone(c):
    dict_cur = psycopg2.extras.DictCursor
    with c.config.conn.cursor(cursor_factory=dict_cur) as cursor:
        cursor.execute(queries.show_database_bloat())
        return cursor.fetchone()
    return None


@task
def build(c):
    """
    Build the Dockerfile
    """
    cmd = f'docker build -t {c.repack.image} .'
    c.run(cmd, hide=False)


@task(help={
    'dbname': 'Name of the database to connect to',
})
def select(c, dbname=None):
    """
    Select one table to rebuild
    """
    initdb(c, dbname)
    record = selectone(c)
    print('INFO: table={} tbloat={} wastedbytes={}'.format(
        record['tablename'], record['tbloat'], record['wastedbytes']))
    c.repack.table = record['tablename']


@task(help={
    'dbname': 'Name of the database to connect to',
    'table': 'Name of the table to rebuild',
})
def repack(c, dbname=None, table=None):
    """
    Repack the specified table
    """
    initdb(c, dbname)
    if table is None:
        select(c)
        print('Ctrl-C to abort or repack in 5 seconds...')
        time.sleep(5)
    else:
        c.repack.table = table

    cmd = ' '.join([
        'docker run', c.repack.image, c.repack.dbname,
        '-h', c.repack.host,
        '-t', c.repack.table,
    ])
    c.run(cmd)


ns = Collection(build, repack, select)
ns.configure({
    'repack': {
        'dbname': 'tfdev1',
        'host': 'host.docker.internal',
        'image': 'peloton/pg_repack:0.1',
    }
})
