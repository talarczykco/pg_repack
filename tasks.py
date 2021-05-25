from invoke import Collection, task

import psycopg2
import psycopg2.extras
import queries
import time


def setup_database(c, dbname=None):
    if dbname:
        c.repack.dbname = dbname
    c.config.conn = psycopg2.connect(dbname=c.repack.dbname, host='localhost')
    c.config.conn.set_session(autocommit=True)
    with c.config.conn.cursor() as cursor:
        cursor.execute('CREATE EXTENSION IF NOT EXISTS pg_repack;')


def find_bloated_table(c):
    dict_cur = psycopg2.extras.DictCursor
    with c.config.conn.cursor(cursor_factory=dict_cur) as cursor:
        cursor.execute(queries.show_database_bloat())
        return cursor.fetchone()
    return None


@task
def build(c):
    """
    Build Docker image from Dockerfile.
    """
    cmd = f'docker build -t {c.repack.image} .'
    c.run(cmd, hide=False)


@task(help={
    'dbname': 'Name of the database to connect to',
    'table': 'Name of the table to rebuild',
})
def repack(c, dbname=None, table=None):
    """
    Repack the specified table, or auto-select one if not specified.
    """
    setup_database(c, dbname)
    print(f'INFO: dbname={c.repack.dbname}')

    if table:
        c.repack.table = table
    else:
        result = find_bloated_table(c)
        print('INFO: table={} tbloat={} wastedbytes={}'.format(
            result['tablename'], result['tbloat'], result['wastedbytes']))
        print('Ctrl-C to abort or repack in 5 seconds...')
        time.sleep(5)
        c.repack.table = result['tablename']

    cmd = ' '.join([
        'docker run', c.repack.image, c.repack.dbname,
        '-h', c.repack.host,
        '-t', c.repack.table,
    ])
    c.run(cmd)


ns = Collection(build, repack)
ns.configure({
    'repack': {
        'dbname': 'tfdev1',
        'host': 'host.docker.internal',
        'image': 'peloton/pg_repack:0.1',
    }
})
