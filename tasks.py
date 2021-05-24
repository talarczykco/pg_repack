from invoke import Collection, task

import queries
import psycopg2
import psycopg2.extras


def init(c, dbname):
    c.config.conn = psycopg2.connect(f'dbname={dbname} host=localhost')
    c.config.conn.set_session(autocommit=True)
    with c.config.conn.cursor() as cur:
        cur.execute('CREATE EXTENSION IF NOT EXISTS pg_repack;')
        cur.execute('CREATE EXTENSION IF NOT EXISTS pgstattuple;')


def get_dead_tuple_percent(c, table):
    with c.config.conn.cursor() as cur:
        cur.execute('SELECT dead_tuple_percent FROM pgstattuple(%s);', [table])
        return cur.fetchone()[0]


@task
def build(c):
    cmd = f'docker build -t {c.repack.image} .'
    c.run(cmd, hide=False)


@task
def test(c, table):
    dbname = c.repack.dbname
    c.config.conn = psycopg2.connect(f'dbname={dbname} host=localhost')
    c.config.conn.set_session(autocommit=True)
    dict_cur = psycopg2.extras.DictCursor
    with c.config.conn.cursor(cursor_factory=dict_cur) as cur:
        # TBD: pass args, fix "IndexError: tuple index out of range"
        cur.execute(queries.show_database_bloat())
        for rec in cur:
            print('{}: {} ({})'.format(
                rec["tablename"],
                rec["tbloat"],
                rec["wastedbytes"]
            ))


@task
def repack(c, table):
    init(c, c.repack.dbname)
    print(f'INFO: dead_tuple_percent={get_dead_tuple_percent(c, table)}')
    cmd = ' '.join([
        'docker run', c.repack.image, c.repack.dbname,
        '-h', c.repack.host,
        '-t', table
    ])
    c.run(cmd)
    print(f'INFO: dead_tuple_percent={get_dead_tuple_percent(c, table)}')


ns = Collection(build, repack, test)
ns.configure({
    'repack': {
        'dbname': 'tfdev1',
        'host': 'host.docker.internal',
        'image': 'peloton/pg_repack:0.1',
        'threshold': 1000000,   # threshold in bytes of wasted space
    }
})
