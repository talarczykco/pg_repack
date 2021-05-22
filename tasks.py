from invoke import Collection, task
import psycopg2


def database_setup(c, dbname):
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
def repack(c, table):
    database_setup(c, c.repack.database)
    print(f'INFO: dead_tuple_percent={get_dead_tuple_percent(c, table)}')
    cmd = ' '.join([
        'docker run', c.repack.image, c.repack.database,
        '-h', c.repack.host,
        '-t', table
    ])
    c.run(cmd)
    print(f'INFO: dead_tuple_percent={get_dead_tuple_percent(c, table)}')


ns = Collection(build, repack)
ns.configure({
    'repack': {
        'database': 'tfdev1',
        'host': 'host.docker.internal',
        'image': 'peloton/pg_repack:0.1',
    }
})
