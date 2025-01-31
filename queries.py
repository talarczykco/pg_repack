from psycopg2.extras import DictCursor


def assert_database_name(c, dbname):
    """
    Return True if `current_database()` is `dbname`
    """
    with c.config.conn.cursor() as cursor:
        cursor.execute("""
            SELECT CASE WHEN current_database() = %s THEN TRUE ELSE FALSE END;
        """, [dbname])
        result = cursor.fetchone()[0]
        return result


def get_dead_tuple_percent(c, table):
    with c.config.conn.cursor(cursor_factory=DictCursor) as cursor:
        cursor.execute("""
            SELECT  pg_size_pretty(table_len) as table_len,
                    tuple_count,
                    pg_size_pretty(tuple_len) as tuple_len,
                    tuple_percent,
                    dead_tuple_count,
                    pg_size_pretty(dead_tuple_len) as dead_tuple_len,
                    dead_tuple_percent,
                    pg_size_pretty(free_space) as free_space,
                    free_percent
            FROM pgstattuple(%s)
        """, [table])
        result = cursor.fetchall()
        return result


def get_bloated_tables(c):
    """
    Notes:
    - "noqa: E501" for flake8 long lines
    - don't forget to double-escape "%" as "%%" when using "%s"!
    """
    with c.config.conn.cursor(cursor_factory=DictCursor) as cursor:
        cursor.execute(''' -- # noqa: E501
        SELECT  schemaname,
                tablename,
                tbloat,
                sum(wastedbytes + wastedibytes)::bigint as totalwaste,
                pg_size_pretty(sum(wastedbytes + wastedibytes)::bigint) as prettywaste
        FROM (
            SELECT
              current_database(), schemaname, tablename, /*reltuples::bigint, relpages::bigint, otta,*/
              ROUND((CASE WHEN otta=0 THEN 0.0 ELSE sml.relpages::float/otta END)::numeric,1) AS tbloat,
              CASE WHEN relpages < otta THEN 0 ELSE bs*(sml.relpages-otta)::BIGINT END AS wastedbytes,
              iname, /*ituples::bigint, ipages::bigint, iotta,*/
              ROUND((CASE WHEN iotta=0 OR ipages=0 THEN 0.0 ELSE ipages::float/iotta END)::numeric,1) AS ibloat,
              CASE WHEN ipages < iotta THEN 0 ELSE bs*(ipages-iotta) END AS wastedibytes
            FROM (
              SELECT
                schemaname, tablename, cc.reltuples, cc.relpages, bs,
                CEIL((cc.reltuples*((datahdr+ma-
                  (CASE WHEN datahdr%%ma=0 THEN ma ELSE datahdr%%ma END))+nullhdr2+4))/(bs-20::float)) AS otta,
                COALESCE(c2.relname,'?') AS iname, COALESCE(c2.reltuples,0) AS ituples, COALESCE(c2.relpages,0) AS ipages,
                COALESCE(CEIL((c2.reltuples*(datahdr-12))/(bs-20::float)),0) AS iotta -- very rough approximation, assumes all cols
              FROM (
                SELECT
                  ma,bs,schemaname,tablename,
                  (datawidth+(hdr+ma-(case when hdr%%ma=0 THEN ma ELSE hdr%%ma END)))::numeric AS datahdr,
                  (maxfracsum*(nullhdr+ma-(case when nullhdr%%ma=0 THEN ma ELSE nullhdr%%ma END))) AS nullhdr2
                FROM (
                  SELECT
                    schemaname, tablename, hdr, ma, bs,
                    SUM((1-null_frac)*avg_width) AS datawidth,
                    MAX(null_frac) AS maxfracsum,
                    hdr+(
                      SELECT 1+count(*)/8
                      FROM pg_stats s2
                      WHERE null_frac<>0 AND s2.schemaname = s.schemaname AND s2.tablename = s.tablename
                    ) AS nullhdr
                  FROM pg_stats s, (
                    SELECT
                      (SELECT current_setting('block_size')::numeric) AS bs,
                      CASE WHEN substring(v,12,3) IN ('8.0','8.1','8.2') THEN 27 ELSE 23 END AS hdr,
                      CASE WHEN v ~ 'mingw32' THEN 8 ELSE 4 END AS ma
                    FROM (SELECT version() AS v) AS foo
                  ) AS constants
                  GROUP BY 1,2,3,4,5
                ) AS foo
              ) AS rs
              JOIN pg_class cc ON cc.relname = rs.tablename
              JOIN pg_namespace nn ON cc.relnamespace = nn.oid AND nn.nspname = rs.schemaname AND nn.nspname <> 'information_schema'
              LEFT JOIN pg_index i ON indrelid = cc.oid
              LEFT JOIN pg_class c2 ON c2.oid = i.indexrelid
            ) AS sml
            WHERE iname != '?'
        ) AS filter
        WHERE schemaname = %s
        AND tbloat > %s
        GROUP BY schemaname, tablename, tbloat
        HAVING sum(wastedbytes + wastedibytes) > %s
        ORDER BY totalwaste DESC;
        ''', [
          c.repack.schema,
          c.repack.tbloat,
          c.repack.threshold,
        ])
        result = cursor.fetchall()
        return result
