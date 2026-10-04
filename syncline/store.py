"""DuckDB: the checkpoint, the raw log, the current-state table and the schema record.

They live in one database on purpose. A page of rows and the checkpoint that
says "this page is done" are written in one transaction, so there is no moment
where one exists without the other.
"""
import json

import duckdb

SCHEMA = """
create table if not exists _sync_state (
    stream      varchar primary key,
    cursor      varchar,            -- highest updated_at committed; the next run starts here
    run_id      varchar,
    updated     timestamptz default now()
);
create table if not exists _runs (
    run_id varchar primary key, stream varchar, mode varchar,
    started timestamptz, finished timestamptz, status varchar,
    pages int, requests int, rows_fetched int, rows_changed int
);
create table if not exists raw_issues (     -- append-only: every row ever fetched, as fetched
    run_id varchar, fetched_at timestamptz, id bigint, updated_at varchar, payload json
);
create table if not exists issues (         -- current state: one row per issue or PR
    id bigint primary key,
    number integer, title varchar, state varchar, is_pull_request boolean,
    user_login varchar, comments integer,
    created_at timestamptz, updated_at timestamptz, closed_at timestamptz,
    payload json,
    deleted_at timestamptz                  -- set by reconcile when GitHub stops returning the id
);
create table if not exists _schema_fields (
    field varchar primary key, first_seen_run varchar
);
create table if not exists _schema_changes (
    field varchar, change varchar, run_id varchar, seen_at timestamptz default now()
);
"""


def connect(path):
    con = duckdb.connect(str(path))
    con.execute(SCHEMA)
    return con


def get_cursor(con, stream):
    row = con.execute("select cursor from _sync_state where stream = ?", [stream]).fetchone()
    return row[0] if row else None


def set_cursor(con, stream, cursor, run_id):
    con.execute(
        "insert into _sync_state (stream, cursor, run_id) values (?, ?, ?) "
        "on conflict (stream) do update set cursor = excluded.cursor, "
        "run_id = excluded.run_id, updated = now()",
        [stream, cursor, run_id],
    )


def typed(row):
    """The columns worth querying directly. Everything else stays in payload."""
    return (
        row["id"], row.get("number"), row.get("title"), row.get("state"),
        "pull_request" in row, (row.get("user") or {}).get("login"), row.get("comments"),
        row.get("created_at"), row["updated_at"], row.get("closed_at"),
        json.dumps(row, sort_keys=True),
    )


def write_page(con, rows, run_id):
    """Append to the raw log and merge into `issues`. Returns how many issues changed.

    A row only replaces the stored one if it is strictly newer, so fetching the
    same row twice (an overlap window, a retried page, a resumed run) changes
    nothing. That is what makes at-least-once fetching safe.
    """
    if not rows:
        return 0
    con.executemany(
        "insert into raw_issues values (?, now(), ?, ?, ?)",
        [(run_id, r["id"], r["updated_at"], json.dumps(r, sort_keys=True)) for r in rows],
    )
    # The same id can appear twice in one page's worth of rows; keep its newest version.
    latest = {}
    for r in rows:
        if r["id"] not in latest or r["updated_at"] > latest[r["id"]]["updated_at"]:
            latest[r["id"]] = r
    # Count before merging: a row changes the table if its id is new or it is strictly newer.
    changed = con.execute(
        "select count(*) from (select unnest(?) as id, unnest(?) as u) p "
        "left join issues i using (id) "
        "where i.id is null or cast(p.u as timestamptz) > i.updated_at",
        [list(latest), [r["updated_at"] for r in latest.values()]],
    ).fetchone()[0]
    con.executemany(
        """
        insert into issues values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, null)
        on conflict (id) do update set
            number = excluded.number, title = excluded.title, state = excluded.state,
            is_pull_request = excluded.is_pull_request, user_login = excluded.user_login,
            comments = excluded.comments, created_at = excluded.created_at,
            updated_at = excluded.updated_at, closed_at = excluded.closed_at,
            payload = excluded.payload, deleted_at = null
        where excluded.updated_at > issues.updated_at
        """,
        [typed(r) for r in latest.values()],
    )
    return changed


def record_fields(con, rows, run_id):
    """Note any field we have never seen. The load carries on; the change is on record."""
    if not rows:
        return []
    fields = con.execute("select field, first_seen_run from _schema_fields").fetchall()
    known = {f for f, _ in fields}
    # The whole first run is the baseline, not just its first page: some fields only
    # appear on some rows (`pull_request` is only on pull requests).
    first_ever = all(r == run_id for _, r in fields)
    seen = set().union(*(r.keys() for r in rows))
    new = sorted(seen - known)
    for f in new:
        con.execute("insert into _schema_fields values (?, ?)", [f, run_id])
        if not first_ever:
            con.execute("insert into _schema_changes (field, change, run_id) values (?, 'added', ?)",
                        [f, run_id])
    return [] if first_ever else new
