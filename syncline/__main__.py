"""Command line.

    python -m syncline sync --repo duckdb/duckdb
    python -m syncline sync --repo duckdb/duckdb --full-refresh
    python -m syncline reconcile --repo duckdb/duckdb
    python -m syncline status
"""
import argparse

from . import store
from .source import GitHubSource, github_token
from .sync import reconcile, sync

DEFAULT_DB = "syncline.duckdb"
DEFAULT_REPO = "duckdb/duckdb"


def status(con):
    for (stream, cursor, run_id) in con.execute("select stream, cursor, run_id from _sync_state").fetchall():
        print(f"{stream}: cursor {cursor} (run {run_id})")
    total, prs, deleted = con.execute(
        "select count(*), count(*) filter (where is_pull_request), count(deleted_at) from issues"
    ).fetchone()
    print(f"issues table: {total} rows ({total - prs} issues, {prs} pull requests, {deleted} deleted)")
    print("last runs:")
    for row in con.execute(
        "select run_id, mode, status, pages, requests, rows_fetched, rows_changed "
        "from _runs order by started desc limit 5"
    ).fetchall():
        print("  ", row)
    changes = con.execute("select field, change, run_id from _schema_changes order by seen_at").fetchall()
    if changes:
        print("schema changes:", changes)


def main():
    ap = argparse.ArgumentParser(prog="syncline")
    ap.add_argument("command", choices=["sync", "reconcile", "status"])
    ap.add_argument("--repo", default=DEFAULT_REPO)
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--full-refresh", action="store_true")
    a = ap.parse_args()

    con = store.connect(a.db)
    if a.command == "status":
        return status(con)
    source = GitHubSource(a.repo, token=github_token())
    if a.command == "sync":
        sync(con, source, full_refresh=a.full_refresh)
    else:
        reconcile(con, source)


if __name__ == "__main__":
    main()
