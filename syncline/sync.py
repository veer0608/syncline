"""The sync loop: fetch a page, commit it with its checkpoint, move on.

Paging is by cursor, not by page number. Every request asks for rows updated
since the newest one already committed. Page numbers are offsets, and offsets
shift while you read: if row 50 is edited after page 1, it jumps to the end of
the list and the old row 101 slides onto page 1, where it is never read. Asking
"since the newest committed update" again for each page cannot skip a row,
because a row that moves only moves later.

The cursor is NOT the newest `updated_at` on the page. GitHub sorts and filters
on its own internal timestamp, and the `updated_at` it returns usually equals it
but not always: the first live sync of duckdb/duckdb met a row showing 2026-05-26
in a page of November 2024, took it as the cursor, and skipped 7,500 rows. So the
cursor is the *smallest* `updated_at` among the page's last few rows (a tenth of
the page). An ordinary row's value is its sort position, and every row after it
sorts later, so the minimum can only land at or before the true position: a row
showing a later time is outvoted, one showing an earlier time just costs a few
re-reads. The cursor overshoots only if every row in the tail is out of place.
The price is about a tenth of each page read twice, which the merge ignores.

The one case a cursor cannot handle is a full page of rows that all share one
timestamp: asking "since that timestamp" returns the same page forever. Only
then does the loop follow GitHub's next-page link until the timestamp moves on.
"""
import uuid
from datetime import datetime, timedelta, timezone

from . import store
from .retry import Clock, HTTPFailure, call_with_retry

# Each run starts this far before the saved cursor. `since` has one-second
# resolution and an edit can land while a run is in flight; re-reading a few
# minutes costs a request or two and the merge ignores rows it already has.
LOOKBACK = timedelta(minutes=5)


def _parse(ts):
    return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def _iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _new_run_id():
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]


def _run_started(con, run_id, stream, mode):
    # A run killed outright (power cut, kill -9) never reaches its `finally`, so it is
    # still marked running. Only one run per stream can be live; anything older is dead.
    con.execute(
        "update _runs set status = 'abandoned' where stream = ? and status = 'running'", [stream]
    )
    con.execute(
        "insert into _runs (run_id, stream, mode, started, status) values (?, ?, ?, now(), 'running')",
        [run_id, stream, mode],
    )


def _run_finished(con, run_id, status, stats):
    con.execute(
        "update _runs set finished = now(), status = ?, pages = ?, requests = ?, "
        "rows_fetched = ?, rows_changed = ? where run_id = ?",
        [status, stats["pages"], stats["requests"], stats["rows_fetched"], stats["rows_changed"], run_id],
    )


def sync(con, source, *, full_refresh=False, clock=None, log=print, run_id=None, max_tries=6, tail=None):
    """Bring `issues` up to date. Safe to interrupt at any point and run again."""
    stream = f"{source.repo}/issues"
    clock = clock or Clock()
    run_id = run_id or _new_run_id()
    tail = tail or max(1, source.per_page // 10)
    if full_refresh:
        con.execute("delete from issues")
        con.execute("delete from _sync_state where stream = ?", [stream])

    high = store.get_cursor(con, stream)
    since = _iso(_parse(high) - LOOKBACK) if high else None
    _run_started(con, run_id, stream, "full_refresh" if full_refresh else ("incremental" if high else "initial"))
    log(f"run {run_id}: {stream} since {since or 'the beginning'}")

    stats = {"pages": 0, "requests": 0, "rows_fetched": 0, "rows_changed": 0}
    requests_before = source.requests
    url = source.url_since(since)
    try:
        while url:
            resp = call_with_retry(lambda: source.get(url), clock=clock, max_tries=max_tries, log=log)
            page = source.parse(resp)
            last = [r["updated_at"] for r in page.rows[-tail:]]
            position = min(last) if last else None  # see the module docstring: not max()

            con.begin()
            try:
                changed = store.write_page(con, page.rows, run_id)
                added = store.record_fields(con, page.rows, run_id)
                if position and (high is None or position > high):
                    high = position
                if high:
                    store.set_cursor(con, stream, high, run_id)
                con.commit()
            except BaseException:
                con.rollback()
                raise

            stats["pages"] += 1
            stats["rows_fetched"] += len(page.rows)
            stats["rows_changed"] += changed
            log(f"  page {stats['pages']}: {len(page.rows)} rows, {changed} changed, cursor {high}"
                + (f", new fields {added}" if added else ""))

            if not page.full:
                url = None
            elif position and (since is None or position > since):
                since = position
                url = source.url_since(since)
            else:
                url = page.next_url
        status = "ok"
    except KeyboardInterrupt:
        status = "interrupted"
        raise
    except BaseException:
        status = "failed"
        raise
    finally:
        stats["requests"] = source.requests - requests_before
        _run_finished(con, run_id, status, stats)
        log(f"run {run_id} {status}: {stats}")
    return stats


def reconcile(con, source, *, clock=None, log=print):
    """Mark issues GitHub no longer returns as deleted. The issues API never reports deletions.

    Lists every id (ordered by creation, which edits cannot reshuffle), then checks
    each missing one individually before marking it: a listing can still skip a row
    if something is deleted mid-scan, and a false "deleted" is worse than a late one.
    """
    clock = clock or Clock()
    seen, url = set(), source.url_all()
    while url:
        resp = call_with_retry(lambda: source.get(url), clock=clock, log=log)
        page = source.parse(resp)
        seen.update(r["id"] for r in page.rows)
        url = page.next_url
    stored = con.execute("select id, number from issues where deleted_at is null").fetchall()
    if stored and not seen:
        raise RuntimeError("listing returned nothing; refusing to mark every issue deleted")

    confirmed = []
    for id_, number in stored:
        if id_ in seen:
            continue
        try:
            call_with_retry(lambda: source.get(source.url_issue(number)), clock=clock, log=log)
        except HTTPFailure as e:
            if e.resp.status in (404, 410):
                confirmed.append(id_)
                continue
            raise
        # Still there (or transferred, which answers with a redirect): not deleted.
    if confirmed:
        con.execute(
            "update issues set deleted_at = now() where id in (select unnest(?))", [confirmed]
        )
    log(f"reconcile: {len(seen)} listed, {len(stored)} stored, {len(confirmed)} marked deleted")
    return confirmed
