# syncline

An incremental connector that syncs a GitHub repository's issues and pull
requests into DuckDB. It only fetches what changed since the last run, waits out
rate limits, survives being killed mid-run, notices schema changes, and runs on a
Dagster schedule.

No LLM anywhere. This is the part of data engineering that sits in front of the
warehouse: pulling from someone else's API correctly, every day, without losing
or doubling a row.

## Where it stands

Measured on 2026-10-04 against live GitHub:

| | |
|---|---|
| Full sync of `duckdb/duckdb` | 22,425 rows (14,635 pull requests, 7,790 issues), 252 requests, 25 minutes |
| GitHub's own count for that repo | 22,425, from the search API: an exact match |
| Two independent full syncs | identical, same hash over every `id` and `updated_at` |
| Rerun straight after | 1 request, 0 rows changed |
| Hard kill mid-sync, then resume (`encode/httpx`) | identical to a clean sync: 2,728 rows, same hash |
| Reconcile on `encode/httpx` | 2,728 listed, 2,728 stored, 0 falsely marked deleted |
| Offline tests | 18 pass, no network |

## How it works

```
GitHub /repos/{repo}/issues?since=<cursor>&sort=updated
   ->  retry          backoff on 5xx and dropped connections, wait out 429 and 403 rate limits
   ->  raw_issues     append-only: every row ever fetched, exactly as fetched
   ->  issues         current state, one row per id, merged only when strictly newer
   ->  _sync_state    the cursor, committed in the same transaction as the page
   ->  _schema_changes  fields that appeared after the first run
```

The classes match the usual shape of a connector: `GitHubSource` fetches one
page and nothing else, `retry.call_with_retry` decides what to do with a failure,
`store` owns the checkpoint and the tables, and `sync` is the loop that joins them.

## The promises, and what checks each one

**Re-running changes nothing.** Each run starts five minutes before the saved
cursor and re-reads that window. The merge only replaces a row with a strictly
newer one, so rows already held cost a request and change nothing.
`test_rerun_is_a_noop`

**A crash loses nothing and repeats nothing.** Each page and the checkpoint
that says it is done are written in one DuckDB transaction. Kill the process
anywhere and the next run resumes from the last committed page.
`test_crash_then_resume_equals_clean_run`, and checked live with a hard kill.

**Incremental equals full refresh.** Syncing in many small runs gives exactly
the table one `--full-refresh` gives. `test_incremental_equals_full_refresh`

**No row is skipped when data changes mid-sync.** Paging is by cursor, not by
page number. Page numbers are offsets, and offsets shift while you read: an issue
edited after page 1 jumps to the end of the list and the issue behind it slides
back onto page 1, where it is never read. `test_row_edited_mid_sync_is_not_skipped`

**A row out of sort order cannot make the cursor jump.** See below; this one was
found by the first live run. `test_row_shown_out_of_sort_order_does_not_skip`

**Rate limits are waited out, not fought.** A 429, or a 403 with no quota left,
waits exactly as long as `Retry-After` or `X-RateLimit-Reset` says, then carries
on. A wait longer than an hour stops the run cleanly with the checkpoint intact.
`test_rate_limit_waits_and_resumes`, `test_rate_limit_too_long_gives_up_cleanly`

**Server errors are retried, then fail cleanly.** 5xx responses and dropped
connections back off exponentially with jitter. Other 4xx errors fail at once,
because retrying cannot fix them. `test_server_error_retries_then_fails_cleanly`,
`test_truncated_response_is_retried`

**A new field does not break the load.** The full payload is kept as JSON, so an
unknown field is stored, and recorded in `_schema_changes`. Fields that only
appear on some rows (`pull_request`) are part of the first run's baseline, not
reported as changes. `test_new_field_recorded_not_fatal`

**Nothing is marked deleted on a guess.** The issues API never reports
deletions, so `reconcile` lists every id and checks each missing one with its own
request before marking it. A listing can skip a live row if something is deleted
mid-scan; a late "deleted" is acceptable, a wrong one is not.
`test_reconcile_marks_deleted`, `test_reconcile_does_not_trust_a_shifted_listing`

## What the live runs found

Three things no test had covered, each now fixed and tested:

1. **GitHub's `updated_at` does not always match the order GitHub sorts by.**
   The first version used the newest `updated_at` on each page as the next
   cursor. On `duckdb/duckdb`, one page of November 2024 rows contained a row
   showing May 2026. The cursor jumped 18 months and the sync silently skipped
   7,553 rows: it finished "ok" with 14,872 rows against GitHub's 22,425. A
   month-by-month comparison against the search API found the gap. The cursor is
   now the *smallest* `updated_at` among the last tenth of each page, which an
   out-of-place row cannot drag forward. Across a full sync, 17 rows appeared out
   of order inside their pages.
2. **GitHub cut a response off mid-read.** A 440 KB page arrived truncated
   (`IncompleteRead`), which was not on the retry list, and the run died. The
   checkpoint held, so nothing was lost, but it should have retried.
3. **A hard-killed run stayed marked "running" forever**, because a kill skips
   the `finally` block that records the outcome. The next run now marks it
   `abandoned`.

The lesson from the first one is the reason this README leads with a count
checked against GitHub's own number: a sync that reports success is not evidence
that it is complete.

## Run it

```bash
python -m venv .venv
.venv/Scripts/pip install -r requirements.txt     # .venv/bin/pip on Linux and macOS
.venv/Scripts/python -m syncline sync --repo encode/httpx
.venv/Scripts/python -m syncline status
.venv/Scripts/python -m pytest
```

A token is read from `GITHUB_TOKEN`, or from `gh auth token` if the GitHub CLI is
logged in. Without one, GitHub allows 60 requests an hour, enough for a small repo.

Other commands: `sync --full-refresh` rebuilds from scratch, and `reconcile` marks
deleted issues.

## Scheduling

`orchestration/definitions.py` defines the sync as a Dagster asset, with a
daily 06:00 IST schedule, a data check that every id ever fetched has exactly one
current row, and a sensor that logs runs which failed after their retries.

```bash
set SYNCLINE_REPO=encode/httpx
dagster dev -m orchestration.definitions
```

Dagster retries a failed run whole, which is only safe because the sync resumes
from its checkpoint instead of starting over.

## Known limits

- Comments, reviews and events are not synced; only issues and pull requests.
- Deletions are found only by `reconcile`, which lists every id, so it costs one
  request per hundred issues. Run it weekly, not daily.
- The cursor would still overshoot if every row in a page's last tenth were out
  of order. Across 24,935 fetched rows, 17 were out of order and never more than
  one in a row.
- DuckDB allows one writer, so two syncs cannot share a database file.
