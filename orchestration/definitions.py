"""Dagster: the sync as an asset, a daily schedule, a data check and a failure sensor.

    dagster dev -m orchestration.definitions

Retrying a failed run whole is safe here only because `sync` resumes from its
checkpoint: a retry re-reads the lookback window and carries on, it does not
start over or load anything twice.
"""
import os
from datetime import datetime, timezone

import dagster as dg

from syncline import store
from syncline.source import GitHubSource, github_token
from syncline.sync import sync

REPO = os.environ.get("SYNCLINE_REPO", "duckdb/duckdb")
DB = os.environ.get("SYNCLINE_DB", "syncline.duckdb")


@dg.asset(
    description=f"Issues and pull requests of {REPO}, synced incrementally into DuckDB.",
    retry_policy=dg.RetryPolicy(max_retries=2, delay=60),
)
def github_issues(context: dg.AssetExecutionContext):
    con = store.connect(DB)
    try:
        stats = sync(con, GitHubSource(REPO, token=github_token()), log=context.log.info)
        cursor = store.get_cursor(con, f"{REPO}/issues")
        total = con.execute("select count(*) from issues").fetchone()[0]
    finally:
        con.close()
    return dg.MaterializeResult(metadata={**stats, "cursor": cursor, "issues_total": total})


@dg.asset_check(asset=github_issues, description="Every id ever fetched has exactly one current row.")
def no_rows_lost_or_doubled():
    con = store.connect(DB)
    try:
        fetched, current = con.execute(
            "select (select count(distinct id) from raw_issues), (select count(*) from issues)"
        ).fetchone()
    finally:
        con.close()
    return dg.AssetCheckResult(
        passed=fetched == current,
        metadata={"distinct_ids_fetched": fetched, "rows_in_issues": current},
    )


sync_job = dg.define_asset_job("sync_issues", selection=[github_issues])

daily = dg.ScheduleDefinition(
    job=sync_job, cron_schedule="0 6 * * *", execution_timezone="Asia/Kolkata"
)


@dg.run_failure_sensor(monitored_jobs=[sync_job])
def report_failed_sync(context: dg.RunFailureSensorContext):
    """A failure that survived the retries. Logged and appended to failures.log for a human."""
    msg = f"{datetime.now(timezone.utc):%Y-%m-%dT%H:%M:%SZ} run {context.dagster_run.run_id} failed: " \
          f"{context.failure_event.message}"
    context.log.error(msg)
    with open("failures.log", "a", encoding="utf-8") as f:
        f.write(msg + "\n")


defs = dg.Definitions(
    assets=[github_issues],
    asset_checks=[no_rows_lost_or_doubled],
    jobs=[sync_job],
    schedules=[daily],
    sensors=[report_failed_sync],
)
