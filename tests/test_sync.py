"""The promises in the README, one test each. No network: every request goes to FakeGitHub."""
import json

import pytest

from syncline import store
from syncline.retry import GaveUp
from syncline.source import GitHubSource, Response
from syncline.sync import reconcile, sync

from .fake_github import Crash, FakeClock, FakeGitHub, issue, ts

REPO = "acme/widgets"
quiet = lambda *a: None  # noqa: E731


def run(con, fake, clock=None, **kw):
    src = GitHubSource(REPO, transport=fake, per_page=3)
    return sync(con, src, clock=clock or FakeClock(), log=quiet, **kw)


def snapshot(con):
    """Everything a downstream user of `issues` could see."""
    return con.execute(
        "select id, updated_at, title, payload, deleted_at from issues order by id"
    ).fetchall()


def ten():
    return [issue(i, ts(600 * i)) for i in range(1, 11)]  # ten minutes apart, wider than LOOKBACK


@pytest.fixture
def con(tmp_path):
    return store.connect(tmp_path / "a.duckdb")


@pytest.fixture
def con2(tmp_path):
    return store.connect(tmp_path / "b.duckdb")


def test_initial_sync_loads_everything(con):
    stats = run(con, FakeGitHub(ten()))
    assert len(snapshot(con)) == 10
    assert stats["rows_changed"] == 10
    assert store.get_cursor(con, f"{REPO}/issues") == ts(6000)


def test_rerun_is_a_noop(con):
    fake = FakeGitHub(ten())
    run(con, fake)
    before = snapshot(con)
    stats = run(con, fake)
    assert snapshot(con) == before
    assert stats["rows_changed"] == 0
    assert stats["requests"] == 1, "a rerun should re-read only the lookback window"


def test_edit_is_picked_up(con):
    fake = FakeGitHub(ten())
    run(con, fake)
    fake.edit(4, ts(7000), title="renamed")
    stats = run(con, fake)
    assert stats["rows_changed"] == 1
    assert con.execute("select title from issues where id = 4").fetchone()[0] == "renamed"


def test_incremental_equals_full_refresh(con, con2):
    fake = FakeGitHub(ten())
    run(con, fake)
    fake.edit(2, ts(7000), state="closed")
    fake.issues[11] = issue(11, ts(7001))
    fake.edit(7, ts(7002), title="changed")
    run(con, fake)
    run(con2, fake, full_refresh=True)
    assert snapshot(con) == snapshot(con2)


def test_crash_then_resume_equals_clean_run(con, con2):
    fake = FakeGitHub(ten())
    fake.crash_at = 3  # pages 1 and 2 commit, the third request dies
    with pytest.raises(Crash):
        run(con, fake)
    assert store.get_cursor(con, f"{REPO}/issues") is not None, "committed pages keep their checkpoint"
    assert con.execute("select status from _runs").fetchone()[0] == "failed"
    fake.crash_at = None
    run(con, fake)
    run(con2, FakeGitHub(ten()))
    assert snapshot(con) == snapshot(con2)


def test_row_edited_mid_sync_is_not_skipped(con):
    """The offset trap: with page-number paging, this edit would hide one row."""
    fake = FakeGitHub(ten())

    def bump_first_row(n, f):
        if n == 2:  # after page 1 was read, issue 1 is edited and jumps to the end
            f.edit(1, ts(9999), title="edited mid-run")

    fake.before_call = bump_first_row
    run(con, fake)
    assert [r[0] for r in snapshot(con)] == list(range(1, 11))
    assert con.execute("select title from issues where id = 1").fetchone()[0] == "edited mid-run"


def test_row_shown_out_of_sort_order_does_not_skip(con):
    """Found live: GitHub's updated_at can disagree with the order it sorts by.

    Issue 2 sorts as if updated at ts(1200) but shows ts(86000). Taking the page's
    max as the cursor jumps past issues 3 to 10; the tail minimum does not.
    """
    rows = ten()
    rows[1] = issue(2, ts(86000), _sort=ts(1200))
    src = GitHubSource(REPO, transport=FakeGitHub(rows), per_page=3)
    sync(con, src, clock=FakeClock(), log=quiet, tail=2)
    assert len(snapshot(con)) == 10


def test_full_page_with_one_timestamp_terminates(con):
    fake = FakeGitHub([issue(i, ts(50)) for i in range(1, 8)])  # 7 rows, one timestamp, 3 per page
    run(con, fake)
    assert len(snapshot(con)) == 7


def test_rate_limit_waits_and_resumes(con):
    fake = FakeGitHub(ten())
    clock = FakeClock()
    fake.faults[2] = Response(429, {"retry-after": "30"}, b"")
    fake.faults[3] = Response(403, {"x-ratelimit-remaining": "0",
                                    "x-ratelimit-reset": str(int(clock.t) + 30 + 120)}, b"")
    run(con, fake, clock=clock)
    assert clock.sleeps == [30.0, 121.0]
    assert len(snapshot(con)) == 10


def test_rate_limit_too_long_gives_up_cleanly(con):
    fake = FakeGitHub(ten())
    fake.faults[2] = Response(429, {"retry-after": "99999"}, b"")
    with pytest.raises(GaveUp):
        run(con, fake)
    assert store.get_cursor(con, f"{REPO}/issues") == ts(1800), "page 1 stays committed"


def test_server_error_retries_then_fails_cleanly(con):
    fake = FakeGitHub(ten())
    fake.faults[2] = Response(502, {}, b"bad gateway")
    run(con, fake)
    assert len(snapshot(con)) == 10

    fake2 = FakeGitHub(ten())
    for n in range(2, 20):
        fake2.faults[n] = Response(503, {}, b"")
    con.execute("delete from issues; delete from _sync_state")
    with pytest.raises(GaveUp):
        run(con, fake2, max_tries=3)
    assert store.get_cursor(con, f"{REPO}/issues") == ts(1800)
    assert len(snapshot(con)) == 3


def test_truncated_response_is_retried(con):
    """Found live: GitHub cut a 440 KB page off mid-read and the first real sync died on it."""
    import http.client

    fake = FakeGitHub(ten())
    real = fake.__call__
    cut = {"done": False}

    def flaky(url, headers):
        if fake.calls == 1 and not cut["done"]:
            cut["done"] = True
            raise http.client.IncompleteRead(b"partial", 51240)
        return real(url, headers)

    clock = FakeClock()
    run(con, flaky, clock=clock)
    assert len(snapshot(con)) == 10
    assert len(clock.sleeps) == 1


def test_duplicate_across_pages_lands_once(con):
    fake = FakeGitHub(ten())
    # Ordered by updated_at as GitHub sends it; issue 1 was edited between two reads.
    page_with_dupes = [issue(1, ts(600)), issue(2, ts(1200)), issue(1, ts(1200), title="newer")]
    fake.faults[1] = Response(200, {}, json.dumps(page_with_dupes).encode())
    run(con, fake)
    assert con.execute("select count(*) from issues where id = 1").fetchone()[0] == 1
    assert con.execute("select title from issues where id = 1").fetchone()[0] == "newer"
    assert len(snapshot(con)) == 10


def test_new_field_recorded_not_fatal(con):
    fake = FakeGitHub(ten())
    fake.issues[9]["pull_request"] = {"url": "x"}  # only on some rows: part of the baseline, not a change
    run(con, fake)
    assert con.execute("select count(*) from _schema_changes").fetchone()[0] == 0
    fake.edit(3, ts(7000), sub_issues_summary={"total": 2})
    run(con, fake)
    assert con.execute("select field, change from _schema_changes").fetchall() == [
        ("sub_issues_summary", "added")]
    payload = json.loads(con.execute("select payload from issues where id = 3").fetchone()[0])
    assert payload["sub_issues_summary"] == {"total": 2}


def test_reconcile_marks_deleted(con):
    fake = FakeGitHub(ten())
    run(con, fake)
    del fake.issues[3]
    src = GitHubSource(REPO, transport=fake, per_page=3)
    assert reconcile(con, src, clock=FakeClock(), log=quiet) == [3]
    deleted = con.execute("select id from issues where deleted_at is not null").fetchall()
    assert deleted == [(3,)]


def test_reconcile_does_not_trust_a_shifted_listing(con):
    """A deletion mid-listing shifts offsets and hides a live row; the per-issue check saves it."""
    fake = FakeGitHub(ten())
    run(con, fake)
    calls_before = fake.calls

    def delete_during_listing(n, f):
        if n == calls_before + 2:
            del f.issues[1]  # page 2 now starts one row later, so issue 4 is never listed

    fake.before_call = delete_during_listing
    src = GitHubSource(REPO, transport=fake, per_page=3)
    # Issue 1 was listed before it vanished, so it is caught next time; issue 4 was skipped
    # by the shift, and the per-issue check finds it alive. Late is fine, wrong is not.
    assert reconcile(con, src, clock=FakeClock(), log=quiet) == []
    assert con.execute("select count(*) from issues where deleted_at is not null").fetchone()[0] == 0


def test_runs_are_recorded(con):
    fake = FakeGitHub(ten())
    run(con, fake)
    run(con, fake)
    rows = con.execute("select mode, status, rows_changed from _runs order by started").fetchall()
    assert rows == [("initial", "ok", 10), ("incremental", "ok", 0)]


def test_hard_killed_run_is_marked_abandoned(con):
    """Found live: a kill -9 skips `finally`, leaving the run marked running forever."""
    con.execute("insert into _runs (run_id, stream, mode, started, status) "
                "values ('dead', ?, 'initial', now(), 'running')", [f"{REPO}/issues"])
    run(con, FakeGitHub(ten()))
    assert con.execute("select status from _runs where run_id = 'dead'").fetchone()[0] == "abandoned"
