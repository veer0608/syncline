"""A fake GitHub issues API that serves from memory and can misbehave on cue."""
import json
import re
import urllib.parse

from syncline.source import Response


class Crash(Exception):
    """Stands in for the process dying: not a network error, so nothing retries it."""


def ts(n):
    """A distinct, ordered timestamp for small integers."""
    return f"2026-01-01T{n // 3600:02d}:{n // 60 % 60:02d}:{n % 60:02d}Z"


def issue(id_, updated, created=None, **extra):
    row = {
        "id": id_, "number": id_, "title": f"issue {id_}", "state": "open",
        "user": {"login": "someone"}, "comments": 0,
        "created_at": created or ts(id_), "updated_at": updated, "closed_at": None,
    }
    row.update(extra)
    return row


class FakeClock:
    def __init__(self):
        self.t = 1_000_000.0
        self.sleeps = []

    def now(self):
        return self.t

    def sleep(self, s):
        self.sleeps.append(s)
        self.t += s


class FakeGitHub:
    def __init__(self, issues):
        self.issues = {i["id"]: dict(i) for i in issues}
        self.calls = 0
        self.faults = {}         # call number -> Response returned instead of data
        self.crash_at = None     # call number that raises Crash
        self.before_call = None  # hook(call_number, fake), to change data mid-run

    def edit(self, id_, updated, **changes):
        self.issues[id_].update(changes, updated_at=updated)

    def __call__(self, url, headers):
        self.calls += 1
        n = self.calls
        if self.before_call:
            self.before_call(n, self)
        if n == self.crash_at:
            raise Crash(f"crashed on request {n}")
        if n in self.faults:
            return self.faults[n]

        parts = urllib.parse.urlparse(url)
        one = re.search(r"/issues/(\d+)$", parts.path)
        if one:
            found = [i for i in self.issues.values() if i["number"] == int(one.group(1))]
            if not found:
                return Response(404, {}, b'{"message": "Not Found"}')
            return Response(200, {}, json.dumps(found[0]).encode())

        q = dict(urllib.parse.parse_qsl(parts.query))
        # Like GitHub, sort and filter on an internal timestamp. It equals the shown
        # updated_at unless a row carries "_sort", which is never sent to the client.
        sort_key = lambda r: r.get("_sort", r["updated_at"])  # noqa: E731
        rows = list(self.issues.values())
        if "since" in q:
            rows = [r for r in rows if sort_key(r) >= q["since"]]
        if q.get("sort") == "created":
            rows.sort(key=lambda r: (r["created_at"], r["id"]))
        else:
            rows.sort(key=lambda r: (sort_key(r), r["id"]))
        rows = [{k: v for k, v in r.items() if k != "_sort"} for r in rows]
        per, page = int(q["per_page"]), int(q.get("page", 1))
        chunk = rows[(page - 1) * per: page * per]
        headers = {}
        if page * per < len(rows):
            nxt = dict(q, page=str(page + 1))
            headers["link"] = f'<{parts._replace(query=urllib.parse.urlencode(nxt)).geturl()}>; rel="next"'
        return Response(200, headers, json.dumps(chunk).encode())
