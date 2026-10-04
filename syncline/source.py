"""Fetching pages from the GitHub issues API.

The source only knows how to ask for one page. It does not loop, retry or
remember anything; those belong to `sync`, `retry` and `state`. The network is
behind a `transport` callable so tests can swap in a fake GitHub.
"""
import json
import os
import subprocess
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass

API = "https://api.github.com"
PER_PAGE = 100


@dataclass
class Response:
    status: int
    headers: dict  # keys lower-cased
    body: bytes


@dataclass
class Page:
    rows: list
    next_url: str | None
    full: bool  # True when the page came back with per_page rows, so more may follow


def urllib_transport(url, headers):
    """One HTTP GET. HTTP errors come back as a Response; network errors raise."""
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return Response(r.status, {k.lower(): v for k, v in r.headers.items()}, r.read())
    except urllib.error.HTTPError as e:
        return Response(e.code, {k.lower(): v for k, v in e.headers.items()}, e.read())


def github_token():
    """GITHUB_TOKEN if set, else whatever `gh` is logged in with, else None (60 requests/hour)."""
    if os.environ.get("GITHUB_TOKEN"):
        return os.environ["GITHUB_TOKEN"]
    try:
        out = subprocess.run(["gh", "auth", "token"], capture_output=True, text=True, timeout=10)
        return out.stdout.strip() or None
    except (OSError, subprocess.TimeoutExpired):
        return None


def next_link(link_header):
    """The rel="next" URL from a GitHub Link header, or None on the last page."""
    for part in (link_header or "").split(","):
        url, _, rel = part.partition(";")
        if 'rel="next"' in rel:
            return url.strip().strip("<>")
    return None


class GitHubSource:
    """Issues and pull requests of one repository, oldest update first."""

    def __init__(self, repo, *, transport=urllib_transport, token=None, per_page=PER_PAGE):
        self.repo = repo
        self.per_page = per_page
        self.transport = transport
        self.headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "syncline",
        }
        if token:
            self.headers["Authorization"] = f"Bearer {token}"
        self.requests = 0

    def url_since(self, since):
        q = {"state": "all", "sort": "updated", "direction": "asc", "per_page": self.per_page}
        if since:
            q["since"] = since
        return f"{API}/repos/{self.repo}/issues?{urllib.parse.urlencode(q)}"

    def url_all(self):
        """Every issue, oldest created first. Creation order is fixed, so edits cannot reshuffle it."""
        q = {"state": "all", "sort": "created", "direction": "asc", "per_page": self.per_page}
        return f"{API}/repos/{self.repo}/issues?{urllib.parse.urlencode(q)}"

    def url_issue(self, number):
        return f"{API}/repos/{self.repo}/issues/{number}"

    def get(self, url):
        """One raw request. Retrying is the caller's job."""
        self.requests += 1
        return self.transport(url, self.headers)

    def parse(self, resp):
        rows = json.loads(resp.body)
        return Page(rows, next_link(resp.headers.get("link")), len(rows) >= self.per_page)
