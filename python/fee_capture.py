"""Capture and persist the raw evidence behind fee-schedule resolution.

Every fee-metadata request is recorded with what is needed to audit it
later: the exact URL and query, local request/response times, status, the
Date / Age / Cache-Control headers, the untouched body (content-addressed by
SHA-256), pagination position, and the assumption context in force. Nothing
here decides fees; fee_schedule.py does that from these captures.

Storage (append-only, under a caller-chosen root):
    <root>/bodies/<sha256>       raw response bytes, written once
    <root>/captures.jsonl        one JSON record per request, in order

Network access goes through an injected async `send(url, params)` returning
a RawResponse, so the capture logic is testable without a network stack.
`httpx_send(client)` adapts an httpx.AsyncClient for production use.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Iterator, Mapping, Optional

import fee_schedule as fs

DEFAULT_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
MAX_EVENT_CHANGE_PAGES = 200


class FeeCaptureError(RuntimeError):
    """A capture could not be made, stored, or trusted. `status` and
    `retry_after` are set when the cause was a recorded HTTP response."""

    def __init__(self, message: str, *, status: Optional[int] = None,
                 retry_after: Optional[str] = None, capture_id: Optional[str] = None):
        super().__init__(message)
        self.status = status
        self.retry_after = retry_after
        self.capture_id = capture_id


@dataclass(frozen=True)
class RawResponse:
    status: int
    headers: Mapping[str, str]
    body: bytes


Send = Callable[[str, Mapping[str, str]], Awaitable[RawResponse]]
Clock = Callable[[], datetime]


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: Optional[datetime]) -> Optional[str]:
    return dt.isoformat() if dt is not None else None


_TICKER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


def _ticker(t: Any) -> str:
    if not isinstance(t, str) or not _TICKER_RE.match(t):
        raise FeeCaptureError(f"bad ticker {t!r}")
    return t


def _header(headers: Mapping[str, str], name: str) -> Optional[str]:
    lname = name.lower()
    for k, v in headers.items():
        if k.lower() == lname:
            return v
    return None


@dataclass(frozen=True)
class CaptureRecord:
    capture_id: str
    batch_id: str
    url: str
    params: tuple[tuple[str, str], ...]
    request_sent: datetime
    response_received: datetime
    status: int
    date_header: Optional[str]
    age_header: Optional[str]
    cache_control: Optional[str]
    body_sha256: str
    body_size: int
    page_index: Optional[int] = None
    request_cursor: Optional[str] = None
    context: dict = field(default_factory=dict)

    def to_json(self) -> dict:
        d = asdict(self)
        d["params"] = [list(p) for p in self.params]
        d["request_sent"] = _iso(self.request_sent)
        d["response_received"] = _iso(self.response_received)
        return d

    @classmethod
    def from_json(cls, d: dict) -> "CaptureRecord":
        try:
            return cls(
                capture_id=d["capture_id"], batch_id=d["batch_id"], url=d["url"],
                params=tuple((str(k), str(v)) for k, v in d["params"]),
                request_sent=datetime.fromisoformat(d["request_sent"]),
                response_received=datetime.fromisoformat(d["response_received"]),
                status=int(d["status"]), date_header=d["date_header"],
                age_header=d["age_header"], cache_control=d["cache_control"],
                body_sha256=d["body_sha256"], body_size=int(d["body_size"]),
                page_index=d.get("page_index"), request_cursor=d.get("request_cursor"),
                context=dict(d.get("context") or {}),
            )
        except (KeyError, TypeError, ValueError) as e:
            raise FeeCaptureError(f"malformed capture record: {e}") from e

    def as_of(self, *, clock_skew_s: float) -> fs.AsOf:
        """Conservative observation window from this capture's own timing and
        cache headers. Missing or unparseable caching information is refused:
        without it the body's age is unknown."""
        cc = (self.cache_control or "").lower()
        directives = [p.strip() for p in cc.split(",") if p.strip()]
        if "no-store" in directives or "no-cache" in directives:
            max_age = 0
        else:
            ages = [p.split("=", 1)[1] for p in directives if p.startswith("max-age=")]
            if len(ages) != 1 or not ages[0].isdigit():
                raise FeeCaptureError(
                    f"capture {self.capture_id}: no usable Cache-Control ({self.cache_control!r})")
            max_age = int(ages[0])
        age = 0
        if self.age_header is not None:
            if not self.age_header.strip().isdigit():
                raise FeeCaptureError(f"capture {self.capture_id}: bad Age {self.age_header!r}")
            age = int(self.age_header.strip())
        return fs.AsOf.from_http(
            request_sent=self.request_sent, response_received=self.response_received,
            cache_max_age_s=max_age, age_s=age, clock_skew_s=clock_skew_s,
        )


class CaptureStore:
    """Append-only, content-addressed store of captures."""

    def __init__(self, root: Path | str):
        self.root = Path(root)
        self.bodies = self.root / "bodies"
        self.log = self.root / "captures.jsonl"
        self.bodies.mkdir(parents=True, exist_ok=True)

    def put_body(self, body: bytes) -> str:
        """Store body under its digest. If a file already exists there, its
        content is verified; a mismatch raises and the corrupted file is
        left untouched for diagnosis (never silently overwritten)."""
        digest = hashlib.sha256(body).hexdigest()
        path = self.bodies / digest
        if path.exists():
            existing = path.read_bytes()
            if hashlib.sha256(existing).hexdigest() != digest:
                raise FeeCaptureError(
                    f"stored body {digest} is corrupted on disk; refusing to reuse it")
        else:
            tmp = path.with_name(f".{digest}.{uuid.uuid4().hex}.tmp")
            with open(tmp, "wb") as f:
                f.write(body)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, path)
        return digest

    def get_body(self, digest: str) -> bytes:
        if not (len(digest) == 64 and all(c in "0123456789abcdef" for c in digest)):
            raise FeeCaptureError(f"bad body digest {digest!r}")
        try:
            body = (self.bodies / digest).read_bytes()
        except FileNotFoundError as e:
            raise FeeCaptureError(f"body {digest} missing") from e
        if hashlib.sha256(body).hexdigest() != digest:
            raise FeeCaptureError(f"body {digest} corrupted on disk")
        return body

    def append(self, record: CaptureRecord) -> None:
        line = json.dumps(record.to_json(), sort_keys=True, separators=(",", ":"))
        with open(self.log, "a", encoding="utf-8") as f:
            f.write(line + "\n")
            f.flush()
            os.fsync(f.fileno())

    def records(self) -> Iterator[CaptureRecord]:
        if not self.log.exists():
            return
        with open(self.log, encoding="utf-8") as f:
            for n, line in enumerate(f, 1):
                if not line.strip():
                    continue
                try:
                    yield CaptureRecord.from_json(json.loads(line))
                except json.JSONDecodeError as e:
                    raise FeeCaptureError(f"captures.jsonl line {n} unreadable") from e

    def get(self, capture_id: str) -> tuple[CaptureRecord, bytes]:
        for r in self.records():
            if r.capture_id == capture_id:
                return r, self.get_body(r.body_sha256)
        raise FeeCaptureError(f"capture {capture_id} not found")


class FeeMetadataCapturer:
    """Fetches fee metadata and records every request before returning it.

    Non-200 responses are recorded too (a 404 is evidence) and then raised.
    `context` (e.g. the assumption ids a run operates under) is stored on
    every record of the batch."""

    def __init__(self, send: Send, store: CaptureStore, *, base_url: str = DEFAULT_BASE_URL,
                 clock: Clock = _utcnow):
        self.send = send
        self.store = store
        self.base_url = base_url.rstrip("/")
        self.clock = clock

    def new_batch(self) -> str:
        return uuid.uuid4().hex

    async def capture(self, path: str, params: Optional[Mapping[str, str]] = None, *,
                      batch_id: str, context: Optional[dict] = None,
                      page_index: Optional[int] = None,
                      request_cursor: Optional[str] = None) -> tuple[CaptureRecord, bytes]:
        if not path.startswith("/"):
            raise FeeCaptureError(f"path must start with '/', got {path!r}")
        url = self.base_url + path
        params = {str(k): str(v) for k, v in (params or {}).items()}
        sent = self.clock()
        resp = await self.send(url, params)
        received = self.clock()
        if not isinstance(resp, RawResponse) or not isinstance(resp.body, (bytes, bytearray)):
            raise FeeCaptureError("send() must return a RawResponse with bytes body")
        body = bytes(resp.body)
        digest = self.store.put_body(body)
        rec = CaptureRecord(
            capture_id=uuid.uuid4().hex, batch_id=batch_id, url=url,
            params=tuple(sorted(params.items())), request_sent=sent,
            response_received=received, status=int(resp.status),
            date_header=_header(resp.headers, "Date"),
            age_header=_header(resp.headers, "Age"),
            cache_control=_header(resp.headers, "Cache-Control"),
            body_sha256=digest, body_size=len(body),
            page_index=page_index, request_cursor=request_cursor,
            context=dict(context or {}),
        )
        self.store.append(rec)
        if rec.status != 200:
            raise FeeCaptureError(f"{url} returned HTTP {rec.status} (capture {rec.capture_id})",
                                  status=rec.status, capture_id=rec.capture_id,
                                  retry_after=_header(resp.headers, "Retry-After"))
        return rec, body

    async def series(self, ticker: str, *, batch_id: str, context: Optional[dict] = None):
        return await self.capture(f"/series/{_ticker(ticker)}", batch_id=batch_id, context=context)

    async def series_fee_changes(self, *, show_historical: bool, batch_id: str,
                                 context: Optional[dict] = None):
        params = {"show_historical": "true"} if show_historical else {}
        return await self.capture("/series/fee_changes", params, batch_id=batch_id, context=context)

    async def event(self, event_ticker: str, *, batch_id: str, context: Optional[dict] = None):
        return await self.capture(f"/events/{_ticker(event_ticker)}", batch_id=batch_id, context=context)

    async def market(self, ticker: str, *, batch_id: str, context: Optional[dict] = None):
        return await self.capture(f"/markets/{_ticker(ticker)}", batch_id=batch_id, context=context)

    async def event_fee_changes(self, *, event_ticker: Optional[str], limit: int = 1000,
                                batch_id: str, context: Optional[dict] = None,
                                max_pages: int = MAX_EVENT_CHANGE_PAGES):
        """Every page of one listing, each captured with its request cursor.
        Raises if the listing does not finish within max_pages or repeats a
        cursor; partial listings are never returned."""
        if not (1 <= limit <= 1000):
            raise FeeCaptureError("limit must be in 1..1000")
        pages: list[tuple[CaptureRecord, bytes]] = []
        cursor = ""
        seen: set[str] = set()
        for i in range(max_pages):
            params = {"limit": str(limit)}
            if event_ticker is not None:
                params["event_ticker"] = _ticker(event_ticker)
            if cursor:
                params["cursor"] = cursor
            rec, body = await self.capture("/events/fee_changes", params, batch_id=batch_id,
                                           context=context, page_index=i, request_cursor=cursor)
            pages.append((rec, body))
            try:
                nxt = fs.loads(body).get("cursor")
            except (fs.FeeMetadataError, AttributeError) as e:
                raise FeeCaptureError(f"page {i} unreadable (capture {rec.capture_id})") from e
            if not isinstance(nxt, str):
                raise FeeCaptureError(f"page {i} has no string cursor (capture {rec.capture_id})")
            if nxt == "":
                return pages
            if nxt in seen:
                raise FeeCaptureError(f"cursor repeated at page {i}: listing not progressing")
            seen.add(nxt)
            cursor = nxt
        raise FeeCaptureError(f"event fee changes exceeded {max_pages} pages")


def httpx_send(client: Any) -> Send:
    """Adapt an httpx.AsyncClient (already configured with proxy/CA/timeouts)."""
    async def send(url: str, params: Mapping[str, str]) -> RawResponse:
        r = await client.get(url, params=dict(params))
        return RawResponse(status=r.status_code, headers=dict(r.headers), body=r.content)
    return send
